"""
vectorizing-service

Konsumiert von sezra.stream.enriched.semantic, nimmt payload.semantic_text,
erzeugt daraus per Ollama (nomic-embed-text) einen Embedding-Vektor und
schreibt ihn zusammen mit Metadaten (event_id, project_id, event_type,
semantic_text) nach Qdrant.

Reiner Consumer: kein Publish zu irgendeiner Exchange. Wie bei
persistence-service ist das Schreiben in einen externen Speicher (hier
Qdrant statt Postgres) die bewusste Ausnahme vom
"immer nur Exchange-zu-Exchange"-Prinzip - ohne sie gaebe es keine
Vektorsuche.
"""

import json
import os
import time
from datetime import datetime, timezone
from uuid import uuid4

import pika
import requests
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

from envelope_validation import InvalidEnvelopeError, validate_envelope

SERVICE_NAME = "vectorizing-service"

DEAD_LETTER_EXCHANGE = "sezra.stream.dead_letter"
DEAD_LETTER_ROUTING_KEY = f"{SERVICE_NAME}.failed"

INPUT_EXCHANGE = "sezra.stream.enriched.semantic"
QUEUE_NAME = f"sezra.queue.{SERVICE_NAME}"

QDRANT_COLLECTION_NAME = "sezra_semantic"
EMBEDDING_VECTOR_SIZE = 768  # nomic-embed-text erzeugt 768-dimensionale Vektoren


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


RABBITMQ_HOST = required_env("RABBITMQ_HOST")
RABBITMQ_PORT = int(required_env("RABBITMQ_PORT"))
RABBITMQ_USER = required_env("RABBITMQ_USER")
RABBITMQ_PASSWORD = required_env("RABBITMQ_PASSWORD")

QDRANT_HOST = required_env("QDRANT_HOST")
QDRANT_PORT = int(required_env("QDRANT_PORT"))

OLLAMA_HOST = required_env("OLLAMA_HOST")
OLLAMA_PORT = required_env("OLLAMA_PORT")
OLLAMA_EMBEDDING_MODEL = required_env("OLLAMA_EMBEDDING_MODEL")


def connect_to_rabbitmq() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(
        username=RABBITMQ_USER,
        password=RABBITMQ_PASSWORD,
    )
    while True:
        try:
            return pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=credentials,
                )
            )
        except pika.exceptions.AMQPConnectionError:
            print(f"[{SERVICE_NAME}] RabbitMQ not ready yet. Retrying...")
            time.sleep(3)


def publish_dead_letter(channel, original_body: bytes, reason: str, failure_class: str) -> None:
    failed_event = {
        "schema_version": "1.0",
        "event_id": str(uuid4()),
        "event_type": "EventProcessingFailed",
        "source": SERVICE_NAME,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "failed_service": SERVICE_NAME,
            "failure_class": failure_class,
            "reason": reason,
            "original_body": original_body.decode("utf-8", errors="replace"),
        },
    }
    channel.basic_publish(
        exchange=DEAD_LETTER_EXCHANGE,
        routing_key=DEAD_LETTER_ROUTING_KEY,
        body=json.dumps(failed_event).encode("utf-8"),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )
    print(f"[{SERVICE_NAME}] Published dead-letter event (class={failure_class}): {reason}")


def create_embedding(text: str) -> list[float]:
    response = requests.post(
        f"http://{OLLAMA_HOST}:{OLLAMA_PORT}/api/embeddings",
        json={"model": OLLAMA_EMBEDDING_MODEL, "prompt": text},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["embedding"]


def write_to_qdrant(qdrant_client: QdrantClient, envelope: dict, vector: list[float]) -> None:
    payload = envelope["payload"]

    qdrant_client.upsert(
        collection_name=QDRANT_COLLECTION_NAME,
        points=[
            PointStruct(
                id=str(uuid4()),
                vector=vector,
                payload={
                    "event_id": envelope["event_id"],
                    "project_id": envelope.get("project_id"),
                    "event_type": envelope["event_type"],
                    "source": envelope["source"],
                    "semantic_text": payload.get("semantic_text"),
                },
            )
        ],
    )


def handle_message(channel, method, properties, body: bytes, qdrant_client: QdrantClient) -> None:
    try:
        envelope = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as error:
        publish_dead_letter(channel, body, f"Invalid JSON: {error}", "permanent")
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    try:
        validate_envelope(envelope)
    except InvalidEnvelopeError as error:
        publish_dead_letter(channel, body, str(error), "permanent")
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    semantic_text = envelope["payload"].get("semantic_text")
    if not semantic_text:
        publish_dead_letter(
            channel, body, "Missing payload.semantic_text - nothing to vectorize", "permanent"
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    try:
        vector = create_embedding(semantic_text)
    except requests.RequestException as error:
        # Kein Dead-Letter, kein Ack: das Envelope war gueltig, das
        # Problem liegt an Ollama (z. B. kurz nicht erreichbar).
        # Nachricht bleibt in der Queue, wird erneut zugestellt.
        print(f"[{SERVICE_NAME}] Ollama error, will retry: {error}")
        return

    try:
        write_to_qdrant(qdrant_client, envelope, vector)
    except Exception as error:
        # Gleiche Logik: Qdrant-Fehler ist kein Nachrichtenfehler.
        print(f"[{SERVICE_NAME}] Qdrant error, will retry: {error}")
        return

    print(f"[{SERVICE_NAME}] Vectorized {envelope['event_type']} ({envelope['event_id']})")

    channel.basic_ack(delivery_tag=method.delivery_tag)


def ensure_qdrant_collection(qdrant_client: QdrantClient) -> None:
    existing = [c.name for c in qdrant_client.get_collections().collections]
    if QDRANT_COLLECTION_NAME in existing:
        return

    print(f"[{SERVICE_NAME}] Creating Qdrant collection: {QDRANT_COLLECTION_NAME}")
    qdrant_client.create_collection(
        collection_name=QDRANT_COLLECTION_NAME,
        vectors_config=VectorParams(size=EMBEDDING_VECTOR_SIZE, distance=Distance.COSINE),
    )


def main() -> None:
    print(f"[{SERVICE_NAME}] starting")

    qdrant_client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
    ensure_qdrant_collection(qdrant_client)
    print(f"[{SERVICE_NAME}] connected to Qdrant")

    connection = connect_to_rabbitmq()
    channel = connection.channel()

    channel.exchange_declare(exchange=INPUT_EXCHANGE, exchange_type="fanout", durable=True)
    channel.exchange_declare(exchange=DEAD_LETTER_EXCHANGE, exchange_type="fanout", durable=True)

    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(exchange=INPUT_EXCHANGE, queue=QUEUE_NAME)

    print(f"[{SERVICE_NAME}] listening on queue: {QUEUE_NAME}")

    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=lambda ch, method, properties, body: handle_message(
            ch, method, properties, body, qdrant_client
        ),
    )
    channel.start_consuming()


if __name__ == "__main__":
    main()