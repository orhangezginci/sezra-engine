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

# Dupliziert aus deviation-detector-service (bewusst, keine geteilte
# Bibliothek - siehe contracts/README.md, Abschnitt "Was ein Service
# nicht tun darf"). "value"/"source_type": wie im Detector. Die uebrigen
# Felder sind Anreicherungs-Metadaten, die knowledge-service zum Payload
# hinzufuegt (semantic_text, source_event_id/type, source_occurred_at) -
# ohne sie auszuschliessen, wuerde der composite_key hier NIE mit dem im
# Detector aus dem rohen Payload berechneten uebereinstimmen (u. a. weil
# source_event_id pro Event eindeutig ist), und der ganze
# Selbst-Ausschluss-Mechanismus im Analyzer waere wirkungslos.
NON_DIMENSION_FIELDS = {
    "value",
    "source_type",
    "semantic_text",
    "source_event_id",
    "source_event_type",
    "source_occurred_at",
}


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
                    # Vorbeugend wie in analyzer-service/context-severity-
                    # detector-service: ein blockierender Embedding-Aufruf
                    # koennte laenger dauern als RabbitMQ's Heartbeat-
                    # Default (60s) toleriert, besonders ueber die
                    # host.docker.internal-Bruecke.
                    heartbeat=600,
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
    """
    nomic-embed-text erwartet ein Task-Prefix ("search_document:" fuer zu
    speichernde/durchsuchende Texte, "search_query:" fuer Suchanfragen -
    siehe Modell-Dokumentation). Ohne Prefix sind die Embeddings zwar
    gueltig, aber nicht optimal fuer Retrieval kalibriert - das Modell
    wurde explizit mit dieser Konvention trainiert.
    """
    prefixed_text = f"search_document: {text}"

    response = requests.post(
        f"http://{OLLAMA_HOST}:{OLLAMA_PORT}/api/embeddings",
        json={"model": OLLAMA_EMBEDDING_MODEL, "prompt": prefixed_text},
        # War 30s - zu knapp bei Ollama-Model-Swapping (siehe
        # analyzer-service main.py fuer die ausfuehrliche Begruendung).
        timeout=90,
    )
    response.raise_for_status()
    return response.json()["embedding"]


def is_observation(payload: dict) -> bool:
    return "metric" in payload and "value" in payload


def build_composite_key(payload: dict) -> str:
    """
    Identisch zur Logik in deviation-detector-service: "metric" plus alle
    weiteren Dimensions-Felder, sortiert. Ermoeglicht dem Analyzer, eine
    Beobachtungsreihe von sich selbst als Ursachenkandidat auszuschliessen
    (z. B. darf math_test_average=79 nicht als "Erklaerung" fuer
    math_test_average=45 aus derselben Reihe gelten), waehrend eine
    ANDERE Metrik-Reihe weiterhin als legitime Ursache infrage kommt.
    """
    dimension_items = sorted(
        (key, value)
        for key, value in payload.items()
        if key not in NON_DIMENSION_FIELDS and key != "metric"
    )
    dimension_suffix = "|".join(f"{k}={v}" for k, v in dimension_items)
    metric = payload["metric"]
    return f"{metric}|{dimension_suffix}" if dimension_suffix else str(metric)


def write_to_qdrant(qdrant_client: QdrantClient, envelope: dict, vector: list[float]) -> None:
    payload = envelope["payload"]
    composite_key = build_composite_key(payload) if is_observation(payload) else None

    qdrant_client.upsert(
        collection_name=QDRANT_COLLECTION_NAME,
        points=[
            PointStruct(
                id=str(uuid4()),
                vector=vector,
                payload={
                    # source_event_id (Payload-Konvention, wie
                    # source_occurred_at) ist die ID des URSPRUENGLICHEN
                    # Events (z. B. das AnomalyDetected/ContextIngested,
                    # bevor knowledge-service es angereichert hat) -
                    # envelope["event_id"] ist nur die frisch generierte
                    # ID des Anreicherungs-Wrappers selbst, nutzlos fuer
                    # Kreuzverweise (Selbstbezug-Ausschluss, "diese
                    # Anomalie wurde bereits anderswo als Ursache
                    # gefunden") - genau derselbe Bug-Typ wie beim
                    # occurred_at-Fix, nur bei der Identitaet statt der
                    # Zeit uebersehen. Fallback auf envelope["event_id"]
                    # fuer Envelopes ohne diese Konvention.
                    "event_id": payload.get("source_event_id", envelope["event_id"]),
                    "project_id": envelope.get("project_id"),
                    "event_type": envelope["event_type"],
                    "source": envelope["source"],
                    # source_occurred_at (payload-Konvention, siehe
                    # payload-conventions.md) ist der Zeitpunkt des
                    # urspruenglichen Envelopes, durchgereicht von
                    # knowledge-service - das braucht der Analyzer fuer
                    # die zeitliche Kausalitaets-Pruefung. Fallback auf
                    # envelope["occurred_at"], falls das Feld fehlt
                    # (z. B. bei aelteren Envelopes ohne diese Konvention).
                    "occurred_at": payload.get("source_occurred_at", envelope["occurred_at"]),
                    "semantic_text": payload.get("semantic_text"),
                    # null bei Kontext-Events (keine Metrik-Beobachtung) -
                    # die werden vom Analyzer dann nie ausgeschlossen.
                    "composite_key": composite_key,
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
