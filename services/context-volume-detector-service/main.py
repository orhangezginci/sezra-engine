"""
context-volume-detector-service

Konsumiert von sezra.stream.enriched.semantic, verarbeitet nur Eintraege,
die urspruenglich ContextIngested waren (z. B. Support-Tickets, Mails -
nicht Metrik-Beobachtungen, nicht bereits erkannte Anomalien). Zaehlt
fuer jede eingehende Nachricht, wie viele THEMATISCH AeHNLICHE Nachrichten
(Kosinus-Aehnlichkeit ueber SIMILARITY_THRESHOLD) innerhalb eines
Zeitfensters bereits in Qdrant liegen. Bei Ueberschreiten von
VOLUME_THRESHOLD wird eine AnomalyDetected-Meldung (anomaly_type:
"volume") ausgeloest.

Ergaenzt context-severity-detector-service (eine einzelne, dringliche
Nachricht loest sofort aus) um den Fall, dass KEINE einzelne Nachricht
fuer sich kritisch ist, aber eine ungewoehnliche HAeUFUNG zum selben
Thema auffaellig ist (z. B. 40 "Passwort-Reset funktioniert nicht"-
Tickets statt der ueblichen 3-5 pro Tag - kein einzelnes davon dringend
genug fuer context-severity-detector-service, aber die Haeufung ist ein
echtes Signal).

Bewusst KEIN Z-Score wie deviation-detector-service: dort gibt es einen
stabilen composite_key (Metrikname + Dimensionen), auf dem sich eine
Baseline ueber Zeit aufbauen laesst. Bei thematischen Haeufungen gibt es
keine vorab definierten "Themen" - deshalb ein einfacherer, fester
Schwellwert (Anzahl aehnlicher Nachrichten pro Zeitfenster) statt einer
statistischen Abweichung von einer gelernten Baseline.

Bekannte Einschraenkung (bewusst, kein Overengineering fuer jetzt):
Sobald der Schwellwert einmal ueberschritten ist, koennte jede weitere,
kurz danach eintreffende aehnliche Nachricht ebenfalls eine eigene
Anomalie ausloesen (kein Cooldown/Deduplizierung). Fuer den aktuellen
Zweck kein Blocker, aber eine sinnvolle spaetere Erweiterung.
"""

import json
import os
import time
from datetime import datetime, timezone
from uuid import uuid4

import pika
from fastembed import TextEmbedding
from qdrant_client import QdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchValue

from envelope_validation import InvalidEnvelopeError, validate_envelope

SERVICE_NAME = "context-volume-detector-service"

DEAD_LETTER_EXCHANGE = "sezra.stream.dead_letter"
DEAD_LETTER_ROUTING_KEY = f"{SERVICE_NAME}.failed"

INPUT_EXCHANGE = "sezra.stream.enriched.semantic"
OUTPUT_EXCHANGE = "sezra.stream.anomaly"
QUEUE_NAME = f"sezra.queue.{SERVICE_NAME}"

QDRANT_COLLECTION_NAME = "sezra_semantic"

# Muss IDENTISCH zu vectorizing-service/analyzer-service sein - sonst
# sind die Vektoren nicht im selben Raum vergleichbar.
EMBEDDING_MODEL_NAME = "jinaai/jina-embeddings-v2-base-de"
_embedding_model = TextEmbedding(model_name=EMBEDDING_MODEL_NAME)

VOLUME_THRESHOLD = int(os.getenv("VOLUME_THRESHOLD", "5"))
SIMILARITY_THRESHOLD = float(os.getenv("VOLUME_SIMILARITY_THRESHOLD", "0.85"))
TIME_WINDOW_MINUTES = int(os.getenv("VOLUME_TIME_WINDOW_MINUTES", "60"))
SEARCH_LIMIT = int(os.getenv("VOLUME_SEARCH_LIMIT", "50"))


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


def connect_to_rabbitmq() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(username=RABBITMQ_USER, password=RABBITMQ_PASSWORD)
    while True:
        try:
            return pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=credentials,
                    heartbeat=600,
                )
            )
        except pika.exceptions.AMQPConnectionError:
            print(f"[{SERVICE_NAME}] RabbitMQ not ready yet. Retrying...")
            time.sleep(3)


def create_embedding(text: str) -> list[float]:
    embeddings = list(_embedding_model.embed([text]))
    return embeddings[0].tolist()


def count_similar_recent_messages(
    qdrant_client: QdrantClient,
    vector: list[float],
    project_id: str | None,
    reference_occurred_at: str,
    exclude_event_id: str,
) -> int:
    """
    Zaehlt, wie viele thematisch aehnliche ContextIngested-Nachrichten
    (nicht Metrik-Beobachtungen, nicht Anomalie-Vektoren - siehe
    source_event_type-Filter) innerhalb des Zeitfensters VOR der
    aktuellen Nachricht bereits in Qdrant liegen. Die aktuelle Nachricht
    selbst wird explizit ausgeschlossen (exclude_event_id) - unabhaengig
    davon, ob vectorizing-service sie zu diesem Zeitpunkt schon
    vektorisiert hat oder nicht (Race Condition zwischen zwei
    unabhaengigen Consumern derselben Exchange).
    """
    must_conditions = [
        FieldCondition(key="source_event_type", match=MatchValue(value="ContextIngested")),
    ]
    if project_id:
        must_conditions.append(FieldCondition(key="project_id", match=MatchValue(value=project_id)))

    query_filter = Filter(must=must_conditions)

    response = qdrant_client.query_points(
        collection_name=QDRANT_COLLECTION_NAME,
        query=vector,
        query_filter=query_filter,
        limit=SEARCH_LIMIT,
    )

    window_start = _minutes_before(reference_occurred_at, TIME_WINDOW_MINUTES)

    count = 0
    for point in response.points:
        if point.payload.get("event_id") == exclude_event_id:
            continue
        if point.score < SIMILARITY_THRESHOLD:
            continue
        candidate_occurred_at = point.payload.get("occurred_at")
        if candidate_occurred_at is None:
            continue
        if not (window_start <= candidate_occurred_at <= reference_occurred_at):
            continue
        count += 1

    return count


def _minutes_before(iso_timestamp: str, minutes: int) -> str:
    from datetime import timedelta

    reference = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
    return (reference - timedelta(minutes=minutes)).isoformat()


def create_volume_anomaly_event(envelope: dict, semantic_text: str, similar_count: int) -> dict:
    source_event_id = envelope["event_id"]

    return {
        "schema_version": "1.1",
        "event_id": str(uuid4()),
        "event_type": "AnomalyDetected",
        "source": SERVICE_NAME,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "correlation_id": envelope.get("correlation_id") or source_event_id,
        "causation_id": source_event_id,
        "project_id": envelope.get("project_id"),
        "payload": {
            "anomaly_type": "volume",
            "similar_message_count": similar_count,
            "text": semantic_text,
            "time_window_minutes": TIME_WINDOW_MINUTES,
            "reason": (
                f"{similar_count} similar messages received within "
                f"{TIME_WINDOW_MINUTES} minutes, exceeding the volume threshold"
            ),
            "source_event_id": source_event_id,
            "source_occurred_at": envelope["occurred_at"],
        },
    }


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

    payload = envelope["payload"]

    if payload.get("source_event_type") != "ContextIngested":
        # Kein Fehler - dieser Service zaehlt ausschliesslich echte
        # Kontext-Einreichungen (Tickets, Mails), keine Metrik-
        # Anreicherungen oder bereits erkannte Anomalien.
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    semantic_text = payload.get("semantic_text")
    if not semantic_text:
        publish_dead_letter(
            channel, body, "Missing payload.semantic_text - nothing to compare", "permanent"
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    try:
        vector = create_embedding(semantic_text)
    except Exception as error:
        print(f"[{SERVICE_NAME}] Embedding error, will retry: {error}")
        return

    reference_occurred_at = payload.get("source_occurred_at", envelope["occurred_at"])
    exclude_event_id = payload.get("source_event_id", envelope["event_id"])

    try:
        similar_count = count_similar_recent_messages(
            qdrant_client, vector, envelope.get("project_id"), reference_occurred_at, exclude_event_id
        )
    except Exception as error:
        print(f"[{SERVICE_NAME}] Qdrant error, will retry: {error}")
        return

    if similar_count >= VOLUME_THRESHOLD:
        anomaly_event = create_volume_anomaly_event(envelope, semantic_text, similar_count)
        channel.basic_publish(
            exchange=OUTPUT_EXCHANGE,
            routing_key="",
            body=json.dumps(anomaly_event).encode("utf-8"),
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        )
        print(
            f"[{SERVICE_NAME}] Volume anomaly detected: {similar_count} similar messages "
            f"within {TIME_WINDOW_MINUTES}min: {semantic_text[:80]}"
        )
    else:
        print(
            f"[{SERVICE_NAME}] {similar_count} similar messages "
            f"(below threshold {VOLUME_THRESHOLD}), no anomaly"
        )

    channel.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    print(f"[{SERVICE_NAME}] starting")

    qdrant_client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
    print(f"[{SERVICE_NAME}] connected to Qdrant")

    connection = connect_to_rabbitmq()
    channel = connection.channel()

    channel.exchange_declare(exchange=INPUT_EXCHANGE, exchange_type="fanout", durable=True)
    channel.exchange_declare(exchange=OUTPUT_EXCHANGE, exchange_type="fanout", durable=True)
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