"""
knowledge-service

Konsumiert von sezra.stream.validated, erzeugt eine deterministische,
semantische Textbeschreibung (semantic_text) aus den vorhandenen
payload-Feldern - noch kein LLM-Aufruf, reine Feld-zu-Text-Zusammensetzung.

Published das Original-Envelope additiv angereichert (Original bleibt
unveraendert erhalten, semantic_text kommt als neues payload-Feld dazu)
zu sezra.stream.enriched.semantic.

Level-1-Knowledge-Builder im Sinne des Master Prompts: generisch,
domaenenagnostisch, kein Spezialwissen ueber bestimmte Metrik-Namen o.ae.
"""

import json
import os
import time
from datetime import datetime, timezone
from uuid import uuid4

import pika

from envelope_validation import InvalidEnvelopeError, validate_envelope

SERVICE_NAME = "knowledge-service"

DEAD_LETTER_EXCHANGE = "sezra.stream.dead_letter"
DEAD_LETTER_ROUTING_KEY = f"{SERVICE_NAME}.failed"

INPUT_EXCHANGE = "sezra.stream.validated"
OUTPUT_EXCHANGE = "sezra.stream.enriched.semantic"
QUEUE_NAME = f"sezra.queue.{SERVICE_NAME}"


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


RABBITMQ_HOST = required_env("RABBITMQ_HOST")
RABBITMQ_PORT = int(required_env("RABBITMQ_PORT"))
RABBITMQ_USER = required_env("RABBITMQ_USER")
RABBITMQ_PASSWORD = required_env("RABBITMQ_PASSWORD")


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


def build_semantic_text(payload: dict) -> str:
    """
    Generische Feld-zu-Text-Zusammensetzung, bewusst ohne Wissen ueber
    bestimmte Feldnamen (kein "if metric_name == ...") - funktioniert
    gleichermassen fuer School-, Healthcare- oder Manufacturing-Payloads,
    weil sie einfach ueber alle vorhandenen Felder iteriert.
    """
    parts = []
    for key, value in payload.items():
        readable_key = key.replace("_", " ")
        parts.append(f"{readable_key}: {value}")
    return "; ".join(parts)


def create_enriched_event(original_envelope: dict, semantic_text: str) -> dict:
    original_event_id = original_envelope["event_id"]

    return {
        "schema_version": "1.1",
        "event_id": str(uuid4()),
        "event_type": "SemanticEnrichmentGenerated",
        "source": SERVICE_NAME,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "correlation_id": original_envelope.get("correlation_id") or original_event_id,
        "causation_id": original_event_id,
        "project_id": original_envelope.get("project_id"),
        "payload": {
            **original_envelope["payload"],
            "semantic_text": semantic_text,
            "source_event_id": original_event_id,
            "source_event_type": original_envelope["event_type"],
            # Payload-Konvention (siehe payload-conventions.md), nicht
            # Envelope-Feld: occurred_at oben ist bewusst DIESES Envelopes
            # eigene Erzeugungszeit (schema-konform). source_occurred_at
            # traegt den Zeitpunkt des urspruenglichen Envelopes durch -
            # das braucht der spaetere Analyzer fuer die zeitliche
            # Kausalitaets-Pruefung (Ursache muss vor der Anomalie liegen).
            "source_occurred_at": original_envelope["occurred_at"],
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


def handle_message(channel, method, properties, body: bytes) -> None:
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

    semantic_text = build_semantic_text(envelope["payload"])
    enriched_event = create_enriched_event(envelope, semantic_text)

    try:
        validate_envelope(enriched_event)
    except InvalidEnvelopeError as error:
        # Wir haben das Envelope selbst gebaut - ein Fehler hier ist ein
        # Bug in diesem Service, kein Problem der Eingangsdaten.
        print(f"[{SERVICE_NAME}] BUG: self-built envelope is invalid: {error}")
        publish_dead_letter(channel, body, f"Self-built envelope invalid: {error}", "permanent")
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    channel.basic_publish(
        exchange=OUTPUT_EXCHANGE,
        routing_key="",
        body=json.dumps(enriched_event).encode("utf-8"),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )

    print(
        f"[{SERVICE_NAME}] Enriched {envelope['event_type']} "
        f"({envelope['event_id']}) -> semantic_text"
    )

    channel.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    print(f"[{SERVICE_NAME}] starting")

    connection = connect_to_rabbitmq()
    channel = connection.channel()

    channel.exchange_declare(exchange=INPUT_EXCHANGE, exchange_type="fanout", durable=True)
    channel.exchange_declare(exchange=OUTPUT_EXCHANGE, exchange_type="fanout", durable=True)
    channel.exchange_declare(exchange=DEAD_LETTER_EXCHANGE, exchange_type="fanout", durable=True)

    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(exchange=INPUT_EXCHANGE, queue=QUEUE_NAME)

    print(f"[{SERVICE_NAME}] listening on queue: {QUEUE_NAME}")

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=handle_message)
    channel.start_consuming()


if __name__ == "__main__":
    main()
