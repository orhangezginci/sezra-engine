"""
__SERVICE_NAME__ - Grundgerüst.

Dies ist ein lauffaehiges Skelett, keine fertige Business-Logik.
Ersetze handle_message mit der tatsaechlichen Aufgabe dieses Service.

WICHTIG: Dieses Skelett nimmt an, dass der Service sowohl konsumiert
(RabbitMQ-Input) als auch published (RabbitMQ-Output). Das trifft nicht
auf jeden Service-Typ zu:
- Reiner Producer (z. B. ein Daten-Adapter, der eine externe Quelle
  beobachtet): den Consumer-Teil (queue_bind, on_message_callback,
  start_consuming) entfernen.
- Reiner Consumer (z. B. ein Service, der nur persistiert, ohne selbst
  etwas zu publizieren): den Producer-Teil entfernen.
Passe main() entsprechend an, statt ungenutzten Code stehen zu lassen.
"""

import json
import os
import time
from datetime import datetime, timezone
from uuid import uuid4

import pika

from envelope_validation import InvalidEnvelopeError, validate_envelope

SERVICE_NAME = "__SERVICE_NAME__"

DEAD_LETTER_EXCHANGE = "sezra.stream.dead_letter"
DEAD_LETTER_ROUTING_KEY = f"{SERVICE_NAME}.failed"

# TODO: anpassen an das, was dieser Service tatsaechlich konsumiert/publiziert
INPUT_EXCHANGE = "sezra.stream.__INPUT_EXCHANGE_NAME__"
OUTPUT_EXCHANGE = "sezra.stream.__OUTPUT_EXCHANGE_NAME__"
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

    # TODO: eigentliche Verarbeitung hier einfuegen.
    print(f"[{SERVICE_NAME}] Received valid envelope: {envelope['event_id']}")

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