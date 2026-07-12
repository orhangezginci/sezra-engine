"""
persistence-service

Konsumiert von sezra.stream.validated UND sezra.stream.investigation,
schreibt jedes gueltige Envelope als Zeile in die events-Tabelle
(PostgreSQL). Additive Knowledge-Kette: Investigation-Ergebnisse werden
genauso persistiert wie die urspruenglichen Beobachtungen, beide bleiben
nebeneinander bestehen (schema-loses payload/JSONB macht das moeglich,
ohne die Tabellenstruktur je Event-Type anzupassen).

Reiner Consumer: kein Publish zu irgendeiner Exchange. Die einzige
bewusste Ausnahme vom "immer nur Exchange-zu-Exchange"-Prinzip ist das
Schreiben nach Postgres selbst - ohne diese Ausnahme gaebe es keine
Persistenz.
"""

import json
import os
import time
from datetime import datetime, timezone
from uuid import uuid4

import pika
import psycopg2

from envelope_validation import InvalidEnvelopeError, validate_envelope

SERVICE_NAME = "persistence-service"

DEAD_LETTER_EXCHANGE = "sezra.stream.dead_letter"
DEAD_LETTER_ROUTING_KEY = f"{SERVICE_NAME}.failed"

INPUT_EXCHANGES = ["sezra.stream.validated", "sezra.stream.investigation"]
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

POSTGRES_HOST = required_env("POSTGRES_HOST")
POSTGRES_PORT = required_env("POSTGRES_PORT")
POSTGRES_USER = required_env("POSTGRES_USER")
POSTGRES_PASSWORD = required_env("POSTGRES_PASSWORD")
POSTGRES_DB = required_env("POSTGRES_DB")


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


def connect_to_postgres():
    while True:
        try:
            connection = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                dbname=POSTGRES_DB,
            )
            connection.autocommit = True
            return connection
        except psycopg2.OperationalError:
            print(f"[{SERVICE_NAME}] Postgres not ready yet. Retrying...")
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


INSERT_EVENT_SQL = """
    INSERT INTO events (
        id, event_id, schema_version, event_type, source,
        occurred_at, correlation_id, causation_id, project_id, payload
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (event_id) DO NOTHING
"""


def insert_event(db_connection, envelope: dict) -> bool:
    """
    Schreibt ein Envelope in die events-Tabelle.

    Gibt True zurueck, wenn tatsaechlich eine neue Zeile eingefuegt
    wurde, False bei einem Duplikat (event_id existiert bereits -
    ON CONFLICT DO NOTHING greift, kein Fehler).
    """
    with db_connection.cursor() as cursor:
        cursor.execute(
            INSERT_EVENT_SQL,
            (
                str(uuid4()),
                envelope["event_id"],
                envelope["schema_version"],
                envelope["event_type"],
                envelope["source"],
                envelope["occurred_at"],
                envelope.get("correlation_id"),
                envelope.get("causation_id"),
                envelope.get("project_id"),
                json.dumps(envelope["payload"]),
            ),
        )
        return cursor.rowcount > 0


def handle_message(channel, method, properties, body: bytes, db_connection) -> None:
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

    try:
        inserted = insert_event(db_connection, envelope)
    except psycopg2.Error as error:
        # Kein Dead-Letter, kein Ack: das Envelope selbst war gueltig,
        # das Problem liegt an der Datenbank (z. B. kurz nicht
        # erreichbar). Nachricht bleibt in der Queue, RabbitMQ liefert
        # sie erneut zu, statt sie stillschweigend zu verlieren.
        print(f"[{SERVICE_NAME}] Database error, will retry: {error}")
        return

    if inserted:
        print(f"[{SERVICE_NAME}] Persisted {envelope['event_type']} ({envelope['event_id']})")
    else:
        print(f"[{SERVICE_NAME}] Duplicate, already persisted: {envelope['event_id']}")

    channel.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    print(f"[{SERVICE_NAME}] starting")

    db_connection = connect_to_postgres()
    print(f"[{SERVICE_NAME}] connected to Postgres")

    connection = connect_to_rabbitmq()
    channel = connection.channel()

    channel.exchange_declare(exchange=DEAD_LETTER_EXCHANGE, exchange_type="fanout", durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    for exchange in INPUT_EXCHANGES:
        channel.exchange_declare(exchange=exchange, exchange_type="fanout", durable=True)
        channel.queue_bind(exchange=exchange, queue=QUEUE_NAME)

    print(f"[{SERVICE_NAME}] listening on queue: {QUEUE_NAME} (exchanges: {INPUT_EXCHANGES})")

    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=lambda ch, method, properties, body: handle_message(
            ch, method, properties, body, db_connection
        ),
    )
    channel.start_consuming()


if __name__ == "__main__":
    main()