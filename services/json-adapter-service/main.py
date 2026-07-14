"""
json-adapter-service

Beobachtet einen Ordner mit rohen JSON-Dateien, verpackt jede Datei in ein
gueltiges SEZRA-Envelope und published es zu RabbitMQ.

Reiner Producer: kein RabbitMQ-Input, kein Consumer-Teil. Der Input ist
das Dateisystem.
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pika

from envelope_validation import InvalidEnvelopeError, validate_envelope

SERVICE_NAME = "json-adapter-service"

OUTPUT_EXCHANGE = "sezra.stream.raw"
DEAD_LETTER_EXCHANGE = "sezra.stream.dead_letter"
DEAD_LETTER_ROUTING_KEY = f"{SERVICE_NAME}.failed"

INBOX_DIR = Path(os.getenv("JSON_ADAPTER_INBOX_DIR", "/data/json-inbox"))
PROCESSED_DIR = INBOX_DIR / "processed"
FAILED_DIR = INBOX_DIR / "failed"

POLL_INTERVAL_SECONDS = float(os.getenv("JSON_ADAPTER_POLL_INTERVAL_SECONDS", "2"))

# source_type (im Rohdaten-JSON) -> event_type (im erzeugten Envelope)
SOURCE_TYPE_TO_EVENT_TYPE = {
    "observation": "ObservationIngested",
    "context": "ContextIngested",
}
FALLBACK_EVENT_TYPE = "RawDataIngested"


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


RABBITMQ_HOST = required_env("RABBITMQ_HOST")
RABBITMQ_PORT = int(required_env("RABBITMQ_PORT"))
RABBITMQ_USER = required_env("RABBITMQ_USER")
RABBITMQ_PASSWORD = required_env("RABBITMQ_PASSWORD")

# Mandatory, nicht optional: ohne feste Projekt-Zuordnung pro
# Adapter-Instanz landen Daten unterschiedlicher Einsatzszenarien
# vermischt in der Pipeline (siehe project_id im Envelope-Schema).
SEZRA_PROJECT_ID = required_env("SEZRA_PROJECT_ID")


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


def derive_event_type(raw_data: dict) -> str:
    source_type = raw_data.get("source_type")
    return SOURCE_TYPE_TO_EVENT_TYPE.get(source_type, FALLBACK_EVENT_TYPE)


def build_envelope(raw_data: dict) -> dict:
    event_id = str(uuid4())
    return {
        "schema_version": "1.1",
        "event_id": event_id,
        "event_type": derive_event_type(raw_data),
        "source": SERVICE_NAME,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "project_id": SEZRA_PROJECT_ID,
        # Selbstreferenziell: dieses Envelope ist der Ursprung einer neuen
        # Kette. Ohne das bleibt correlation_id null, und nachgelagerte
        # Services (die per "envelope.get('correlation_id') or
        # source_event_id" auf die event_id ausweichen) koennen die Kette
        # nie bis zum Ursprung zurueckverfolgen - der erste Link fehlt.
        "correlation_id": event_id,
        "payload": raw_data,
    }


def publish_envelope(channel, envelope: dict) -> None:
    channel.basic_publish(
        exchange=OUTPUT_EXCHANGE,
        routing_key="",
        body=json.dumps(envelope).encode("utf-8"),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )


def process_file(channel, file_path: Path) -> None:
    raw_text = file_path.read_text(encoding="utf-8")

    try:
        raw_data = json.loads(raw_text)
    except json.JSONDecodeError as error:
        print(f"[{SERVICE_NAME}] Invalid JSON in {file_path.name}: {error}")
        publish_dead_letter(channel, raw_text.encode("utf-8"), f"Invalid JSON: {error}", "permanent")
        file_path.rename(FAILED_DIR / file_path.name)
        return

    if not isinstance(raw_data, dict):
        print(f"[{SERVICE_NAME}] {file_path.name} does not contain a JSON object")
        publish_dead_letter(
            channel, raw_text.encode("utf-8"), "Top-level JSON must be an object", "permanent"
        )
        file_path.rename(FAILED_DIR / file_path.name)
        return

    envelope = build_envelope(raw_data)

    try:
        validate_envelope(envelope)
    except InvalidEnvelopeError as error:
        # Wir haben das Envelope selbst gebaut - ein Fehler hier ist ein
        # Bug in diesem Service, kein Problem der Rohdaten.
        print(f"[{SERVICE_NAME}] BUG: self-built envelope is invalid: {error}")
        publish_dead_letter(
            channel, raw_text.encode("utf-8"), f"Self-built envelope invalid: {error}", "permanent"
        )
        file_path.rename(FAILED_DIR / file_path.name)
        return

    publish_envelope(channel, envelope)
    print(
        f"[{SERVICE_NAME}] Ingested {file_path.name} -> "
        f"{envelope['event_type']} ({envelope['event_id']})"
    )
    file_path.rename(PROCESSED_DIR / file_path.name)


def main() -> None:
    print(f"[{SERVICE_NAME}] starting")

    INBOX_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    FAILED_DIR.mkdir(parents=True, exist_ok=True)

    connection = connect_to_rabbitmq()
    channel = connection.channel()

    channel.exchange_declare(exchange=OUTPUT_EXCHANGE, exchange_type="fanout", durable=True)
    channel.exchange_declare(exchange=DEAD_LETTER_EXCHANGE, exchange_type="fanout", durable=True)

    print(f"[{SERVICE_NAME}] watching {INBOX_DIR} every {POLL_INTERVAL_SECONDS}s")

    while True:
        json_files = sorted(INBOX_DIR.glob("*.json"))

        for file_path in json_files:
            try:
                process_file(channel, file_path)
            except Exception as error:
                # Unerwarteter Fehler (z. B. RabbitMQ-Verbindung kurz weg).
                # Datei NICHT verschieben, damit sie beim naechsten Poll
                # erneut versucht wird.
                print(f"[{SERVICE_NAME}] Unexpected error processing {file_path.name}: {error}")

        # connection.sleep() statt time.sleep(): verarbeitet nebenbei
        # eingehende Heartbeat-Frames von RabbitMQ. Mit time.sleep()
        # bleibt die Verbindung waehrend des Wartens "taub" - RabbitMQ
        # schliesst sie nach einiger Zeit wegen "missed heartbeats",
        # selbst wenn der Service technisch noch laeuft.
        connection.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
