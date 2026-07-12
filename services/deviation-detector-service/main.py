"""
deviation-detector-service

Konsumiert von sezra.stream.validated, erkennt statistische Abweichungen
(Z-Score) bei Metrik-Beobachtungen und published gefundene Anomalien zu
sezra.stream.anomaly.

Domaenenagnostisch: kennt keine bestimmten Metrik-Namen, arbeitet nur mit
den generischen Feldern "metric" und "value" im Payload.

Composite-Key statt nur "metric": die Historie wird nach "metric" PLUS
allen weiteren Payload-Feldern (ausser "value" und "source_type")
gruppiert. Ohne das wuerden z. B. Grade-7- und Grade-8-Beobachtungen mit
demselben metric-Namen sich faelschlich eine gemeinsame Baseline teilen
(Bug, der im urspruenglichen SEZRA-Detector so vorlag).

Bekannte Einschraenkung (bewusst, kein Overengineering fuer jetzt):
Die Historie lebt nur im Prozessspeicher. Ein Neustart dieses Service
loescht die gelernte Baseline vollstaendig - fuer produktiven Einsatz
waere eine externe Persistenz (z. B. Redis) ein sinnvoller naechster
Schritt, aber kein Blocker fuer den aktuellen Zweck.
"""

import json
import os
import time
from datetime import datetime, timezone
from enum import Enum
from uuid import uuid4

import numpy as np
import pika

from envelope_validation import InvalidEnvelopeError, validate_envelope

SERVICE_NAME = "deviation-detector-service"

DEAD_LETTER_EXCHANGE = "sezra.stream.dead_letter"
DEAD_LETTER_ROUTING_KEY = f"{SERVICE_NAME}.failed"

INPUT_EXCHANGE = "sezra.stream.validated"
OUTPUT_EXCHANGE = "sezra.stream.anomaly"
QUEUE_NAME = f"sezra.queue.{SERVICE_NAME}"

# Felder, die NIE Teil des Gruppierungs-Schluessels sind: "value" ist der
# eigentliche Messwert, "source_type" ist reine Adapter-Metadatum, keine
# fachliche Dimension.
NON_DIMENSION_FIELDS = {"value", "source_type"}

MIN_HISTORY_SIZE = int(os.getenv("DETECTOR_MIN_HISTORY_SIZE", "5"))
MAX_HISTORY_SIZE = int(os.getenv("DETECTOR_MAX_HISTORY_SIZE", "100"))
STDDEV_MULTIPLIER = float(os.getenv("DETECTOR_STDDEV_MULTIPLIER", "2.0"))

metric_history: dict[str, list[float]] = {}


class DeviationType(str, Enum):
    SPIKE = "spike"
    DROP = "drop"


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


def is_observation(payload: dict) -> bool:
    return "metric" in payload and "value" in payload


def build_composite_key(payload: dict) -> str:
    """
    "metric" plus alle weiteren Dimensions-Felder, sortiert (damit die
    Reihenfolge im payload-Dict keinen Einfluss auf den Key hat).
    """
    dimension_items = sorted(
        (key, value)
        for key, value in payload.items()
        if key not in NON_DIMENSION_FIELDS and key != "metric"
    )
    dimension_suffix = "|".join(f"{k}={v}" for k, v in dimension_items)
    metric = payload["metric"]
    return f"{metric}|{dimension_suffix}" if dimension_suffix else str(metric)


def detect_deviation(
    composite_key: str,
    current_value: float,
) -> tuple[DeviationType | None, float | None]:
    history = metric_history.get(composite_key, [])

    if len(history) < MIN_HISTORY_SIZE:
        history.append(current_value)
        metric_history[composite_key] = history
        print(
            f"[{SERVICE_NAME}] History initialized for {composite_key} "
            f"({len(history)}/{MIN_HISTORY_SIZE})"
        )
        return None, None

    previous_value = history[-1]
    mean = np.mean(history)
    stddev = np.std(history)

    history.append(current_value)
    if len(history) > MAX_HISTORY_SIZE:
        history = history[-MAX_HISTORY_SIZE:]
    metric_history[composite_key] = history

    if stddev == 0:
        return None, previous_value

    z_score = (current_value - mean) / stddev
    print(f"[{SERVICE_NAME}] {composite_key}: mean={mean:.2f} stddev={stddev:.2f} z={z_score:.2f}")

    if z_score >= STDDEV_MULTIPLIER:
        return DeviationType.SPIKE, previous_value
    if z_score <= -STDDEV_MULTIPLIER:
        return DeviationType.DROP, previous_value

    return None, previous_value


def create_anomaly_event(
    envelope: dict,
    composite_key: str,
    previous_value: float,
    current_value: float,
    deviation_type: DeviationType,
) -> dict:
    source_event_id = envelope["event_id"]
    change_amount = current_value - previous_value

    if deviation_type == DeviationType.SPIKE:
        reason = "value increased significantly compared to recent history"
    else:
        reason = "value decreased significantly compared to recent history"

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
            "anomaly_type": deviation_type.value,
            "metric": envelope["payload"]["metric"],
            "composite_key": composite_key,
            "previous_value": previous_value,
            "current_value": current_value,
            "change_amount": change_amount,
            "reason": reason,
            "source_event_id": source_event_id,
            # Payload-Konvention wie in knowledge-service: occurred_at
            # oben ist dieses Envelopes eigene Erzeugungszeit. Das
            # konsumierte Envelope kommt unveraendert von
            # ingestion-service durch, also ist envelope["occurred_at"]
            # hier tatsaechlich die echte Adapter-Erfassungszeit - der
            # spaetere Analyzer braucht sie fuer die zeitliche
            # Kausalitaets-Pruefung.
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

    payload = envelope["payload"]

    if not is_observation(payload):
        # Kein Fehler - dieses Envelope ist schlicht keine Metrik-
        # Beobachtung (z. B. ein ContextIngested-Event). Kein
        # Dead-Letter, einfach ignorieren.
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    try:
        current_value = float(payload["value"])
    except (TypeError, ValueError) as error:
        publish_dead_letter(channel, body, f"Non-numeric value: {error}", "permanent")
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    composite_key = build_composite_key(payload)
    deviation_type, previous_value = detect_deviation(composite_key, current_value)

    if deviation_type and previous_value is not None:
        anomaly_event = create_anomaly_event(
            envelope, composite_key, previous_value, current_value, deviation_type
        )
        channel.basic_publish(
            exchange=OUTPUT_EXCHANGE,
            routing_key="",
            body=json.dumps(anomaly_event).encode("utf-8"),
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        )
        print(
            f"[{SERVICE_NAME}] {deviation_type.value.upper()} detected for {composite_key}: "
            f"{previous_value} -> {current_value}"
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
