"""
knowledge-service

Konsumiert von sezra.stream.validated UND sezra.stream.anomaly, erzeugt
eine deterministische, semantische Textbeschreibung (semantic_text) aus
den vorhandenen payload-Feldern - noch kein LLM-Aufruf, reine
Feld-zu-Text-Zusammensetzung.

Nur ContextIngested- und AnomalyDetected-Events werden tatsaechlich
angereichert (siehe ENRICHABLE_EVENT_TYPES) - eine normale Beobachtung
kann keine Anomalie ausloesen, sie beschreibt nur einen Normalzustand.
Ohne diese Einschraenkung fluteten Baseline-Werte den spaeteren
Ursachen-Kandidatenpool des Analyzers mit bedeutungslosen, aber zeitlich
zufaellig passenden Treffern.

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

INPUT_EXCHANGES = ["sezra.stream.validated", "sezra.stream.anomaly"]
OUTPUT_EXCHANGE = "sezra.stream.enriched.semantic"

# Nur diese Event-Types werden angereichert/vektorisiert - eine normale
# Beobachtung ("Conversion-Rate war heute 3.6%, wie ueblich") kann keine
# Anomalie ausloesen, sie beschreibt nur einen Normalzustand. Nur
# Kontext-Events (bewusst eingereichter Text) und bereits erkannte
# Anomalien sind sinnvolle Ursachen-Kandidaten fuer den spaeteren
# Analyzer. Gefunden, weil der Analyzer sonst triviale Baseline-Werte
# als "Ursache" fuer andere Anomalien vorschlug, nur weil sie zeitlich
# davor lagen und strukturell aehnlichen Text erzeugten.
ENRICHABLE_EVENT_TYPES = {"ContextIngested", "AnomalyDetected"}
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


# Technische Pipeline-Metadaten, nie fachlicher Inhalt - wuerden den
# Einbettungstext mit bedeutungslosem Rauschen (UUIDs, internen
# Zeitstempeln) verunreinigen, wenn sie mit eingebettet wuerden. Gilt
# uebergreifend fuer jeden Event-Type, der sie fuehrt (z. B.
# AnomalyDetected).
NON_SEMANTIC_FIELDS = {"composite_key", "source_event_id", "source_event_type", "source_occurred_at"}


def build_semantic_text(payload: dict) -> str:
    """
    Generische Feld-zu-Text-Zusammensetzung, bewusst ohne Wissen ueber
    bestimmte Feldnamen (kein "if metric_name == ...") - funktioniert
    gleichermassen fuer School-, Healthcare- oder Manufacturing-Payloads,
    weil sie einfach ueber alle vorhandenen Felder iteriert (ausser
    technischen Pipeline-Feldern, siehe NON_SEMANTIC_FIELDS).

    Macht nicht nur Feldnamen (Schluessel) lesbar, sondern auch
    Werte, die wie Bezeichner aussehen (Unterstriche enthalten, z. B.
    ein Metrik-Name wie "customs_clearance_delay_hours") - bewusst
    weiterhin generisch (jeder String mit Unterstrich, kein Wissen
    ueber "das ist ein Metrik-Name"). Gefunden, als ein LLM beim
    Kausal-Rerank zwei rohe Metrik-Bezeichner kaum als zusammenhaengend
    einstufte, obwohl der Zusammenhang bei lesbarer Formulierung
    ("customs clearance delay hours") eher erkennbar sein sollte.
    """
    parts = []
    for key, value in payload.items():
        if key in NON_SEMANTIC_FIELDS:
            continue
        readable_key = key.replace("_", " ")
        readable_value = value.replace("_", " ") if isinstance(value, str) else value
        parts.append(f"{readable_key}: {readable_value}")
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

    if envelope["event_type"] not in ENRICHABLE_EVENT_TYPES:
        # Kein Fehler - z. B. eine normale ObservationIngested-Beobachtung
        # wird bewusst nicht angereichert/vektorisiert (siehe
        # ENRICHABLE_EVENT_TYPES). Sie bleibt fuer die Anomalieerkennung
        # selbst nutzbar (deviation-detector-service) und wird weiterhin
        # in Postgres gespeichert, taucht aber nie als Ursachen-Kandidat
        # in der Vektorsuche auf.
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

    channel.exchange_declare(exchange=OUTPUT_EXCHANGE, exchange_type="fanout", durable=True)
    channel.exchange_declare(exchange=DEAD_LETTER_EXCHANGE, exchange_type="fanout", durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    for exchange in INPUT_EXCHANGES:
        channel.exchange_declare(exchange=exchange, exchange_type="fanout", durable=True)
        channel.queue_bind(exchange=exchange, queue=QUEUE_NAME)

    print(f"[{SERVICE_NAME}] listening on queue: {QUEUE_NAME} (exchanges: {INPUT_EXCHANGES})")

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=handle_message)
    channel.start_consuming()


if __name__ == "__main__":
    main()
