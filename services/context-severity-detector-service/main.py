"""
context-severity-detector-service

Konsumiert von sezra.stream.validated, verarbeitet nur ContextIngested-
Events. Bewertet JEDE einzelne Nachricht per LLM auf einer
Dringlichkeits-Skala (0.0-1.0) - im Gegensatz zu deviation-detector-
service (statistische Abweichung ueber Zeit) reicht hier bereits EIN
einzelner Text, um eine Anomalie auszuloesen (z. B. "Login nicht
moeglich" ist sofort untersuchungswuerdig, unabhaengig von Wiederholung).

Ergaenzt, nicht ersetzt, einen spaeteren Volumen-basierten Detector
(context-volume-detector-service, noch nicht gebaut) fuer Faelle, die
erst bei gehaeuftem Auftreten relevant werden (z. B. "Seitenaufbau
teilweise langsam").

Domaenenagnostisch: kein Keyword-Filter, das LLM beurteilt generisch,
nicht anhand bestimmter Woerter.
"""

import json
import os
import time
from datetime import datetime, timezone
from uuid import uuid4

import pika
import requests

from envelope_validation import InvalidEnvelopeError, validate_envelope

SERVICE_NAME = "context-severity-detector-service"

DEAD_LETTER_EXCHANGE = "sezra.stream.dead_letter"
DEAD_LETTER_ROUTING_KEY = f"{SERVICE_NAME}.failed"

INPUT_EXCHANGE = "sezra.stream.validated"
OUTPUT_EXCHANGE = "sezra.stream.anomaly"
QUEUE_NAME = f"sezra.queue.{SERVICE_NAME}"

# Felder, die nie Teil der zu bewertenden Textzusammenfassung sind -
# reine Adapter-Metadaten, kein fachlicher Inhalt.
NON_CONTENT_FIELDS = {"source_type"}


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


RABBITMQ_HOST = required_env("RABBITMQ_HOST")
RABBITMQ_PORT = int(required_env("RABBITMQ_PORT"))
RABBITMQ_USER = required_env("RABBITMQ_USER")
RABBITMQ_PASSWORD = required_env("RABBITMQ_PASSWORD")

SEVERITY_THRESHOLD = float(os.getenv("SEVERITY_THRESHOLD", "0.8"))

OLLAMA_HOST = None
OLLAMA_PORT = None
OLLAMA_GENERATION_MODEL = None
OPENAI_API_KEY = None
OPENAI_GENERATION_MODEL = None
OPENAI_BASE_URL = None
GEMINI_API_KEY = None
GEMINI_GENERATION_MODEL = None

# Gleicher Mechanismus wie in analyzer-service (bewusst dupliziert, keine
# geteilte Bibliothek): welcher Anbieter fuer die LLM-Bewertung genutzt
# wird, hat Datenschutz-Implikationen (Ollama lokal vs. Cloud) - niemals
# stillschweigend ein Default.
LLM_PROVIDER = required_env("LLM_PROVIDER").lower()

if LLM_PROVIDER == "ollama":
    OLLAMA_HOST = required_env("OLLAMA_HOST")
    OLLAMA_PORT = required_env("OLLAMA_PORT")
    OLLAMA_GENERATION_MODEL = required_env("OLLAMA_GENERATION_MODEL")
elif LLM_PROVIDER == "openai":
    OPENAI_API_KEY = required_env("OPENAI_API_KEY")
    OPENAI_GENERATION_MODEL = required_env("OPENAI_GENERATION_MODEL")
    OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1"
elif LLM_PROVIDER == "gemini":
    GEMINI_API_KEY = required_env("GEMINI_API_KEY")
    GEMINI_GENERATION_MODEL = required_env("GEMINI_GENERATION_MODEL")
else:
    raise RuntimeError(
        f"Unknown LLM_PROVIDER: '{LLM_PROVIDER}'. Must be 'ollama', 'openai', or 'gemini'."
    )


def connect_to_rabbitmq() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(username=RABBITMQ_USER, password=RABBITMQ_PASSWORD)
    while True:
        try:
            return pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials
                )
            )
        except pika.exceptions.AMQPConnectionError:
            print(f"[{SERVICE_NAME}] RabbitMQ not ready yet. Retrying...")
            time.sleep(3)


def build_context_text(payload: dict) -> str:
    """
    Generische Feld-zu-Text-Zusammensetzung, analog zu knowledge-
    service's build_semantic_text - domaenenagnostisch, kein Wissen
    ueber bestimmte Feldnamen.
    """
    parts = []
    for key, value in payload.items():
        if key in NON_CONTENT_FIELDS:
            continue
        readable_key = key.replace("_", " ")
        parts.append(f"{readable_key}: {value}")
    return "; ".join(parts)


def build_severity_prompt(text: str) -> str:
    return (
        f"Text: \"{text}\"\n\n"
        "Bewerte auf einer Skala von 0.0 bis 1.0, wie dringend/"
        "schwerwiegend dieser Text ein Problem beschreibt, das eine "
        "sofortige Untersuchung rechtfertigt.\n\n"
        "Orientierung an konkreten Beispielen:\n"
        "0.9-1.0: Kernfunktion komplett unbenutzbar fuer den Nutzer "
        "(z. B. Login/Bezahlung/Anmeldung funktioniert gar nicht, "
        "Datenverlust, Sicherheitsvorfall).\n"
        "0.5-0.7: Funktion beeintraechtigt, aber nutzbar (z. B. "
        "einzelne Fehlermeldung, ein Feature funktioniert nicht wie "
        "erwartet).\n"
        "0.1-0.3: Kleinere Unannehmlichkeit, kein Blocker (z. B. "
        "Performance, optisches Detail).\n"
        "0.0: kein Problem, rein informativ.\n\n"
        "Antworte AUSSCHLIESSLICH mit der Zahl, keine Erklaerung, kein "
        "zusaetzlicher Text."
    )


def _parse_score(raw_text: str) -> float:
    # Modelle haengen gelegentlich Satzzeichen/Leerzeichen an, auch bei
    # strikter Anweisung - robust extrahieren statt blind float() zu
    # versuchen.
    cleaned = raw_text.strip().split()[0].rstrip(".,;:")
    score = float(cleaned)
    return max(0.0, min(1.0, score))


def _get_score_via_ollama(prompt: str) -> float:
    response = requests.post(
        f"http://{OLLAMA_HOST}:{OLLAMA_PORT}/api/generate",
        json={
            "model": OLLAMA_GENERATION_MODEL,
            "prompt": prompt,
            "stream": False,
            "think": False,
            "options": {"temperature": 0.1, "num_predict": 10},
        },
        timeout=180,
    )
    response.raise_for_status()
    return _parse_score(response.json()["response"])


def _get_score_via_openai(prompt: str) -> float:
    response = requests.post(
        f"{OPENAI_BASE_URL}/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
        json={
            "model": OPENAI_GENERATION_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": 10,
        },
        timeout=60,
    )
    response.raise_for_status()
    return _parse_score(response.json()["choices"][0]["message"]["content"])


def _get_score_via_gemini(prompt: str) -> float:
    response = requests.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{GEMINI_GENERATION_MODEL}:generateContent",
        params={"key": GEMINI_API_KEY},
        json={
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.1,
                "maxOutputTokens": 300,
                "thinkingConfig": {"thinkingBudget": 0},
            },
        },
        timeout=60,
    )
    response.raise_for_status()
    return _parse_score(response.json()["candidates"][0]["content"]["parts"][0]["text"])


def get_severity_score(text: str) -> float | None:
    """
    Gibt None zurueck statt zu werfen, wenn die Bewertung fehlschlaegt -
    der Aufrufer entscheidet dann, die Nachricht nicht zu acken (Retry),
    analog zu den anderen LLM-Aufrufen in dieser Pipeline.
    """
    prompt = build_severity_prompt(text)

    if LLM_PROVIDER == "ollama":
        return _get_score_via_ollama(prompt)
    elif LLM_PROVIDER == "openai":
        return _get_score_via_openai(prompt)
    elif LLM_PROVIDER == "gemini":
        return _get_score_via_gemini(prompt)


def create_severity_anomaly_event(envelope: dict, text: str, score: float) -> dict:
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
            "anomaly_type": "severity",
            "severity_score": score,
            "text": text,
            "reason": (
                "a single message was rated highly urgent/severe by "
                "content evaluation, not a statistical deviation"
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

    if envelope["event_type"] != "ContextIngested":
        # Kein Fehler - dieser Service bewertet ausschliesslich
        # Kontext-Events, alles andere geht ihn nichts an.
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    text = build_context_text(envelope["payload"])

    try:
        score = get_severity_score(text)
    except (requests.RequestException, ValueError, IndexError, KeyError) as error:
        # Kein Dead-Letter, kein Ack: das Envelope war gueltig, das
        # Problem liegt am LLM-Aufruf (Timeout, unparsebare Antwort).
        # Nachricht bleibt in der Queue, wird erneut zugestellt.
        print(f"[{SERVICE_NAME}] Severity scoring failed, will retry: {error}")
        return

    if score >= SEVERITY_THRESHOLD:
        anomaly_event = create_severity_anomaly_event(envelope, text, score)
        channel.basic_publish(
            exchange=OUTPUT_EXCHANGE,
            routing_key="",
            body=json.dumps(anomaly_event).encode("utf-8"),
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        )
        print(f"[{SERVICE_NAME}] High severity ({score}) detected: {text[:80]}")
    else:
        print(f"[{SERVICE_NAME}] Severity {score} below threshold ({SEVERITY_THRESHOLD}), no anomaly")

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
