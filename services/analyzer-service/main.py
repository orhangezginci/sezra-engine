"""
analyzer-service

Konsumiert von sezra.stream.anomaly, sucht in Qdrant nach semantisch
verwandtem Kontext (z. B. eine Rektor-Mail), und published eine
strukturierte Investigation zu sezra.stream.investigation.

Drei bewusste Lehren aus dem urspruenglichen SEZRA-Analyzer, hier von
Anfang an eingebaut, nicht nachtraeglich geflickt:

1. Zeitfilter: Kandidaten, die NACH der Anomalie liegen, werden verworfen
   - eine Mail, die nach dem Notenabfall verschickt wurde, kann nicht
   dessen Ursache sein.
2. Sichtbarer Confidence-Score: der Qdrant-Similarity-Score steht direkt
   im Ergebnis, nicht versteckt.
3. Unsicherheits-Fallback: unterhalb eines Schwellwerts wird ehrlich
   "keine ueberzeugende Erklaerung gefunden" gemeldet, statt schwache
   Treffer als vermeintliche Erklaerung zu praesentieren.

Reiner Consumer mit Publish (kein Producer-only, kein Consumer-only):
konsumiert Anomalien, published strukturierte Investigations.
"""

import json
import os
import time
from datetime import datetime, timezone
from uuid import uuid4

import pika
import requests
from qdrant_client import QdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchValue

from envelope_validation import InvalidEnvelopeError, validate_envelope

SERVICE_NAME = "analyzer-service"

DEAD_LETTER_EXCHANGE = "sezra.stream.dead_letter"
DEAD_LETTER_ROUTING_KEY = f"{SERVICE_NAME}.failed"

INPUT_EXCHANGE = "sezra.stream.anomaly"
OUTPUT_EXCHANGE = "sezra.stream.investigation"
QUEUE_NAME = f"sezra.queue.{SERVICE_NAME}"

QDRANT_COLLECTION_NAME = "sezra_semantic"
SEARCH_LIMIT = int(os.getenv("ANALYZER_SEARCH_LIMIT", "5"))
CONFIDENCE_THRESHOLD = float(os.getenv("ANALYZER_CONFIDENCE_THRESHOLD", "0.5"))


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
OLLAMA_GENERATION_MODEL = required_env("OLLAMA_GENERATION_MODEL")


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


def build_anomaly_search_text(payload: dict) -> str:
    """
    Baut eine natuerlichsprachige Beschreibung der Anomalie statt einer
    knappen "key: value"-Aneinanderreihung. Embedding-Modelle bewerten
    die Aehnlichkeit zwischen strukturell verschiedenen, aber inhaltlich
    verwandten Texten (z. B. eine kurze Metrik-Beschreibung vs. natuerlich-
    sprachiger Mailtext) tendenziell hoeher, wenn beide Seiten eher wie
    natuerliche Sprache formuliert sind - eine reine "field: value"-Liste
    liegt stilistisch naeher an anderen "field: value"-Listen (z. B.
    weiteren Metrik-Beobachtungen) als an Fliesstext.

    Domaenenagnostisch: kein Wissen ueber bestimmte Metrik-Namen, nur
    generische Satzbausteine aus den vorhandenen Feldern.
    """
    metric = payload.get("metric")
    anomaly_type = payload.get("anomaly_type", "change")
    previous_value = payload.get("previous_value")
    current_value = payload.get("current_value")
    reason = payload.get("reason", "")

    if metric and previous_value is not None and current_value is not None:
        sentence = (
            f"The metric {metric} showed a significant {anomaly_type}, "
            f"changing from {previous_value} to {current_value}."
        )
    elif metric:
        sentence = f"An anomaly was detected for the metric {metric}."
    else:
        sentence = "An anomaly was detected."

    if reason:
        sentence += f" {reason.capitalize()}."

    return sentence


def create_embedding(text: str) -> list[float]:
    """
    nomic-embed-text erwartet ein Task-Prefix - "search_query:" hier,
    weil dieser Text eine Suchanfrage gegen die in vectorizing-service
    mit "search_document:" gespeicherten Vektoren ist. Beide Seiten
    muessen das jeweils passende Prefix nutzen, sonst sind die Embeddings
    nicht optimal fuer Retrieval kalibriert.
    """
    prefixed_text = f"search_query: {text}"

    response = requests.post(
        f"http://{OLLAMA_HOST}:{OLLAMA_PORT}/api/embeddings",
        json={"model": OLLAMA_EMBEDDING_MODEL, "prompt": prefixed_text},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["embedding"]


def generate_causal_explanation(anomaly_summary: str, cause_text: str) -> str | None:
    """
    Nutzt ein generatives Modell (nicht das Embedding-Modell), um in
    eigenen Worten zu erklaeren, WIE der gefundene Kontext zur Anomalie
    gefuehrt haben koennte - Ergaenzung zum rohen semantic_text, nicht
    Ersatz dafuer (Transparenz/Nachvollziehbarkeit bleibt erhalten).

    Bewusst vorsichtig formuliert im Prompt: keine Tatsachenbehauptung,
    da es sich weiterhin nur um eine semantische Korrelation handelt,
    keine bewiesene Kausalitaet (siehe confidence_note).

    Gibt None zurueck statt zu werfen, wenn die Generierung fehlschlaegt -
    die Investigation soll trotzdem mit dem rohen semantic_text nutzbar
    bleiben, auch ohne generierte Erklaerung.
    """
    prompt = (
        f"Beobachtete Anomalie: {anomaly_summary}\n"
        f"Moeglicher Ausloeser: \"{cause_text}\"\n\n"
        "Erklaere in GENAU EINEM vollstaendigen Satz auf Deutsch die "
        "Wirkungskette: WARUM koennte dieser Ausloeser konkret zu GENAU "
        "DIESER Anomalie gefuehrt haben? Nenne einen plausiblen "
        "Zwischenschritt (z. B. Auswirkung auf Konzentration, Zeit, "
        "Ressourcen). Nutze \"koennte\" oder \"moeglicherweise\". Nur der "
        "eine Satz, keine Wiederholung der Eingabe."
    )

    try:
        response = requests.post(
            f"http://{OLLAMA_HOST}:{OLLAMA_PORT}/api/generate",
            json={
                "model": OLLAMA_GENERATION_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    # Niedrige temperature gegen thematisches Abschweifen,
                    # num_predict begrenzt die Antwortlaenge hart, statt
                    # nur per Prompt-Anweisung ("ein Satz") zu hoffen, dass
                    # das Modell sich daran haelt - qwen2.5:1.5b ignoriert
                    # Laengenvorgaben im Prompt sonst zuverlaessig.
                    "temperature": 0.3,
                    "num_predict": 150,
                },
            },
            timeout=180,
        )
        response.raise_for_status()
        return response.json()["response"].strip()
    except (requests.RequestException, KeyError, ValueError) as error:
        print(f"[{SERVICE_NAME}] Explanation generation failed, continuing without it: {error}")
        return None


def search_related_context(
    qdrant_client: QdrantClient,
    vector: list[float],
    project_id: str | None,
    anomaly_occurred_at: str,
    anomaly_composite_key: str | None,
) -> list[dict]:
    """
    Sucht die naechsten Nachbarn in Qdrant, gefiltert nach project_id
    (Isolation zwischen Einsatzszenarien), zeitlich VOR der Anomalie
    (Kausalitaets-Plausibilitaet: eine Ursache kann nicht nach ihrer
    Wirkung liegen), und schliesst Kandidaten mit demselben composite_key
    wie die Anomalie selbst aus (eine Beobachtungsreihe kann sich nicht
    selbst erklaeren - "math_test_average war neulich auch mal 79" ist
    keine Ursache fuer "math_test_average ist jetzt 45", das ist nur ein
    weiterer Messpunkt derselben Reihe). Eine ANDERE Metrik-Reihe oder ein
    Kontext-Event (composite_key ist dort None) bleibt weiterhin ein
    legitimer Kandidat.

    Zeit- und composite_key-Filter passieren client-seitig in Python,
    nicht als Qdrant-Filter - einfacher zu lesen und zu testen als
    Qdrant-Filter auf nicht dafuer indizierten Feldern.
    """
    query_filter = Filter(
        must=[FieldCondition(key="project_id", match=MatchValue(value=project_id))]
    ) if project_id else None

    response = qdrant_client.query_points(
        collection_name=QDRANT_COLLECTION_NAME,
        query=vector,
        query_filter=query_filter,
        limit=SEARCH_LIMIT * 2,  # grosszuegiger holen, da nachgelagerte Filter noch aussieben
    )

    candidates = []
    for point in response.points:
        candidate_occurred_at = point.payload.get("occurred_at")
        candidate_composite_key = point.payload.get("composite_key")

        occurred_before_anomaly = (
            candidate_occurred_at is not None and candidate_occurred_at < anomaly_occurred_at
        )
        is_same_series_as_anomaly = (
            anomaly_composite_key is not None
            and candidate_composite_key == anomaly_composite_key
        )

        if is_same_series_as_anomaly:
            continue

        candidates.append(
            {
                "semantic_text": point.payload.get("semantic_text"),
                "confidence": point.score,
                "source_event_id": point.payload.get("event_id"),
                "occurred_at": candidate_occurred_at,
                "occurred_before_anomaly": occurred_before_anomaly,
            }
        )

    plausible = [c for c in candidates if c["occurred_before_anomaly"]]
    plausible.sort(key=lambda c: c["confidence"], reverse=True)
    return plausible[:SEARCH_LIMIT]


def build_investigation_payload(anomaly_envelope: dict, candidates: list[dict]) -> dict:
    payload = anomaly_envelope["payload"]
    anomaly_summary = (
        f"{payload.get('metric')} changed from {payload.get('previous_value')} "
        f"to {payload.get('current_value')} ({payload.get('anomaly_type')})"
    )

    confident_candidates = [c for c in candidates if c["confidence"] >= CONFIDENCE_THRESHOLD]

    if not confident_candidates:
        return {
            "anomaly_summary": anomaly_summary,
            "possible_causes": [],
            "confidence_note": (
                "No context above the confidence threshold "
                f"({CONFIDENCE_THRESHOLD}) was found. This does not mean "
                "there is no cause - only that no sufficiently similar "
                "context exists in the available data."
            ),
        }

    # Nur fuer bereits bestaetigte (ueber dem Threshold liegende) Kandidaten
    # eine Erklaerung generieren - spart Rechenzeit und vermeidet, dass das
    # Modell plausibel klingende Geschichten zu eigentlich verworfenen,
    # schwachen Treffern erfindet.
    for candidate in confident_candidates:
        candidate["explanation"] = generate_causal_explanation(
            anomaly_summary, candidate["semantic_text"]
        )

    return {
        "anomaly_summary": anomaly_summary,
        "possible_causes": confident_candidates,
        "confidence_note": (
            "Results are based on semantic similarity, not proven causality."
        ),
    }


def create_investigation_event(anomaly_envelope: dict, investigation_payload: dict) -> dict:
    anomaly_event_id = anomaly_envelope["event_id"]

    return {
        "schema_version": "1.1",
        "event_id": str(uuid4()),
        "event_type": "InvestigationGenerated",
        "source": SERVICE_NAME,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "correlation_id": anomaly_envelope.get("correlation_id") or anomaly_event_id,
        "causation_id": anomaly_event_id,
        "project_id": anomaly_envelope.get("project_id"),
        "payload": investigation_payload,
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
    anomaly_occurred_at = payload.get("source_occurred_at", envelope["occurred_at"])
    anomaly_composite_key = payload.get("composite_key")
    search_text = build_anomaly_search_text(payload)

    try:
        vector = create_embedding(search_text)
    except requests.RequestException as error:
        print(f"[{SERVICE_NAME}] Ollama error, will retry: {error}")
        return

    try:
        candidates = search_related_context(
            qdrant_client, vector, envelope.get("project_id"), anomaly_occurred_at, anomaly_composite_key
        )
    except Exception as error:
        print(f"[{SERVICE_NAME}] Qdrant error, will retry: {error}")
        return

    investigation_payload = build_investigation_payload(envelope, candidates)
    investigation_event = create_investigation_event(envelope, investigation_payload)

    channel.basic_publish(
        exchange=OUTPUT_EXCHANGE,
        routing_key="",
        body=json.dumps(investigation_event).encode("utf-8"),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )

    print(
        f"[{SERVICE_NAME}] Investigated {envelope['event_type']} ({envelope['event_id']}): "
        f"{len(investigation_payload['possible_causes'])} cause(s) found"
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
