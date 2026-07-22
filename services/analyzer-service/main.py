"""
analyzer-service

Konsumiert von sezra.stream.anomaly, sucht in Qdrant nach semantisch
verwandtem Kontext (z. B. eine Rektor-Mail), bewertet die Kandidaten per
LLM auf tatsaechliche KAUSALE Plausibilitaet (nicht nur thematische
Naehe), und published eine strukturierte Investigation zu
sezra.stream.investigation.

Vier bewusste Lehren aus dem urspruenglichen SEZRA-Analyzer bzw. aus
spaeterer Live-Erprobung, hier eingebaut statt als bekannte Schwaeche
hingenommen:

1. Zeitfilter: Kandidaten, die NACH der Anomalie liegen, werden verworfen
   - eine Mail, die nach dem Notenabfall verschickt wurde, kann nicht
   dessen Ursache sein.
2. Sichtbarer Confidence-Score: im Ergebnis steht direkt, wie plausibel
   ein Kandidat bewertet wurde, nicht versteckt.
3. Unsicherheits-Fallback: unterhalb eines Schwellwerts wird ehrlich
   "keine ueberzeugende Erklaerung gefunden" gemeldet, statt schwache
   Treffer als vermeintliche Erklaerung zu praesentieren.
4. Kausal-Rerank statt reiner Vektor-Aehnlichkeit: Embeddings holen einen
   groesseren Kandidatenpool (thematische Naehe), ein LLM bewertet
   anschliessend echte kausale Plausibilitaet und ersetzt den rohen
   Embedding-Score. Gefunden im Severity-Demo: eine echte Ursache
   (Wartungsankuendigung, kaum Wortueberschneidung mit "Login nicht
   moeglich") wurde durchgaengig niedriger bewertet als oberflaechlich
   aehnlich klingende, aber irrelevante Nachrichten - auch mit einem
   staerkeren Embedding-Modell. Reine Vektor-Aehnlichkeit misst Thema,
   nicht Kausalitaet.

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
from fastembed import TextEmbedding
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
# Groesserer Pool VOR dem Rerank - die embedding-basierte Vorauswahl misst
# nur thematische Naehe, nicht kausale Plausibilitaet, die tatsaechliche
# Ursache kann also unter den Top SEARCH_LIMIT nach reinem Embedding-Score
# fehlen. Der Rerank-Schritt bewertet diesen groesseren Pool neu.
RERANK_POOL_SIZE = int(os.getenv("ANALYZER_RERANK_POOL_SIZE", "10"))

# Grenze zwischen "echtes Rauschen" und "unsicheres, aber reales Signal" -
# ein Kandidat unter CONFIDENCE_THRESHOLD wird nicht komplett verworfen,
# nur weil er nicht "sicher genug" ist. "Eine 0.2 ist nur relevant, wenn's
# keine 0.8 gibt" - SEZRA soll auch nicht-offensichtliche, unsichere
# Richtungen aufzeigen, nicht nur bewiesene Ursachen, aber klar getrennt
# von sicheren Funden praesentieren (siehe weak_leads in
# build_investigation_payload).
NOISE_FLOOR = float(os.getenv("ANALYZER_NOISE_FLOOR", "0.15"))
WEAK_LEADS_LIMIT = int(os.getenv("ANALYZER_WEAK_LEADS_LIMIT", "3"))


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
OLLAMA_GENERATION_MODEL = None
OPENAI_API_KEY = None
OPENAI_GENERATION_MODEL = None
OPENAI_BASE_URL = None
GEMINI_API_KEY = None
GEMINI_GENERATION_MODEL = None

# jina-embeddings-v2-base-de ist ein SYMMETRISCHES Modell (anders als
# nomic-embed-text) - kein "search_query:"/"search_document:"-Prefix
# noetig, dieselbe Funktion fuer Speichern (vectorizing-service) und
# Suchen (hier) nutzbar. Laeuft lokal im Prozess (FastEmbed/ONNX),
# unabhaengig von LLM_PROVIDER - Embeddings bleiben IMMER lokal, das ist
# eine bewusste Datenschutz-Entscheidung, nicht nur Bequemlichkeit.
EMBEDDING_MODEL_NAME = "jinaai/jina-embeddings-v2-base-de"
_embedding_model = TextEmbedding(model_name=EMBEDDING_MODEL_NAME)

# Entscheidet, welcher Anbieter fuer die Textgenerierung (nicht Embeddings -
# die bleiben immer Ollama, siehe OLLAMA_EMBEDDING_MODEL oben) genutzt wird.
# Mandatory, nicht optional mit Fallback: welcher Anbieter laeuft, hat
# direkte Datenschutz-Implikationen (Ollama = lokal, OpenAI/Gemini = Cloud) -
# das soll niemals stillschweigend "irgendwas" sein.
LLM_PROVIDER = required_env("LLM_PROVIDER").lower()

if LLM_PROVIDER == "ollama":
    OLLAMA_GENERATION_MODEL = required_env("OLLAMA_GENERATION_MODEL")
elif LLM_PROVIDER == "openai":
    OPENAI_API_KEY = required_env("OPENAI_API_KEY")
    OPENAI_GENERATION_MODEL = required_env("OPENAI_GENERATION_MODEL")
    # Grok (xAI) und DeepSeek bieten beide OpenAI-kompatible APIs an -
    # gleiches Request-/Response-Format wie OpenAI selbst. Deshalb reicht
    # eine konfigurierbare Basis-URL statt eines eigenen Code-Pfads pro
    # Anbieter. Default bleibt die echte OpenAI-API.
    # "or" statt Default-Parameter: docker-compose setzt bei fehlendem
    # .env-Eintrag einen LEEREN String, nicht "gar nicht gesetzt" -
    # os.getenv(..., default) wuerde den leeren String zurueckgeben, nicht
    # den Default (derselbe Bug-Typ, der uns schon beim Postgres-
    # Healthcheck begegnet ist).
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
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=credentials,
                    # Waehrend eines LLM-Aufrufs (bis zu 180s bei Ollama)
                    # blockiert handle_message vollstaendig - pika kann in
                    # dieser Zeit keine Heartbeats senden. Mit dem
                    # RabbitMQ-Default (60s) killt der Broker die
                    # Verbindung dann selbst, mitten in der Verarbeitung
                    # (Bug gefunden in context-severity-detector-service,
                    # betrifft aber jeden Service mit langen LLM-Aufrufen).
                    heartbeat=600,
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
    text = payload.get("text")
    metric = payload.get("metric")
    anomaly_type = payload.get("anomaly_type", "change")
    previous_value = payload.get("previous_value")
    current_value = payload.get("current_value")
    reason = payload.get("reason", "")

    if text:
        # Severity-Anomalie (context-severity-detector-service): der
        # rohe Beschwerdetext wird DIREKT als Suchtext genutzt, ohne
        # Meta-Umhuellung ("A message was flagged as..."). Der Rahmensatz
        # verwaesserte die inhaltliche Aehnlichkeit gegenueber den
        # gespeicherten Kontext-Dokumenten (die selbst kein solches
        # Meta-Framing tragen) - gefunden, als eine plausible Ursache
        # (Wartungsankuendigung) niedriger bewertet wurde als mehrere
        # klar irrelevante Nachrichten, alle in einem einzigen, kaum
        # unterscheidbaren Score-Band.
        sentence = text
    elif metric and previous_value is not None and current_value is not None:
        sentence = (
            f"The metric {metric} showed a significant {anomaly_type}, "
            f"changing from {previous_value} to {current_value}."
        )
    elif metric:
        sentence = f"An anomaly was detected for the metric {metric}."
    else:
        sentence = "An anomaly was detected."

    # reason nur bei Metrik-Anomalien anhaengen - bei Severity-Anomalien
    # ist es reines englisches Boilerplate ("single message was rated
    # highly urgent..."), das den moeglichst reinen Beschwerdetext
    # wieder verwaessern wuerde, genau das Problem, das oben behoben
    # wurde.
    if reason and not text:
        sentence += f" {reason.capitalize()}."

    return sentence


def create_embedding(text: str) -> list[float]:
    """
    Laeuft lokal im Prozess (ONNX via FastEmbed), keine Netzwerkanfrage.
    Muss demselben Modell entsprechen, mit dem vectorizing-service die
    gespeicherten Dokumente vektorisiert hat, sonst sind die Vektoren
    nicht im selben Raum vergleichbar.
    """
    embeddings = list(_embedding_model.embed([text]))
    return embeddings[0].tolist()


def build_explanation_prompt(anomaly_summary: str, cause_text: str) -> str:
    return (
        f"Beobachtete Anomalie: {anomaly_summary}\n"
        f"Moeglicher Ausloeser: \"{cause_text}\"\n\n"
        "Erklaere in GENAU EINEM vollstaendigen Satz auf Deutsch die "
        "Wirkungskette: WARUM koennte dieser Ausloeser konkret zu GENAU "
        "DIESER Anomalie gefuehrt haben? Nenne einen plausiblen "
        "Zwischenschritt (z. B. Auswirkung auf Konzentration, Zeit, "
        "Ressourcen). Metriken wie 'rate' oder 'average' sind statistische "
        "Zusammenfassungen vieler Einzelereignisse, keine direkt "
        "steuerbaren Werte - formuliere entsprechend praezise (z. B. "
        "'wodurch sich X erhoehte/verringerte', nicht 'die Nutzer haben "
        "die Rate gesenkt'). Nutze \"koennte\" oder \"moeglicherweise\". Nur "
        "der eine Satz, keine Wiederholung der Eingabe."
    )


def _generate_via_ollama(prompt: str) -> str:
    response = requests.post(
        f"http://{OLLAMA_HOST}:{OLLAMA_PORT}/api/generate",
        json={
            "model": OLLAMA_GENERATION_MODEL,
            "prompt": prompt,
            "stream": False,
            # Hybrid-Reasoning-Modelle (z. B. qwen3) denken sonst intern
            # nach, bevor sichtbarer Text entsteht - dasselbe Problem, das
            # bei gemini-2.5-flash zu abgeschnittenen Antworten fuehrte
            # (Denk-Tokens zaehlten gegen das Antwort-Budget). Modelle ohne
            # Reasoning-Unterstuetzung ignorieren dieses Feld einfach.
            "think": False,
            "options": {
                # Niedrige temperature gegen thematisches Abschweifen,
                # num_predict begrenzt die Antwortlaenge hart, statt nur
                # per Prompt-Anweisung ("ein Satz") zu hoffen, dass das
                # Modell sich daran haelt - kleine Modelle ignorieren
                # Laengenvorgaben im Prompt sonst zuverlaessig.
                "temperature": 0.3,
                "num_predict": 150,
            },
        },
        timeout=180,  # lokale CPU-Inferenz ist deutlich langsamer als Cloud
    )
    response.raise_for_status()
    return response.json()["response"].strip()


def _generate_via_openai(prompt: str) -> str:
    response = requests.post(
        f"{OPENAI_BASE_URL}/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
        json={
            "model": OPENAI_GENERATION_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3,
            "max_tokens": 150,
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"].strip()


def _generate_via_gemini(prompt: str) -> str:
    response = requests.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{GEMINI_GENERATION_MODEL}:generateContent",
        params={"key": GEMINI_API_KEY},
        json={
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.3,
                "maxOutputTokens": 400,
                # gemini-2.5-* nutzt standardmaessig interne "Thinking"-
                # Tokens, die GEGEN maxOutputTokens zaehlen - bei einem
                # knappen Budget kann das Nachdenken den gesamten Rahmen
                # aufbrauchen, bevor sichtbarer Text entsteht (beobachtet:
                # Antwort brach nach zwei Wörtern ab). Fuer diese simple
                # Ein-Satz-Aufgabe ist tiefes Reasoning nicht noetig.
                "thinkingConfig": {"thinkingBudget": 0},
            },
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()["candidates"][0]["content"]["parts"][0]["text"].strip()


def _call_llm(prompt: str) -> str:
    """
    Gemeinsamer Provider-Dispatch fuer jeden LLM-Aufruf dieses Service
    (Erklaerungsgenerierung UND Rerank) - ein Ort fuer die Anbieter-Logik,
    statt sie doppelt zu pflegen.
    """
    if LLM_PROVIDER == "ollama":
        return _generate_via_ollama(prompt)
    elif LLM_PROVIDER == "openai":
        return _generate_via_openai(prompt)
    elif LLM_PROVIDER == "gemini":
        return _generate_via_gemini(prompt)


def generate_causal_explanation(anomaly_summary: str, cause_text: str) -> str | None:
    """
    Nutzt ein generatives Modell (nicht das Embedding-Modell), um in
    eigenen Worten zu erklaeren, WIE der gefundene Kontext zur Anomalie
    gefuehrt haben koennte - Ergaenzung zum rohen semantic_text, nicht
    Ersatz dafuer (Transparenz/Nachvollziehbarkeit bleibt erhalten).

    Anbieter ist ueber LLM_PROVIDER konfigurierbar: "ollama" fuer
    produktiven Einsatz mit sensiblen Daten (bleibt lokal), "openai"/
    "gemini" z. B. fuer schnellere/qualitativ bessere Ergebnisse waehrend
    der Entwicklung mit unkritischen Testdaten.

    Bewusst vorsichtig formuliert im Prompt: keine Tatsachenbehauptung,
    da es sich weiterhin nur um eine semantische Korrelation handelt,
    keine bewiesene Kausalitaet (siehe confidence_note).

    Gibt None zurueck statt zu werfen, wenn die Generierung fehlschlaegt -
    die Investigation soll trotzdem mit dem rohen semantic_text nutzbar
    bleiben, auch ohne generierte Erklaerung.
    """
    prompt = build_explanation_prompt(anomaly_summary, cause_text)

    try:
        return _call_llm(prompt)
    except (requests.RequestException, KeyError, IndexError, ValueError) as error:
        print(f"[{SERVICE_NAME}] Explanation generation failed, continuing without it: {error}")
        return None


def build_rerank_prompt(anomaly_summary: str, candidates: list[dict]) -> str:
    candidate_lines = "\n".join(
        f"{i + 1}. \"{c['semantic_text']}\"" for i, c in enumerate(candidates)
    )
    return (
        f"Anomalie: {anomaly_summary}\n\n"
        "Bewerte fuer JEDEN der folgenden Kandidaten, wie plausibel er als "
        "URSACHE fuer die Anomalie ist - nicht wie thematisch AEHNLICH er "
        "klingt, sondern ob ein nachvollziehbarer WIRKUNGSMECHANISMUS "
        "denkbar ist (z. B. ueber Zeit, Ressourcen, Systeme, "
        "Abhaengigkeiten).\n\n"
        "Orientierung an konkreten Beispielen:\n"
        "0.9-1.0: Eindeutiger, gut nachvollziehbarer Wirkungsmechanismus "
        "(z. B. eine Systemkomponente faellt aus, wodurch eine andere "
        "Funktion direkt betroffen ist).\n"
        "0.5-0.7: Moeglich, aber spekulativ - ein indirekter Zusammenhang "
        "ist denkbar, aber nicht zwingend.\n"
        "0.1-0.3: Kein erkennbarer inhaltlicher Zusammenhang, nur "
        "oberflaechliche sprachliche oder strukturelle Aehnlichkeit.\n"
        "0.0: Voellig unabhaengige Themen.\n\n"
        f"{candidate_lines}\n\n"
        "Antworte AUSSCHLIESSLICH im Format 'NUMMER: ZAHL', eine Zeile pro "
        "Kandidat, keine Erklaerung, kein zusaetzlicher Text. Beispiel:\n"
        "1: 0.2\n2: 0.9"
    )


def _parse_rerank_scores(raw_text: str, count: int) -> list[float]:
    """
    Robust gegen Modell-Abweichungen vom exakten Format (analog zu
    _parse_score in context-severity-detector-service) - Zeilen, die
    nicht zugeordnet werden koennen, werden ignoriert statt die ganze
    Auswertung scheitern zu lassen. Nicht erwaehnte Kandidaten bleiben
    bei 0.0 (konservativ: eher zu niedrig als erfunden hoch bewertet).
    """
    scores = [0.0] * count
    for line in raw_text.strip().splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        index_part, score_part = line.split(":", 1)
        try:
            index = int(index_part.strip()) - 1
            score = float(score_part.strip().split()[0].rstrip(".,;:"))
        except (ValueError, IndexError):
            continue
        if 0 <= index < count:
            scores[index] = max(0.0, min(1.0, score))
    return scores


def rerank_candidates_by_causal_plausibility(anomaly_summary: str, candidates: list[dict]) -> list[dict]:
    """
    Reine Vektor-Aehnlichkeit (Embeddings) misst thematische/lexikalische
    Naehe, nicht KAUSALE Naehe - "Authentifizierungsserver-Wartung" und
    "Login nicht moeglich" teilen kaum Wortschatz, obwohl der Zusammenhang
    fuer einen Menschen offensichtlich ist, waehrend zwei oberflaechlich
    aehnlich klingende, aber inhaltlich unabhaengige Kurzbeschwerden
    embeddingsseitig naeher beieinander liegen koennen. Gefunden im
    Severity-Demo: eine echte Ursache (Wartungsankuendigung) wurde
    durchgaengig niedriger bewertet als mehrere klar irrelevante
    Nachrichten - auch nach dem Wechsel zu einem staerkeren, explizit
    deutschsprachigen Embedding-Modell (jina-embeddings-v2-base-de).
    Kein Modell-Qualitaetsproblem, sondern eine grundsaetzliche Grenze
    von reiner Vektor-Aehnlichkeit fuer kausale (statt thematische)
    Fragen.

    Dieser Schritt legt dem LLM alle zeitlich plausiblen Kandidaten vor
    und laesst es KAUSALE Plausibilitaet bewerten. Der LLM-Score ERSETZT
    den rohen Embedding-Score als massgebliches Konfidenzmass fuer
    Threshold und Sortierung.

    Bei Fehlschlag: faellt auf die urspruengliche, embedding-basierte
    Reihenfolge zurueck, statt die Investigation komplett scheitern zu
    lassen - Kausal-Bewertung ist eine Verbesserung, ihr Fehlen darf
    nicht die gesamte Funktion blockieren.
    """
    if not candidates:
        return candidates

    prompt = build_rerank_prompt(anomaly_summary, candidates)

    try:
        raw_text = _call_llm(prompt)
    except (requests.RequestException, KeyError, IndexError, ValueError) as error:
        print(f"[{SERVICE_NAME}] Reranking failed, falling back to embedding-based order: {error}")
        return candidates

    scores = _parse_rerank_scores(raw_text, len(candidates))

    reranked = []
    for candidate, score in zip(candidates, scores):
        updated = dict(candidate)
        updated["confidence"] = score
        reranked.append(updated)

    reranked.sort(key=lambda c: c["confidence"], reverse=True)
    return reranked


def search_related_context(
    qdrant_client: QdrantClient,
    vector: list[float],
    project_id: str | None,
    anomaly_occurred_at: str,
    anomaly_composite_key: str | None,
    anomaly_source_event_id: str | None,
) -> list[dict]:
    """
    Sucht die naechsten Nachbarn in Qdrant, gefiltert nach project_id
    (Isolation zwischen Einsatzszenarien), zeitlich VOR der Anomalie
    (Kausalitaets-Plausibilitaet: eine Ursache kann nicht nach ihrer
    Wirkung liegen), und schliesst zwei Arten von Selbstbezug aus:

    1. Kandidaten mit demselben composite_key wie die Anomalie selbst
       (eine Beobachtungsreihe kann sich nicht selbst erklaeren -
       "math_test_average war neulich auch mal 79" ist keine Ursache
       fuer "math_test_average ist jetzt 45", das ist nur ein weiterer
       Messpunkt derselben Reihe).
    2. Den urspruenglichen Event, der die Anomalie ausgeloest hat
       (anomaly_source_event_id) - relevant vor allem bei
       Severity-Anomalien (context-severity-detector-service): die
       gemeldete Beschwerde wird sowohl als ContextIngested als auch als
       Teil der resultierenden AnomalyDetected-Payload vektorisiert -
       ohne diesen Ausschluss koennte die Original-Nachricht als
       "Ursache" ihrer eigenen Anomalie-Meldung erscheinen.

    Eine ANDERE Metrik-Reihe oder ein unabhaengiges Kontext-Event bleibt
    weiterhin ein legitimer Kandidat.

    Zeit-, composite_key- und Selbstbezugs-Filter passieren client-seitig
    in Python, nicht als Qdrant-Filter - einfacher zu lesen und zu testen
    als Qdrant-Filter auf nicht dafuer indizierten Feldern.
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
        candidate_event_id = point.payload.get("event_id")

        occurred_before_anomaly = (
            candidate_occurred_at is not None and candidate_occurred_at < anomaly_occurred_at
        )
        is_same_series_as_anomaly = (
            anomaly_composite_key is not None
            and candidate_composite_key == anomaly_composite_key
        )
        is_the_triggering_event_itself = (
            anomaly_source_event_id is not None
            and candidate_event_id == anomaly_source_event_id
        )

        if is_same_series_as_anomaly or is_the_triggering_event_itself:
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
    # RERANK_POOL_SIZE statt SEARCH_LIMIT: der finale Cut auf SEARCH_LIMIT
    # passiert erst NACH dem Rerank-Schritt, nicht schon hier auf Basis
    # des rohen Embedding-Scores.
    return plausible[:RERANK_POOL_SIZE]


def build_anomaly_summary(payload: dict) -> str:
    """
    Wie build_anomaly_search_text: unterscheidet zwischen Metrik- und
    Text-Anomalien, statt blind von "metric"/"previous_value"/
    "current_value" auszugehen. Ohne diese Unterscheidung erzeugte eine
    Severity-Anomalie (kein "metric"-Feld) die irrefuehrende
    Zusammenfassung "None changed from None to None (severity)".
    """
    text = payload.get("text")
    metric = payload.get("metric")

    if text:
        return f"High-severity message flagged (score: {payload.get('severity_score')}): \"{text}\""
    elif metric:
        return (
            f"{metric} changed from {payload.get('previous_value')} "
            f"to {payload.get('current_value')} ({payload.get('anomaly_type')})"
        )
    return "Anomaly detected."


def build_investigation_payload(anomaly_envelope: dict, candidates: list[dict]) -> dict:
    payload = anomaly_envelope["payload"]
    anomaly_summary = build_anomaly_summary(payload)

    confident_candidates = [c for c in candidates if c["confidence"] >= CONFIDENCE_THRESHOLD][:SEARCH_LIMIT]

    if confident_candidates:
        # Nur fuer bereits bestaetigte (ueber dem Threshold liegende)
        # Kandidaten eine Erklaerung generieren - spart Rechenzeit und
        # vermeidet, dass das Modell plausibel klingende Geschichten zu
        # eigentlich verworfenen, schwachen Treffern erfindet.
        for candidate in confident_candidates:
            candidate["explanation"] = generate_causal_explanation(
                anomaly_summary, candidate["semantic_text"]
            )

        return {
            "anomaly_summary": anomaly_summary,
            "possible_causes": confident_candidates,
            "weak_leads": [],
            "confidence_note": (
                "Results reflect an LLM's judgment of causal plausibility, "
                "not proven causality."
            ),
        }

    # Keine sichere Ursache - aber vielleicht schwache, unsichere Signale,
    # die trotzdem eine manuelle Pruefung wert sind. Ein niedriger Score
    # verschwindet nicht komplett im Nichts, wird aber klar von einer
    # sicheren Ursache unterschieden (eigenes Feld, eigene
    # confidence_note) - SEZRA soll auch nicht-offensichtliche, unsichere
    # Richtungen aufzeigen, nicht nur bewiesene Ursachen praesentieren
    # oder schweigen.
    weak_candidates = [
        c for c in candidates if NOISE_FLOOR <= c["confidence"] < CONFIDENCE_THRESHOLD
    ][:WEAK_LEADS_LIMIT]

    if weak_candidates:
        for candidate in weak_candidates:
            candidate["explanation"] = generate_causal_explanation(
                anomaly_summary, candidate["semantic_text"]
            )

        return {
            "anomaly_summary": anomaly_summary,
            "possible_causes": [],
            "weak_leads": weak_candidates,
            "confidence_note": (
                f"No cause reached the confidence threshold ({CONFIDENCE_THRESHOLD}), "
                "but weak leads below that threshold are listed separately - "
                "worth a manual look, not a confirmed cause."
            ),
        }

    return {
        "anomaly_summary": anomaly_summary,
        "possible_causes": [],
        "weak_leads": [],
        "confidence_note": (
            "No context above the noise floor "
            f"({NOISE_FLOOR}) was found. This does not mean there is no "
            "cause - only that no sufficiently similar context exists in "
            "the available data."
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
    anomaly_source_event_id = payload.get("source_event_id")
    search_text = build_anomaly_search_text(payload)

    try:
        vector = create_embedding(search_text)
    except Exception as error:
        # Kein requests.RequestException mehr moeglich, da FastEmbed
        # lokal im Prozess laeuft, kein Netzwerkaufruf.
        print(f"[{SERVICE_NAME}] Embedding error, will retry: {error}")
        return

    try:
        candidates = search_related_context(
            qdrant_client, vector, envelope.get("project_id"), anomaly_occurred_at,
            anomaly_composite_key, anomaly_source_event_id,
        )
    except Exception as error:
        print(f"[{SERVICE_NAME}] Qdrant error, will retry: {error}")
        return

    # Kausal-Rerank VOR dem Aufbau der Investigation - der rohe Embedding-
    # Score misst nur thematische Naehe, nicht kausale Plausibilitaet
    # (siehe rerank_candidates_by_causal_plausibility fuer die
    # ausfuehrliche Begruendung).
    anomaly_summary_for_rerank = build_anomaly_summary(payload)
    candidates = rerank_candidates_by_causal_plausibility(anomaly_summary_for_rerank, candidates)

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