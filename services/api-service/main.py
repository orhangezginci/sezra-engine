"""
api-service

Middleware zwischen SEZRA-ENGINE und einem Frontend (z. B. SEZRA Studio
Light). Zwei Rollen in einem Service, bewusst kein reiner Pipeline-Baustein:

- Schreibend: POST /observations, POST /context nehmen rohe JSON-Daten per
  HTTP entgegen, envelope'n sie (wie json-adapter-service, aber
  HTTP-getriggert statt Datei-Polling-getriggert) und publizieren direkt
  zu sezra.stream.raw - kein Umweg ueber das Dateisystem/json-adapter-
  service, um Polling-Latenz zu vermeiden.
- Lesend: GET /investigations, GET /events lesen direkt aus der
  events-Tabelle in Postgres (kein RabbitMQ noetig, Daten liegen dort
  bereits persistiert).

Jede Anfrage oeffnet ihre eigene RabbitMQ-/Postgres-Verbindung (kein
geteilter Zustand ueber Threadpool-Worker hinweg) - fuer den aktuellen
Umfang (Proof of Concept) einfacher und sicherer als Connection-Pooling.
"""

import json
import os
from datetime import datetime, timezone
from uuid import uuid4

import pika
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from envelope_validation import InvalidEnvelopeError, validate_envelope

SERVICE_NAME = "api-service"

OUTPUT_EXCHANGE = "sezra.stream.raw"
DEAD_LETTER_EXCHANGE = "sezra.stream.dead_letter"
DEAD_LETTER_ROUTING_KEY = f"{SERVICE_NAME}.failed"

SOURCE_TYPE_TO_EVENT_TYPE = {
    "observation": "ObservationIngested",
    "context": "ContextIngested",
}


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

# Wie bei json-adapter-service: hart verdrahtet pro Adapter-Instanz, kein
# dynamisches Projekt-Konzept.
SEZRA_PROJECT_ID = required_env("SEZRA_PROJECT_ID")


def connect_to_rabbitmq() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(username=RABBITMQ_USER, password=RABBITMQ_PASSWORD)
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
    )


def connect_to_postgres():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )


def build_envelope(raw_data: dict, source_type: str) -> dict:
    """
    Wie json-adapter-service's build_envelope, aber source_type kommt
    vom Endpoint (Pfad /observations vs. /context), nicht aus einem
    Feld in raw_data - eindeutig statt interpretationsbeduerftig.
    """
    payload = {**raw_data, "source_type": source_type}
    event_type = SOURCE_TYPE_TO_EVENT_TYPE[source_type]
    event_id = str(uuid4())

    return {
        "schema_version": "1.1",
        "event_id": event_id,
        "event_type": event_type,
        "source": SERVICE_NAME,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "project_id": SEZRA_PROJECT_ID,
        # Selbstreferenziell: siehe json-adapter-service main.py fuer die
        # ausfuehrliche Begruendung - ohne das faengt keine Korrelations-
        # Kette bei ihrem eigentlichen Ursprung an.
        "correlation_id": event_id,
        "payload": payload,
    }


def publish_envelope(envelope: dict) -> None:
    connection = connect_to_rabbitmq()
    try:
        channel = connection.channel()
        channel.exchange_declare(exchange=OUTPUT_EXCHANGE, exchange_type="fanout", durable=True)
        channel.basic_publish(
            exchange=OUTPUT_EXCHANGE,
            routing_key="",
            body=json.dumps(envelope).encode("utf-8"),
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        )
    finally:
        connection.close()


def ingest(raw_data: dict, source_type: str) -> dict:
    envelope = build_envelope(raw_data, source_type)

    try:
        validate_envelope(envelope)
    except InvalidEnvelopeError as error:
        # Selbst gebautes Envelope ist ungueltig -> Bug in diesem Service,
        # kein Problem der Nutzereingabe. 500 statt 400.
        raise HTTPException(status_code=500, detail=f"Failed to build a valid envelope: {error}")

    publish_envelope(envelope)

    return {"event_id": envelope["event_id"], "event_type": envelope["event_type"]}


app = FastAPI(title="SEZRA-ENGINE API", version="1.0")

# CORS: erlaubt einem lokalen Experiment (z. B. eine index.html, die man
# direkt im Browser oeffnet oder ueber einen simplen Static-Server
# ausliefert) den Zugriff auf diese API. Bewusst offen (allow_origins=["*"])
# fuer dieses Entwicklungsstadium - keine Authentifizierung, kein
# Produktivbetrieb. Muesste vor einem echten Deployment eingeschraenkt
# werden.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.post("/observations")
def post_observation(raw_data: dict):
    return ingest(raw_data, "observation")


@app.post("/context")
def post_context(raw_data: dict):
    return ingest(raw_data, "context")


@app.get("/investigations")
def get_investigations(limit: int = 20):
    """
    Drei Prioritaetsstufen statt nur "Ursache gefunden vs. nicht":

    1. Eigene Ursache gefunden - hoechste Prioritaet.
    2. Echtes ungeloestes Raetsel - keine Ursache gefunden, UND diese
       Anomalie erklaert selbst auch nirgendwo sonst etwas.
    3. Anderswo bereits erklaert - keine eigene Ursache, ABER diese
       Anomalie wurde selbst als Ursache einer ANDEREN Investigation
       identifiziert - niedrigste Prioritaet, aber sichtbar (nicht
       versteckt), mit Verweis auf die erklaerende Investigation.

    Ohne Stufe 3 erschien z. B. im E-Commerce-Szenario ein
    checkout_error_rate-Spike als eigenstaendiges, ungeloestes Raetsel,
    obwohl er bereits korrekt als Ursache fuer den conversion_rate-Abfall
    gefunden wurde - fuer den Nutzer wirkte das wie ein Ratespiel
    zwischen zwei gleichrangigen Ergebnissen, obwohl SEZRA die
    Verbindung laengst kannte. Kreuzverweis passiert rein lesend, ohne
    dass deviation-detector-service oder analyzer-service voneinander
    wissen muessen - haelt die Services entkoppelt.

    Holt grosszuegig mehr Zeilen als angefordert (Kreuzverweis-Analyse
    braucht den vollen Kontext, nicht nur die ersten `limit` Zeilen -
    sonst koennte die erklaerende Investigation ausserhalb der
    betrachteten Seite liegen und uebersehen werden), sortiert dann in
    Python nach den drei Stufen und schneidet erst danach auf `limit` zu.
    """
    connection = connect_to_postgres()
    try:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT event_id, correlation_id, causation_id, occurred_at,
                       received_at, project_id, payload
                FROM events
                WHERE event_type = 'InvestigationGenerated'
                ORDER BY received_at DESC
                LIMIT 500
                """,
            )
            rows = cursor.fetchall()
    finally:
        connection.close()

    # Wer erklaert wen: source_event_id (aus jeder possible_cause) ->
    # event_id der Investigation, die diese Ursache gefunden hat.
    explained_by: dict[str, str] = {}
    for row in rows:
        for cause in row["payload"].get("possible_causes", []):
            source_event_id = cause.get("source_event_id")
            if source_event_id:
                explained_by[source_event_id] = row["event_id"]

    def tier(row: dict) -> int:
        has_own_causes = bool(row["payload"].get("possible_causes"))
        if has_own_causes:
            return 0
        causation_id = str(row["causation_id"]) if row["causation_id"] else None
        if causation_id and causation_id in explained_by:
            return 2
        return 1

    for row in rows:
        causation_id = str(row["causation_id"]) if row["causation_id"] else None
        explaining_investigation_id = explained_by.get(causation_id) if causation_id else None
        row["explained_elsewhere"] = explaining_investigation_id is not None
        row["explained_by_investigation_event_id"] = explaining_investigation_id

    # Pythons sort() ist stabil: zuerst nach Zeitpunkt sortieren (neueste
    # zuerst), danach nach Stufe (0 zuerst) - das Ergebnis ist "Stufe
    # aufsteigend, innerhalb einer Stufe neueste zuerst", ohne dass eine
    # einzelne sort()-Anweisung zwei unterschiedliche Richtungen
    # gleichzeitig braucht.
    rows.sort(key=lambda row: row["received_at"], reverse=True)
    rows.sort(key=tier)

    return JSONResponse(content=json.loads(json.dumps(rows[:limit], default=str)))


@app.get("/investigations/{event_id}")
def get_investigation(event_id: str):
    connection = connect_to_postgres()
    try:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT event_id, correlation_id, occurred_at, received_at,
                       project_id, payload
                FROM events
                WHERE event_type = 'InvestigationGenerated' AND event_id = %s
                """,
                (event_id,),
            )
            row = cursor.fetchone()
    finally:
        connection.close()

    if row is None:
        raise HTTPException(status_code=404, detail="Investigation not found")

    return JSONResponse(content=json.loads(json.dumps(row, default=str)))


@app.get("/events")
def get_events(
    event_type: str | None = None,
    correlation_id: str | None = None,
    limit: int = 50,
):
    """
    correlation_id erlaubt, die komplette Kette eines Vorfalls abzurufen -
    z. B. die urspruengliche Beobachtung, die erkannte Anomalie und die
    daraus entstandene Investigation gehoeren alle zur selben
    correlation_id (additiv durch die Pipeline durchgereicht, siehe
    contracts/README.md). Kombinierbar mit event_type, um z. B. gezielt
    nur die Anomalie einer bestimmten Kette zu holen.
    """
    conditions = []
    params: list = []

    if event_type:
        conditions.append("event_type = %s")
        params.append(event_type)
    if correlation_id:
        conditions.append("correlation_id = %s")
        params.append(correlation_id)

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.append(limit)

    connection = connect_to_postgres()
    try:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(
                f"""
                SELECT event_id, event_type, source, correlation_id, occurred_at,
                       received_at, project_id, payload
                FROM events
                {where_clause}
                ORDER BY received_at DESC
                LIMIT %s
                """,
                params,
            )
            rows = cursor.fetchall()
    finally:
        connection.close()

    return JSONResponse(content=json.loads(json.dumps(rows, default=str)))


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME}
