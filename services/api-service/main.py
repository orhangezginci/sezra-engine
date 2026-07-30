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
from fastapi import Body, FastAPI, HTTPException
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

# Fallback-Workspace, falls eine Anfrage kein project_id-Feld mitschickt.
# WICHTIG: muss selbst per POST /projects angelegt worden sein, sonst
# lehnt ingest() jede Anfrage ab (siehe project_exists) - strikte
# Pruefung, kein stillschweigendes Anlegen unbekannter Workspaces.
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

    project_id ist optional pro Anfrage ueberschreibbar (z. B. fuer
    mehrere Analyseprojekte auf derselben Instanz, siehe Studio-
    Anforderung) - faellt auf SEZRA_PROJECT_ID zurueck, wenn nicht
    angegeben, voll rueckwaertskompatibel zu bestehenden Aufrufen ohne
    dieses Feld. Wird VOR dem Payload-Merge herausgeloest, sonst wuerde
    die Projekt-ID versehentlich mit in den semantischen Text
    eingebettet (derselbe Fehlertyp wie frueher bei source_event_type).
    """
    raw_data = dict(raw_data)  # Kopie - das Original der Anfrage nicht mutieren
    project_id = raw_data.pop("project_id", None) or SEZRA_PROJECT_ID

    payload = {**raw_data, "source_type": source_type}
    event_type = SOURCE_TYPE_TO_EVENT_TYPE[source_type]
    event_id = str(uuid4())

    return {
        "schema_version": "1.1",
        "event_id": event_id,
        "event_type": event_type,
        "source": SERVICE_NAME,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "project_id": project_id,
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


def project_exists(project_id: str) -> bool:
    """
    Strikte Pruefung, bewusst kein nachsichtiges Automatisch-Anlegen
    unbekannter Workspaces - passt zum hyper-entkoppelten Grundprinzip
    der Architektur: eine project_id muss explizit ueber POST /projects
    entstanden sein, sonst existiert der Workspace fuer die Engine
    schlicht nicht, egal wie plausibel der UUID-Wert aussieht.
    """
    connection = connect_to_postgres()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM projects WHERE id = %s", (project_id,))
            return cursor.fetchone() is not None
    finally:
        connection.close()


def ingest(raw_data: dict, source_type: str) -> dict:
    envelope = build_envelope(raw_data, source_type)

    if not project_exists(envelope["project_id"]):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unknown project_id: {envelope['project_id']} - "
                "create it first via POST /projects"
            ),
        )

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


def validate_batch_entry(entry) -> str | None:
    """Gibt eine Fehlermeldung zurueck, oder None wenn der Eintrag gueltig ist."""
    if not isinstance(entry, dict):
        return "kein gueltiges JSON-Objekt"
    if entry.get("source_type") not in SOURCE_TYPE_TO_EVENT_TYPE:
        return '"source_type" fehlt oder ist ungueltig (muss "observation" oder "context" sein)'
    return None


@app.post("/ingest/batch")
def post_batch(raw_data: list = Body(...)):
    """
    Nimmt ein JSON-Array mehrerer Eintraege entgegen (z. B. eine
    exportierte Server-Log-Datei mit vielen Zeilen) und verteilt jeden
    einzeln an die gleiche Logik wie POST /observations bzw.
    POST /context - intern in der Engine, NICHT als mehrere HTTP-
    Anfragen von einem Client aus.

    Bewusst hier gebaut, nicht in Studio: ein Client soll niemals selbst
    entscheiden oder in einer Schleife aufrufen muessen, welcher Eintrag
    wohin gehoert - das waere Geschaeftslogik im Frontend, ueber curl
    direkt nicht reproduzierbar. json-adapter-service lehnt Arrays
    bewusst ab (eine Datei = ein Objekt, siehe dortiger Kommentar) -
    dieser Endpunkt ist die dedizierte, saubere Antwort auf "eine Datei
    = mehrere Eintraege", ohne json-adapter-service selbst aufzuweichen.

    Ein nicht-Array-Koerper wird bereits von FastAPI selbst mit 422
    abgelehnt (durch die list-Typannotation), bevor diese Funktion
    ueberhaupt laeuft - keine manuelle Pruefung noetig.

    Liefert pro Eintrag ein eigenes Ergebnis zurueck (Erfolg mit
    event_id, oder Fehlschlag mit Grund) - ein einzelner ungueltiger
    Eintrag blockiert nicht die anderen, aehnlich wie einzelne
    fehlerhafte Nachrichten in der eigentlichen Pipeline per Dead-
    Letter-Queue isoliert werden, statt die gesamte Verarbeitung zu
    stoppen.
    """
    results = []
    for index, entry in enumerate(raw_data):
        error = validate_batch_entry(entry)
        if error:
            results.append({"index": index, "ok": False, "error": error})
            continue

        source_type = entry["source_type"]
        try:
            outcome = ingest(entry, source_type)
            results.append({
                "index": index,
                "ok": True,
                "event_id": outcome["event_id"],
                "event_type": outcome["event_type"],
            })
        except HTTPException as error:
            results.append({"index": index, "ok": False, "error": error.detail})

    return {"results": results}


@app.get("/investigations")
def get_investigations(limit: int = 20, project_id: str | None = None):
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

    project_id filtert optional auf ein einzelnes Analyseprojekt -
    faellt der Filter weg, werden Investigations projektuebergreifend
    zurueckgegeben (bisheriges Verhalten, unveraendert). Filterung
    passiert bereits in der SQL-Abfrage, nicht erst danach in Python -
    dadurch bleibt auch der Kreuzverweis-Mechanismus (Stufe 3) korrekt
    auf das gefilterte Projekt beschraenkt, statt versehentlich
    Ursachen aus einem ANDEREN Projekt als "erklaert durch" anzuzeigen.

    Holt grosszuegig mehr Zeilen als angefordert (Kreuzverweis-Analyse
    braucht den vollen Kontext, nicht nur die ersten `limit` Zeilen -
    sonst koennte die erklaerende Investigation ausserhalb der
    betrachteten Seite liegen und uebersehen werden), sortiert dann in
    Python nach den drei Stufen und schneidet erst danach auf `limit` zu.
    """
    connection = connect_to_postgres()
    try:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            if project_id:
                cursor.execute(
                    """
                    SELECT event_id, correlation_id, causation_id, occurred_at,
                           received_at, project_id, payload
                    FROM events
                    WHERE event_type = 'InvestigationGenerated' AND project_id = %s
                    ORDER BY received_at DESC
                    LIMIT 500
                    """,
                    (project_id,),
                )
            else:
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
def get_investigation(event_id: str, project_id: str | None = None):
    connection = connect_to_postgres()
    try:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            if project_id:
                cursor.execute(
                    """
                    SELECT event_id, correlation_id, occurred_at, received_at,
                           project_id, payload
                    FROM events
                    WHERE event_type = 'InvestigationGenerated' AND event_id = %s
                          AND project_id = %s
                    """,
                    (event_id, project_id),
                )
            else:
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
    project_id: str | None = None,
    limit: int = 50,
):
    """
    correlation_id erlaubt, die komplette Kette eines Vorfalls abzurufen -
    z. B. die urspruengliche Beobachtung, die erkannte Anomalie und die
    daraus entstandene Investigation gehoeren alle zur selben
    correlation_id (additiv durch die Pipeline durchgereicht, siehe
    contracts/README.md). Kombinierbar mit event_type, um z. B. gezielt
    nur die Anomalie einer bestimmten Kette zu holen. project_id filtert
    optional auf ein einzelnes Analyseprojekt, analog zu /investigations.
    """
    conditions = []
    params: list = []

    if event_type:
        conditions.append("event_type = %s")
        params.append(event_type)
    if correlation_id:
        conditions.append("correlation_id = %s")
        params.append(correlation_id)
    if project_id:
        conditions.append("project_id = %s")
        params.append(project_id)

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


@app.post("/projects")
def post_project(raw_data: dict):
    """
    Legt ein neues Analyseprojekt an - der Nutzer gibt nur einen
    lesbaren Namen an, die Engine generiert die UUID. sezra-engine
    selbst (Pipeline, Detektoren) kommt weiterhin ausschliesslich mit
    UUIDs aus und braucht nie einen lesbaren Namen - das ist bewusst
    ein reines api-service/Studio-Anliegen, keine Pipeline-Aenderung.
    """
    name = raw_data.get("name", "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Missing required field: name")

    project_id = str(uuid4())

    connection = connect_to_postgres()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO projects (id, name) VALUES (%s, %s)",
                (project_id, name),
            )
        connection.commit()
    finally:
        connection.close()

    return {"id": project_id, "name": name}


@app.get("/projects")
def get_projects():
    """
    Liest aus der projects-Tabelle (echte Quelle der Wahrheit fuer
    "welche Projekte gibt es, wie heissen sie"), NICHT mehr abgeleitet
    aus vorhandenen events - ein frisch angelegtes, noch leeres Projekt
    (per POST /projects) waere dort sonst schlicht nicht aufgetaucht,
    bevor die erste Beobachtung/Kontext-Nachricht dafuer eingereicht
    wurde.
    """
    connection = connect_to_postgres()
    try:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute("SELECT id, name, created_at FROM projects ORDER BY created_at DESC")
            rows = cursor.fetchall()
    finally:
        connection.close()

    return JSONResponse(content=json.loads(json.dumps(rows, default=str)))


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME}