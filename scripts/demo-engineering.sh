#!/bin/bash
set -e

# Demo-Szenario: eine einzelne, sicherheitskritische Strukturschaden-
# Meldung loest sofort eine Untersuchung aus (context-severity-detector-
# service) - physische statt digitale Dringlichkeit, im Gegensatz zu den
# bisherigen IT-/Business-nahen Szenarien.
#
# Echte Ursache ist NICHT offensichtlich: vibrationsinduzierte
# Materialermuedung durch Bauarbeiten im angrenzenden Sektor - ein
# real dokumentiertes Phaenomen im Bauingenieurwesen (wiederholte
# dynamische Belastung kann Ermuedungsrisse in Stahltraegern
# beguenstigen), aber kein Zusammenhang, den ein Mensch sofort
# herstellt. Testet dieselbe "nicht-offensichtlich"-Staerke wie das
# Sonnensturm-Astrophysik-Szenario, hier in einer komplett anderen
# Domaene mit physischen statt digitalen Einsaetzen.
#
# Reicht mehrere Kontext-Nachrichten unterschiedlicher Relevanz ein,
# BEVOR die kritische Meldung kommt - realistischer Testkorpus statt
# nur einer einzelnen, zufaellig passenden Alternative (siehe
# demo-severity.sh fuer dieselbe Lehre im IT-Support-Kontext).
#
# Legt einen frischen, eindeutig benannten Workspace an (POST /projects,
# strikte Pruefung in api-service - siehe demo-school.sh fuer die
# ausfuehrliche Begruendung).
#
# Voraussetzung: im Repo-Root ausfuehren, curl muss lokal verfuegbar sein.

API_URL="http://localhost:8000"
RABBITMQ_MGMT_URL="http://localhost:15672"
POLL_INTERVAL_SECONDS=3
POLL_TIMEOUT_SECONDS=210
STACK_READY_TIMEOUT_SECONDS=180
CONSUMER_READY_TIMEOUT_SECONDS=60

CONSUMER_QUEUES="sezra.queue.ingestion-service sezra.queue.knowledge-service sezra.queue.vectorizing-service sezra.queue.deviation-detector-service sezra.queue.persistence-service sezra.queue.analyzer-service sezra.queue.context-severity-detector-service"

STACK_SERVICES="rabbitmq postgres qdrant persistence-migrations api-service ingestion-service knowledge-service persistence-service vectorizing-service deviation-detector-service analyzer-service context-severity-detector-service"

if [ ! -f "docker-compose.yml" ]; then
  echo "Fehler: docker-compose.yml nicht gefunden. Im Repo-Root ausfuehren."
  exit 1
fi

if ! command -v curl > /dev/null; then
  echo "Fehler: curl wird benoetigt, ist aber nicht installiert."
  exit 1
fi

if [ -f ".env" ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

if [ -z "$RABBITMQ_USER" ] || [ -z "$RABBITMQ_PASSWORD" ]; then
  echo "Fehler: RABBITMQ_USER/RABBITMQ_PASSWORD nicht gesetzt (.env fehlt oder unvollstaendig)."
  exit 1
fi

echo "=== SEZRA Demo: Ingenieurwesen - Strukturschaden (Text <- Text) ==="
echo ""

echo "0/4 Stack wird neu gestartet (down + up --build)..."
docker compose down > /dev/null 2>&1 || true
docker compose up --build -d $STACK_SERVICES

echo "Warte, bis alle Services bereit sind (bis zu ${STACK_READY_TIMEOUT_SECONDS}s)..."
elapsed=0
not_running=999
while [ "$elapsed" -lt "$STACK_READY_TIMEOUT_SECONDS" ]; do
  not_running=$(docker compose ps $STACK_SERVICES --format json 2>/dev/null \
    | python3 -c "
import sys, json

raw = sys.stdin.read().strip()
entries = []
if raw:
    try:
        parsed = json.loads(raw)
        entries = parsed if isinstance(parsed, list) else [parsed]
    except json.JSONDecodeError:
        for line in raw.splitlines():
            line = line.strip()
            if line:
                entries.append(json.loads(line))

count = 0
for entry in entries:
    state = entry.get('State', '')
    health = entry.get('Health', '')
    if entry.get('Service') == 'persistence-migrations':
        if state == 'exited' and entry.get('ExitCode') == 0:
            continue
        count += 1
        continue
    if state != 'running':
        count += 1
        continue
    if health and health != 'healthy':
        count += 1
print(count)
" 2>/dev/null || echo "999")

  if [ "$not_running" = "0" ]; then
    break
  fi

  sleep 3
  elapsed=$((elapsed + 3))
done

if [ "$not_running" != "0" ]; then
  echo "Warnung: nicht alle Services meldeten sich als bereit innerhalb von ${STACK_READY_TIMEOUT_SECONDS}s."
  echo "Fahre trotzdem fort - falls das Ergebnis fehlt, pruefe: docker compose ps"
fi

echo "Warte, bis alle Consumer tatsaechlich an ihren Queues haengen (bis zu ${CONSUMER_READY_TIMEOUT_SECONDS}s)..."
elapsed=0
all_consuming=""
while [ "$elapsed" -lt "$CONSUMER_READY_TIMEOUT_SECONDS" ]; do
  all_consuming="yes"

  for queue in $CONSUMER_QUEUES; do
    consumers=$(curl -s -u "${RABBITMQ_USER}:${RABBITMQ_PASSWORD}" \
      "${RABBITMQ_MGMT_URL}/api/queues/%2F/${queue}" 2>/dev/null \
      | python3 -c "import sys,json; print(json.load(sys.stdin).get('consumers', 0))" 2>/dev/null || echo "0")

    if [ "$consumers" -lt 1 ] 2>/dev/null; then
      all_consuming=""
      break
    fi
  done

  if [ -n "$all_consuming" ]; then
    break
  fi

  sleep 2
  elapsed=$((elapsed + 2))
done

if [ -z "$all_consuming" ]; then
  echo "Warnung: nicht alle Queues hatten einen aktiven Consumer innerhalb von ${CONSUMER_READY_TIMEOUT_SECONDS}s."
  echo "Fahre trotzdem fort - Nachrichten koennten verloren gehen. Pruefe: ${RABBITMQ_MGMT_URL}"
fi

echo "Warte auf api-service..."
for i in $(seq 1 20); do
  if curl -s -o /dev/null -w "" "$API_URL/health" 2>/dev/null; then
    break
  fi
  sleep 1
done

echo "Stack ist bereit."
echo ""

echo "Neuen Workspace fuer diesen Demo-Lauf anlegen..."
PROJECT_NAME="Demo: Engineering - $(date '+%Y-%m-%d %H:%M:%S')"
PROJECT_RESPONSE=$(curl -s -X POST "$API_URL/projects" \
  -H "Content-Type: application/json" \
  -d "{\"name\": \"$PROJECT_NAME\"}")
PROJECT_ID=$(echo "$PROJECT_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])" 2>/dev/null || true)

if [ -z "$PROJECT_ID" ]; then
  echo "Fehler: Workspace konnte nicht angelegt werden."
  echo "Antwort: $PROJECT_RESPONSE"
  exit 1
fi

echo "Workspace angelegt: $PROJECT_NAME"
echo "  ID: $PROJECT_ID"
echo ""

echo "Leere vorherige Demo-Daten (Postgres-Tabelle, Qdrant-Punkte)..."
docker compose exec -T postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" \
  -c "TRUNCATE TABLE events;" > /dev/null 2>&1 || true
curl -s -X POST "http://localhost:6333/collections/sezra_semantic/points/delete" \
  -H "Content-Type: application/json" \
  -d '{"filter": {}}' > /dev/null 2>&1 || true

echo ""

post_context() {
  curl -s -X POST "$API_URL/context" \
    -H "Content-Type: application/json" \
    -d "$1" > /dev/null
}

echo "1/6 Irrelevante Nachricht (defekte Beleuchtung) wird eingereicht..."
post_context "{\"project_id\": \"$PROJECT_ID\", \"sender\": \"facility-mgmt@site.internal\", \"subject\": \"Wartungsanfrage\", \"text\": \"Beleuchtung im Lager Sektor C ist seit gestern defekt, bitte Ersatz einplanen.\"}"
sleep 5

echo "2/6 Irrelevante Nachricht (Ersatzteil-Bestellung) wird eingereicht..."
post_context "{\"project_id\": \"$PROJECT_ID\", \"sender\": \"procurement@site.internal\", \"subject\": \"Bestellanfrage\", \"text\": \"Bitte Ersatzteile fuer Kran Nr. 4 nachbestellen, Lagerbestand niedrig.\"}"
sleep 5

echo "3/6 Geringfuegige Meldung (Kratzer am Gelaender) wird eingereicht..."
post_context "{\"project_id\": \"$PROJECT_ID\", \"sender\": \"site-inspection@site.internal\", \"subject\": \"Routineinspektion\", \"text\": \"Kleiner Kratzer am Handlauf im Treppenhaus B festgestellt, kosmetisch, keine Handlungsdringlichkeit.\"}"
sleep 5

echo "4/6 Echte, plausible Ursache wird eingereicht (Vibrationsmessung)..."
post_context "{\"project_id\": \"$PROJECT_ID\", \"sender\": \"structural-monitoring@site.internal\", \"subject\": \"Vibrationsmessung ueberschritten\", \"text\": \"Erhoehte Vibrationswerte durch Bauarbeiten im angrenzenden Sektor A wurden ueber mehrere Tage in Sektor B gemessen, Grenzwert phasenweise ueberschritten.\"}"
sleep 5

echo "5/6 Weitere irrelevante Nachricht (Parkplatzanfrage) wird eingereicht..."
post_context "{\"project_id\": \"$PROJECT_ID\", \"sender\": \"hr@site.internal\", \"subject\": \"Parkplatzzuteilung\", \"text\": \"Neue Mitarbeiterin benoetigt einen Parkplatz auf dem Werksgelaende ab naechster Woche.\"}"
sleep 8

echo "6/6 Kritische Meldung wird eingereicht (sollte SOFORT eine Anomalie ausloesen)..."
post_context "{\"project_id\": \"$PROJECT_ID\", \"sender\": \"structural-monitoring@site.internal\", \"subject\": \"Strukturschaden festgestellt\", \"text\": \"Riss in tragendem Stahltraeger im Sektor B3 festgestellt, sofortige Ueberpruefung erforderlich.\"}"

echo ""
echo "Warte auf Investigation-Ergebnis (bis zu ${POLL_TIMEOUT_SECONDS}s)..."

elapsed=0
result=""
while [ "$elapsed" -lt "$POLL_TIMEOUT_SECONDS" ]; do
  result=$(curl -s "$API_URL/investigations?project_id=$PROJECT_ID&limit=1" 2>/dev/null || true)

  if [ -n "$result" ] && [ "$result" != "[]" ]; then
    break
  fi

  sleep "$POLL_INTERVAL_SECONDS"
  elapsed=$((elapsed + POLL_INTERVAL_SECONDS))
  result=""
done

echo ""
if [ -z "$result" ]; then
  echo "Kein Investigation-Ergebnis innerhalb von ${POLL_TIMEOUT_SECONDS}s gefunden."
  echo "Pruefe: docker compose logs context-severity-detector-service analyzer-service"
  exit 1
fi

echo "=== Investigation-Ergebnis ==="
echo "$result" | python3 -m json.tool

echo ""
echo "=== Severity-Bewertungen (zur Kontrolle) ==="
docker compose logs context-severity-detector-service 2>/dev/null | grep -i "severity\|detected" | tail -8