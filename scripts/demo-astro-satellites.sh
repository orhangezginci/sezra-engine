#!/bin/bash
set -e

# Demo-Szenario: LEO-Satelliten (z. B. Starlink) emittieren unbeabsichtigte
# Radiofrequenzen ("Unintended ElectroMagnetic Radiation"), die
# Radioteleskop-Beobachtungen nachweislich stoeren - ein reales, aktiv
# erforschtes Problem der Radioastronomie (LOFAR-, Curtin-University-
# Studien zeigen bis zu 30% gestoerte Beobachtungsbilder). Testet
# zuverlaessige, automatische Erkennung eines BEKANNTEN Mechanismus -
# wertvoll, weil ein Techniker um 3 Uhr nachts nicht zwingend sofort an
# Satelliten-Durchgaenge denkt, auch wenn Experten das Muster kennen.
#
# leo_satellite_pass_count waere in einem echten Einsatz aus oeffentlichen
# TLE-Bahndaten (CelesTrak/Space-Track) abgeleitet - hier fuer die Demo
# gemockt, da ein echter Astro-Daten-Adapter (noch) kein SEZRA-
# Standardbaustein ist (im Gegensatz zu einem geplanten Jira-Adapter).
#
# Metrik-zu-Metrik-Muster wie E-Commerce/Pharma-Logistik:
# leo_satellite_pass_count (Ursache) -> radio_frequency_interference_rate
# (Anomalie).
#
# Voraussetzung: im Repo-Root ausfuehren, curl muss lokal verfuegbar sein.

API_URL="http://localhost:8000"
RABBITMQ_MGMT_URL="http://localhost:15672"
POLL_INTERVAL_SECONDS=3
POLL_TIMEOUT_SECONDS=210
STACK_READY_TIMEOUT_SECONDS=180
CONSUMER_READY_TIMEOUT_SECONDS=60
# Metrik-zu-Metrik-Szenario: Ursache- UND Wirkung-Metrik loesen je eine
# eigene Anomalie/Investigation aus - erst wenn beide vorliegen, ist die
# Sortierung (Ursache-gefunden zuerst) zuverlaessig vollstaendig. Ohne
# das koennte ein Nutzer zufaellig nur die schneller fertige, aber
# weniger interessante Investigation sehen (die des Ursprungs-Ereignisses
# ohne eigene Ursache) und faelschlich denken, SEZRA haette nichts
# gefunden.
EXPECTED_INVESTIGATION_COUNT=2

CONSUMER_QUEUES="sezra.queue.ingestion-service sezra.queue.knowledge-service sezra.queue.vectorizing-service sezra.queue.deviation-detector-service sezra.queue.persistence-service sezra.queue.analyzer-service"

STACK_SERVICES="rabbitmq postgres qdrant persistence-migrations api-service ingestion-service knowledge-service persistence-service vectorizing-service deviation-detector-service analyzer-service"

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

echo "=== SEZRA Demo: Astrophysik - Satelliten-Interferenz (Metrik -> Metrik) ==="
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

echo "Leere vorherige Demo-Daten (Postgres-Tabelle, Qdrant-Punkte)..."
docker compose exec -T postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" \
  -c "TRUNCATE TABLE events;" > /dev/null 2>&1 || true
curl -s -X POST "http://localhost:6333/collections/sezra_semantic/points/delete" \
  -H "Content-Type: application/json" \
  -d '{"filter": {}}' > /dev/null 2>&1 || true

echo ""

post_observation() {
  curl -s -X POST "$API_URL/observations" \
    -H "Content-Type: application/json" \
    -d "$1" > /dev/null
}

echo "1/4 Baseline: leo_satellite_pass_count (stabil, ~12 pro Beobachtungsfenster)..."
for value in 11 13 12 12 14; do
  post_observation "{\"metric\": \"leo_satellite_pass_count\", \"value\": $value}"
  sleep 3
done

echo "2/4 Baseline: radio_frequency_interference_rate (stabil, ~2.5%)..."
for value in 2.3 2.6 2.4 2.5 2.7; do
  post_observation "{\"metric\": \"radio_frequency_interference_rate\", \"value\": $value}"
  sleep 3
done

echo "3/4 Satelliten-Durchgangsrate steigt deutlich (neue Konstellations-Charge, Ursache)..."
post_observation '{"metric": "leo_satellite_pass_count", "value": 68}'
sleep 8

echo "4/4 Stoerungsrate der Radiobeobachtungen steigt an (Anomalie)..."
post_observation '{"metric": "radio_frequency_interference_rate", "value": 24.8}'

echo ""
echo "Warte auf ${EXPECTED_INVESTIGATION_COUNT} Investigation-Ergebnisse (bis zu ${POLL_TIMEOUT_SECONDS}s)..."
echo "    (Metrik-zu-Metrik-Szenario: sowohl Ursache- als auch Wirkung-Metrik"
echo "    loesen jeweils eine eigene Anomalie aus - erst wenn BEIDE fertig"
echo "    analysiert sind, ist die Sortierung zuverlaessig vollstaendig)"

elapsed=0
count=0
while [ "$elapsed" -lt "$POLL_TIMEOUT_SECONDS" ]; do
  count=$(curl -s "$API_URL/investigations?limit=10" 2>/dev/null \
    | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")

  if [ "$count" -ge "$EXPECTED_INVESTIGATION_COUNT" ] 2>/dev/null; then
    break
  fi

  sleep "$POLL_INTERVAL_SECONDS"
  elapsed=$((elapsed + POLL_INTERVAL_SECONDS))
done

echo ""
if [ "$count" -lt "$EXPECTED_INVESTIGATION_COUNT" ] 2>/dev/null; then
  echo "Nur ${count} von ${EXPECTED_INVESTIGATION_COUNT} erwarteten Investigations"
  echo "innerhalb von ${POLL_TIMEOUT_SECONDS}s gefunden - zeige trotzdem das bisher"
  echo "vorliegende Ergebnis, moeglicherweise noch nicht vollstaendig sortiert."
  echo "Pruefe manuell: docker compose logs deviation-detector-service analyzer-service"
fi

result=$(curl -s "$API_URL/investigations?limit=1" 2>/dev/null || true)

if [ -z "$result" ] || [ "$result" = "[]" ]; then
  echo "Kein Investigation-Ergebnis gefunden."
  echo "Pruefe manuell: docker compose logs deviation-detector-service analyzer-service"
  exit 1
fi

echo "=== Investigation-Ergebnis ==="
echo "$result" | python3 -m json.tool