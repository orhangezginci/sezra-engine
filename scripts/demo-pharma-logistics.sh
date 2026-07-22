#!/bin/bash
set -e

# Demo-Szenario: verzoegerte Zollabfertigung fuehrt zu laengeren
# Transitzeiten unter weniger kontrollierten Bedingungen, wodurch die
# Kuehlkette (temperaturkritischer Pharma-Transport, z. B. Impfstoffe/
# Biologika) haeufiger ueberschritten wird. Bewusst reine Logistik-
# Kennzahlen, KEIN Patientenbezug - SEZRA praesentiert sich hier als
# Korrelations-Hinweisgeber fuer Fachpersonal (Lieferketten-Monitoring),
# nicht als klinisches oder diagnostisches System.
#
# Metrik-zu-Metrik-Muster wie das E-Commerce-Szenario (checkout_error_rate
# -> conversion_rate), nur aus der Pharma-Lieferkette statt dem Online-
# Handel: customs_clearance_delay_hours (Ursache) -> 
# cold_chain_temperature_excursion_rate (Anomalie).
#
# Startet den kompletten Stack sauber (down + gezieltes Leeren von
# Postgres/Qdrant, ollama-data bleibt erhalten, falls vorhanden), reicht
# Baseline-Werte fuer beide Metriken sowie den Zoll-Verzoegerungs-Anstieg
# und den Kuehlketten-Uebershreitungs-Anstieg per HTTP ein, wartet auf
# und zeigt das Investigation-Ergebnis.
#
# Voraussetzung: im Repo-Root ausfuehren, curl muss lokal verfuegbar sein.

API_URL="http://localhost:8000"
RABBITMQ_MGMT_URL="http://localhost:15672"
POLL_INTERVAL_SECONDS=3
POLL_TIMEOUT_SECONDS=210
STACK_READY_TIMEOUT_SECONDS=180
CONSUMER_READY_TIMEOUT_SECONDS=60

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

echo "=== SEZRA Demo: Pharma-Logistik Scenario (Metrik -> Metrik) ==="
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

echo "1/4 Baseline: customs_clearance_delay_hours (stabil, ~4h)..."
for value in 3.8 4.1 4.0 3.9 4.2; do
  post_observation "{\"metric\": \"customs_clearance_delay_hours\", \"value\": $value}"
  sleep 3
done

echo "2/4 Baseline: cold_chain_temperature_excursion_rate (stabil, ~1.2%)..."
for value in 1.1 1.3 1.2 1.1 1.3; do
  post_observation "{\"metric\": \"cold_chain_temperature_excursion_rate\", \"value\": $value}"
  sleep 3
done

echo "3/4 Zoll-Verzoegerung steigt deutlich an (Ursache)..."
post_observation '{"metric": "customs_clearance_delay_hours", "value": 18.5}'
sleep 8

echo "4/4 Kuehlketten-Ueberschreitungsrate steigt an (Anomalie)..."
post_observation '{"metric": "cold_chain_temperature_excursion_rate", "value": 9.7}'

echo ""
echo "Warte auf Investigation-Ergebnis (bis zu ${POLL_TIMEOUT_SECONDS}s)..."

elapsed=0
result=""
while [ "$elapsed" -lt "$POLL_TIMEOUT_SECONDS" ]; do
  result=$(curl -s "$API_URL/investigations?limit=1" 2>/dev/null || true)

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
  echo "Pruefe manuell: docker compose logs deviation-detector-service analyzer-service"
  exit 1
fi

echo "=== Investigation-Ergebnis ==="
echo "$result" | python3 -m json.tool