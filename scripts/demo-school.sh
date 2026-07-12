#!/bin/bash
set -e

# Demo-Szenario: frueherer Unterrichtsbeginn -> Notenabfall in Periode 1.
# Startet den kompletten Stack sauber neu (down -v + up --build), reicht
# Rektor-Mail + Notenverlauf + Abfall per HTTP an api-service ein (kein
# Datei-Drop mehr - das ist der Weg, den auch ein spaeteres Studio-
# Frontend nehmen wuerde), wartet auf und zeigt das Investigation-
# Ergebnis. Ein einziger Befehl, keine manuellen Docker-/curl-Kommandos
# drumherum noetig.
#
# Voraussetzung: im Repo-Root ausfuehren, curl muss lokal verfuegbar sein.

API_URL="http://localhost:8000"
RABBITMQ_MGMT_URL="http://localhost:15672"
POLL_INTERVAL_SECONDS=3
POLL_TIMEOUT_SECONDS=60
STACK_READY_TIMEOUT_SECONDS=180
CONSUMER_READY_TIMEOUT_SECONDS=60

# Queues, die tatsaechlich einen aktiven Consumer haben MUESSEN, bevor wir
# Daten einreichen - sonst gehen fanout-Nachrichten, die ankommen, waehrend
# die Queue noch ungebunden ist, unwiederbringlich verloren (genau das
# Problem, das die Rektor-Mail beim allerersten API-Testlauf verschluckt
# hat: der Container lief schon, aber vectorizing-service hatte seine
# Queue an sezra.stream.enriched.semantic noch nicht gebunden).
CONSUMER_QUEUES="sezra.queue.ingestion-service sezra.queue.knowledge-service sezra.queue.vectorizing-service sezra.queue.deviation-detector-service sezra.queue.persistence-service sezra.queue.analyzer-service"

# json-adapter-service wird fuer diese Demo nicht gebraucht (wir reichen
# per HTTP ein, nicht per Datei) - laeuft aber nicht im Weg, falls man
# ihn trotzdem mitstarten will. Hier bewusst weggelassen, um den Stack
# schlank zu halten.
STACK_SERVICES="rabbitmq postgres ollama ollama-model-pull qdrant persistence-migrations api-service ingestion-service knowledge-service persistence-service vectorizing-service deviation-detector-service analyzer-service"

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

echo "=== SEZRA Demo: School Scenario ==="
echo ""

echo "0/4 Stack wird sauber neu gestartet (down -v + up --build)..."
docker compose down -v > /dev/null 2>&1 || true
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
    if entry.get('Service') in ('ollama-model-pull', 'persistence-migrations'):
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

# api-service selbst braucht noch einen Moment nach "running", bis
# uvicorn tatsaechlich Requests annimmt.
echo "Warte auf api-service..."
for i in $(seq 1 20); do
  if curl -s -o /dev/null -w "" "$API_URL/health" 2>/dev/null; then
    break
  fi
  sleep 1
done

echo "Stack ist bereit."
echo ""

echo "1/3 Rektor-Mail (Kontext) wird per POST eingereicht..."
curl -s -X POST "$API_URL/context" \
  -H "Content-Type: application/json" \
  -d '{"sender": "rektor@schule.de", "subject": "Neuer Unterrichtsbeginn", "text": "Liebe Kolleginnen und Kollegen, ab naechster Woche wird der Unterrichtsbeginn von 7:30 auf 7:00 Uhr um eine halbe Stunde vorgezogen."}' \
  > /dev/null
sleep 5

echo "2/3 Baseline-Werte fuer math_test_average, Periode 1 werden per POST eingereicht..."
for value in 78 79 77 78 80; do
  curl -s -X POST "$API_URL/observations" \
    -H "Content-Type: application/json" \
    -d "{\"metric\": \"math_test_average\", \"period\": 1, \"value\": $value}" \
    > /dev/null
  sleep 3
done

echo "3/3 Notenabfall wird per POST eingereicht..."
curl -s -X POST "$API_URL/observations" \
  -H "Content-Type: application/json" \
  -d '{"metric": "math_test_average", "period": 1, "value": 45}' \
  > /dev/null

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