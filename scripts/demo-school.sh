#!/bin/bash
set -e

# Demo-Szenario: frueherer Unterrichtsbeginn -> Notenabfall in Periode 1.
# Startet den kompletten Stack sauber neu (down -v + up --build), reicht
# Rektor-Mail + Notenverlauf + Abfall ein, wartet auf und zeigt das
# Investigation-Ergebnis. Ein einziger Befehl, keine manuellen
# Docker-Kommandos drumherum noetig.
#
# Voraussetzung: im Repo-Root ausfuehren.

INBOX_DIR="data/json-inbox"
POLL_INTERVAL_SECONDS=3
POLL_TIMEOUT_SECONDS=60
STACK_READY_TIMEOUT_SECONDS=180

STACK_SERVICES="rabbitmq postgres ollama ollama-model-pull qdrant json-adapter-service ingestion-service knowledge-service persistence-service vectorizing-service deviation-detector-service analyzer-service"

if [ ! -d "$INBOX_DIR" ]; then
  echo "Fehler: $INBOX_DIR nicht gefunden. Im Repo-Root ausfuehren."
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
        # Manche docker-compose-Versionen liefern ein einzelnes JSON-Array
        parsed = json.loads(raw)
        entries = parsed if isinstance(parsed, list) else [parsed]
    except json.JSONDecodeError:
        # Andere liefern JSON Lines (ein Objekt pro Zeile)
        for line in raw.splitlines():
            line = line.strip()
            if line:
                entries.append(json.loads(line))

count = 0
for entry in entries:
    state = entry.get('State', '')
    health = entry.get('Health', '')
    if entry.get('Service') == 'ollama-model-pull':
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

echo "Stack ist bereit."
echo ""

echo "1/3 Rektor-Mail (Kontext) wird eingereicht..."
cat > "$INBOX_DIR/rektor-mail.json" <<'EOF'
{"source_type": "context", "sender": "rektor@schule.de", "subject": "Neuer Unterrichtsbeginn", "text": "Liebe Kolleginnen und Kollegen, ab naechster Woche wird der Unterrichtsbeginn von 7:30 auf 7:00 Uhr um eine halbe Stunde vorgezogen."}
EOF
sleep 5

echo "2/3 Baseline-Werte fuer math_test_average, Periode 1 werden eingereicht..."
for value in 78 79 77 78 80; do
  cat > "$INBOX_DIR/period1-$value.json" <<EOF
{"source_type": "observation", "metric": "math_test_average", "period": 1, "value": $value}
EOF
  sleep 3
done

echo "3/3 Notenabfall wird eingereicht..."
cat > "$INBOX_DIR/period1-drop.json" <<'EOF'
{"source_type": "observation", "metric": "math_test_average", "period": 1, "value": 45}
EOF

echo ""
echo "Warte auf Investigation-Ergebnis (bis zu ${POLL_TIMEOUT_SECONDS}s)..."

elapsed=0
result=""
while [ "$elapsed" -lt "$POLL_TIMEOUT_SECONDS" ]; do
  result=$(docker compose exec -T postgres psql -U sezra -d sezra_engine -t -A \
    -c "SELECT payload FROM events WHERE event_type = 'InvestigationGenerated' AND received_at > now() - interval '2 minutes' ORDER BY received_at DESC LIMIT 1;" \
    2>/dev/null || true)

  if [ -n "$result" ]; then
    break
  fi

  sleep "$POLL_INTERVAL_SECONDS"
  elapsed=$((elapsed + POLL_INTERVAL_SECONDS))
done

echo ""
if [ -z "$result" ]; then
  echo "Kein Investigation-Ergebnis innerhalb von ${POLL_TIMEOUT_SECONDS}s gefunden."
  echo "Pruefe manuell: docker compose logs deviation-detector-service analyzer-service"
  exit 1
fi

echo "=== Investigation-Ergebnis ==="
echo "$result" | python3 -m json.tool