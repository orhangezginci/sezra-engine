#!/bin/bash
set -e

# Demo-Szenario: keine einzelne Support-Anfrage ist fuer sich kritisch
# genug fuer context-severity-detector-service, aber eine ungewoehnliche
# HAeUFUNG zum selben Thema ist ein echtes Signal - genau der Fall, fuer
# den context-volume-detector-service gebaut wurde. Realistische,
# Jira-artige Ticket-Payloads (issue_key, summary, reporter, priority)
# statt generischer Testtexte, um naeher an echten Helpdesk-Daten zu
# sein, nicht nur an "pipeline-freundlichen" Mock-Saetzen.
#
# Reicht zunaechst einige thematisch UNABHAeNGIGE Tickets ein (kein
# Volumen-Signal, nur Hintergrundrauschen), dann eine Haeufung fast
# identischer "Passwort-Reset"-Tickets, die den Volumen-Schwellwert
# (Default 5) ueberschreiten sollte.
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

CONSUMER_QUEUES="sezra.queue.ingestion-service sezra.queue.knowledge-service sezra.queue.vectorizing-service sezra.queue.deviation-detector-service sezra.queue.persistence-service sezra.queue.analyzer-service sezra.queue.context-volume-detector-service"

STACK_SERVICES="rabbitmq postgres qdrant persistence-migrations api-service ingestion-service knowledge-service persistence-service vectorizing-service deviation-detector-service analyzer-service context-volume-detector-service"

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

echo "=== SEZRA Demo: Helpdesk - Ticket-Haeufung (Volumen-Anomalie) ==="
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
PROJECT_NAME="Demo: Helpdesk - $(date '+%Y-%m-%d %H:%M:%S')"
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

post_ticket() {
  curl -s -X POST "$API_URL/context" \
    -H "Content-Type: application/json" \
    -d "$1" > /dev/null
}

echo "1/2 Hintergrundrauschen: thematisch unabhaengige Tickets werden eingereicht..."
post_ticket "{\"project_id\": \"$PROJECT_ID\", \"issue_key\": \"HELP-4001\", \"summary\": \"Drucker im 2. Stock druckt nicht mehr\", \"reporter\": \"a.wagner@company.com\", \"priority\": \"Low\"}"
sleep 3
post_ticket "{\"project_id\": \"$PROJECT_ID\", \"issue_key\": \"HELP-4002\", \"summary\": \"Bitte Zugriff auf Shared Drive Marketing freischalten\", \"reporter\": \"l.becker@company.com\", \"priority\": \"Low\"}"
sleep 3
post_ticket "{\"project_id\": \"$PROJECT_ID\", \"issue_key\": \"HELP-4003\", \"summary\": \"VPN-Verbindung bricht gelegentlich ab\", \"reporter\": \"t.hoffmann@company.com\", \"priority\": \"Medium\"}"
sleep 3

echo "2/2 Haeufung fast identischer Passwort-Reset-Tickets wird eingereicht..."
post_ticket "{\"project_id\": \"$PROJECT_ID\", \"issue_key\": \"HELP-4471\", \"summary\": \"Passwort-Reset E-Mail kommt nicht an\", \"reporter\": \"m.schmidt@company.com\", \"priority\": \"Medium\"}"
sleep 3
post_ticket "{\"project_id\": \"$PROJECT_ID\", \"issue_key\": \"HELP-4472\", \"summary\": \"Passwort zuruecksetzen funktioniert nicht, keine E-Mail erhalten\", \"reporter\": \"s.fischer@company.com\", \"priority\": \"Medium\"}"
sleep 3
post_ticket "{\"project_id\": \"$PROJECT_ID\", \"issue_key\": \"HELP-4473\", \"summary\": \"Kann mein Passwort nicht zuruecksetzen, E-Mail fehlt\", \"reporter\": \"j.weber@company.com\", \"priority\": \"Medium\"}"
sleep 3
post_ticket "{\"project_id\": \"$PROJECT_ID\", \"issue_key\": \"HELP-4474\", \"summary\": \"Passwort-Reset-Mail kommt einfach nicht an\", \"reporter\": \"n.klein@company.com\", \"priority\": \"Medium\"}"
sleep 3
post_ticket "{\"project_id\": \"$PROJECT_ID\", \"issue_key\": \"HELP-4475\", \"summary\": \"Passwort vergessen - Reset-E-Mail wird nicht zugestellt\", \"reporter\": \"c.wolf@company.com\", \"priority\": \"Medium\"}"
sleep 3
post_ticket "{\"project_id\": \"$PROJECT_ID\", \"issue_key\": \"HELP-4476\", \"summary\": \"Erhalte keine E-Mail beim Passwort-Reset-Versuch\", \"reporter\": \"p.schroeder@company.com\", \"priority\": \"Medium\"}"
sleep 3
post_ticket "{\"project_id\": \"$PROJECT_ID\", \"issue_key\": \"HELP-4477\", \"summary\": \"Passwort-Reset-Funktion sendet keine Bestaetigungsmail\", \"reporter\": \"d.zimmermann@company.com\", \"priority\": \"Medium\"}"
sleep 3
post_ticket "{\"project_id\": \"$PROJECT_ID\", \"issue_key\": \"HELP-4478\", \"summary\": \"Keine Reset-Mail nach Klick auf Passwort vergessen erhalten\", \"reporter\": \"r.krueger@company.com\", \"priority\": \"Medium\"}"

echo ""
echo "Warte auf Investigation-Ergebnis (bis zu ${POLL_TIMEOUT_SECONDS}s)..."

elapsed=0
result=""
while [ "$elapsed" -lt "$POLL_TIMEOUT_SECONDS" ]; do
  result=$(curl -s "$API_URL/investigations?project_id=$PROJECT_ID&limit=5" 2>/dev/null || true)

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
  echo "Pruefe manuell: docker compose logs context-volume-detector-service analyzer-service"
  exit 1
fi

echo "=== Investigation-Ergebnis(se) ==="
echo "$result" | python3 -m json.tool