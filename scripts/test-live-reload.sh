#!/bin/bash
set -e

# Kleines Testwerkzeug fuer SEZRA Studio's automatische Hintergrund-
# aktualisierung: findet einen Workspace anhand eines Namensteils,
# reicht eine einzelne kritische Kontext-Nachricht ein (loest
# context-severity-detector-service sofort aus, keine Baseline noetig),
# damit man in Studio pruefen kann, ob die neue Investigation ohne
# manuellen Klick auf "Aktualisieren" von selbst erscheint.
#
# Nicht Teil der eigentlichen Demo-Suite (demo-*.sh) - reines
# Testwerkzeug fuer die Studio-Entwicklung.
#
# Verwendung: ./scripts/test-live-reload.sh <Teil des Workspace-Namens>
# Beispiel:   ./scripts/test-live-reload.sh Severity

API_URL="http://localhost:8000"

if [ -z "$1" ]; then
  echo "Verwendung: $0 <Teil des Workspace-Namens>"
  echo "Beispiel:   $0 Severity"
  exit 1
fi

RESULT=$(curl -s "$API_URL/projects" | python3 -c "
import sys, json
search = sys.argv[1].lower()
projects = json.load(sys.stdin)
matches = [p for p in projects if search in p['name'].lower()]
if not matches:
    sys.exit(1)
print(matches[0]['id'])
print(matches[0]['name'])
" "$1")

if [ -z "$RESULT" ]; then
  echo "Kein Workspace gefunden, der '$1' enthaelt."
  echo "Verfuegbare Workspaces:"
  curl -s "$API_URL/projects" | python3 -c "
import sys, json
for p in json.load(sys.stdin):
    print(' -', p['name'])
"
  exit 1
fi

PROJECT_ID=$(echo "$RESULT" | sed -n '1p')
PROJECT_NAME=$(echo "$RESULT" | sed -n '2p')

echo "Workspace gefunden: $PROJECT_NAME"
echo "  ID: $PROJECT_ID"
echo ""
echo "Sende kritische Test-Nachricht..."

curl -s -X POST "$API_URL/context" \
  -H "Content-Type: application/json" \
  -d "{\"project_id\": \"$PROJECT_ID\", \"sender\": \"test@example.com\", \"subject\": \"Live-Reload-Test $(date '+%H:%M:%S')\", \"text\": \"Kritischer Systemausfall im Hauptserver festgestellt.\"}" \
  > /dev/null

echo "Gesendet."
echo ""
echo "Jetzt in Studio den Workspace '$PROJECT_NAME' geoeffnet lassen und"
echo "NICHT auf 'Aktualisieren' klicken - die neue Investigation sollte"
echo "innerhalb von ca. 10-15s von selbst erscheinen."