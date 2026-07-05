#!/bin/bash
set -e

# Legt ein neues Python-Service-Verzeichnis unter services/<name> an,
# indem scaffold/python-service/ hineinkopiert und __SERVICE_NAME__
# ersetzt wird. Danach besteht KEINE Verbindung mehr zur Quelle -
# der neue Service besitzt seine Dateien vollstaendig selbst.
#
# Nutzung: ./scripts/new-service.sh mein-neuer-service

if [ -z "$1" ]; then
  echo "Usage: $0 <service-name>"
  echo "Beispiel: $0 metric-ingestion-service"
  exit 1
fi

SERVICE_NAME="$1"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="$REPO_ROOT/services/$SERVICE_NAME"
SCAFFOLD_DIR="$REPO_ROOT/scaffold/python-service"

if [ -d "$TARGET_DIR" ]; then
  echo "Fehler: services/$SERVICE_NAME existiert bereits."
  exit 1
fi

mkdir -p "$REPO_ROOT/services"
cp -r "$SCAFFOLD_DIR" "$TARGET_DIR"

# __SERVICE_NAME__ in allen kopierten Dateien ersetzen (macOS und Linux sed-kompatibel)
find "$TARGET_DIR" -type f | while read -r file; do
  sed -i.bak "s/__SERVICE_NAME__/$SERVICE_NAME/g" "$file"
  rm -f "$file.bak"
done

echo "Service angelegt: services/$SERVICE_NAME"
echo ""
echo "Noch zu erledigen:"
echo "  1. main.py durchgehen und an die tatsaechliche Rolle dieses Service anpassen:"
echo "     - Reiner Producer (z. B. ein Adapter)? Consumer-Teil (Queue-Bind, on_message_callback,"
echo "       start_consuming) entfernen."
echo "     - Reiner Consumer? Producer-Teil (falls nicht gebraucht) entfernen."
echo "     - Beides? __INPUT_EXCHANGE_NAME__ und __OUTPUT_EXCHANGE_NAME__ setzen."
echo "  2. handle_message() bzw. die Kernlogik mit der tatsaechlichen Aufgabe fuellen"
echo "  3. Docker-Compose-Eintrag fuer diesen Service manuell ergaenzen"
echo "     (build.context muss Repo-Root sein, dockerfile: services/$SERVICE_NAME/Dockerfile)"