#!/bin/sh
set -e

MODEL="${OLLAMA_EMBEDDING_MODEL:?Missing OLLAMA_EMBEDDING_MODEL}"
OLLAMA_URL="http://${OLLAMA_HOST}:${OLLAMA_PORT}/api/pull"

echo "Pulling model '${MODEL}' from ${OLLAMA_URL}..."

# /api/pull liefert einen Stream von JSON-Zeilen (Download-Fortschritt).
# Wir geben jede Zeile aus (Sichtbarkeit beim Warten) und pruefen am
# Ende, ob die letzte Zeile den Erfolg bestaetigt.
RESPONSE=$(curl -s -X POST "$OLLAMA_URL" \
  -d "{\"name\": \"${MODEL}\"}" \
  --no-buffer | tee /dev/stderr | tail -n 1)

if echo "$RESPONSE" | grep -q '"status":"success"'; then
  echo "Model '${MODEL}' pulled successfully."
  exit 0
else
  echo "Model pull did not report success. Last response line: $RESPONSE"
  exit 1
fi