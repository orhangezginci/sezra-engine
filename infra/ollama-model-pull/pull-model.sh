#!/bin/sh
set -e

MODELS="${OLLAMA_MODELS_TO_PULL:?Missing OLLAMA_MODELS_TO_PULL}"
OLLAMA_URL="http://${OLLAMA_HOST}:${OLLAMA_PORT}/api/pull"

pull_model() {
  MODEL="$1"
  echo "Pulling model '${MODEL}' from ${OLLAMA_URL}..."

  # /api/pull liefert einen Stream von JSON-Zeilen (Download-Fortschritt).
  # Wir geben jede Zeile aus (Sichtbarkeit beim Warten) und pruefen am
  # Ende, ob die letzte Zeile den Erfolg bestaetigt.
  RESPONSE=$(curl -s -X POST "$OLLAMA_URL" \
    -d "{\"name\": \"${MODEL}\"}" \
    --no-buffer | tee /dev/stderr | tail -n 1)

  if echo "$RESPONSE" | grep -q '"status":"success"'; then
    echo "Model '${MODEL}' pulled successfully."
    return 0
  else
    echo "Model pull did not report success for '${MODEL}'. Last response line: $RESPONSE"
    return 1
  fi
}

# OLLAMA_MODELS_TO_PULL ist kommagetrennt, z. B.
# "nomic-embed-text,qwen2.5:1.5b" - ein Modell fuer Embeddings, eines fuer
# Textgenerierung. Spaeter auf ein groesseres/besseres Modell zu wechseln
# ist dann nur eine Aenderung dieser Variable in docker-compose.yml, kein
# Code-Change.
OLD_IFS="$IFS"
IFS=','
for model in $MODELS; do
  pull_model "$model"
done
IFS="$OLD_IFS"

echo "All models pulled successfully."