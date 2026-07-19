# Build-Context MUSS das Repo-Root sein (nicht dieser Ordner), damit
# contracts/ mitkopiert werden kann. Siehe docker-compose.yml-Eintrag
# fuer diesen Service.

FROM python:3.12-slim

# Ohne das bleiben print()-Ausgaben im Python-internen Puffer haengen,
# solange stdout nicht an ein Terminal geht (wie in Docker) - "docker
# compose logs" zeigt dann scheinbar gar nichts, obwohl der Service
# korrekt laeuft.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY services/vectorizing-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Embedding-Modell beim Image-Build vorab herunterladen (~300MB), damit
# der Container beim Start nicht jedes Mal neu laden muss - analog zum
# fruehreren ollama-model-pull-Job, nur diesmal in das Image selbst
# gebacken statt zur Laufzeit gegen einen externen Ollama-Prozess.
RUN python -c "from fastembed import TextEmbedding; TextEmbedding(model_name='jinaai/jina-embeddings-v2-base-de')"

COPY contracts/envelope.schema.json ./contracts/envelope.schema.json
COPY services/vectorizing-service/ .

CMD ["python", "main.py"]