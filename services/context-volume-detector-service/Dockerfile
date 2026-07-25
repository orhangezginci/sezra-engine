# Build-Context MUSS das Repo-Root sein (nicht dieser Ordner), damit
# contracts/ mitkopiert werden kann. Siehe docker-compose.yml-Eintrag
# fuer diesen Service.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY services/context-volume-detector-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Embedding-Modell beim Image-Build vorab herunterladen (~300MB), damit
# der Container beim Start nicht jedes Mal neu laden muss - muss
# identisch zu vectorizing-service/analyzer-service sein.
RUN python -c "from fastembed import TextEmbedding; TextEmbedding(model_name='jinaai/jina-embeddings-v2-base-de')"

COPY contracts/envelope.schema.json ./contracts/envelope.schema.json
COPY services/context-volume-detector-service/ .

CMD ["python", "main.py"]