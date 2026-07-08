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

COPY services/knowledge-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY contracts/envelope.schema.json ./contracts/envelope.schema.json
COPY services/knowledge-service/ .

CMD ["python", "main.py"]