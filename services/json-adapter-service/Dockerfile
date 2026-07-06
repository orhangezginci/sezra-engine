# Build-Context MUSS das Repo-Root sein (nicht dieser Ordner), damit
# contracts/ mitkopiert werden kann. Siehe docker-compose.yml-Eintrag
# fuer diesen Service.

FROM python:3.12-slim

WORKDIR /app

COPY services/json-adapter-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY contracts/envelope.schema.json ./contracts/envelope.schema.json
COPY services/json-adapter-service/ .

CMD ["python", "main.py"]