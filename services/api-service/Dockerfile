# Build-Context MUSS das Repo-Root sein (siehe docker-compose.yml).

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY services/api-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY contracts/envelope.schema.json ./contracts/envelope.schema.json
COPY services/api-service/ .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]