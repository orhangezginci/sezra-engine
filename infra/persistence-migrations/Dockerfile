# Build-Context MUSS das Repo-Root sein (siehe docker-compose.yml).
# Dieser Container laeuft einmalig (alembic upgrade head) und beendet
# sich danach - kein Dauerlauf-Service.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY infra/persistence-migrations/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY infra/persistence-migrations/ .

CMD ["alembic", "upgrade", "head"]