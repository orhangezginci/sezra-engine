# Testet das Python-Scaffold selbst (scaffold/python-service/), nicht
# einen echten Service. Bewusst eigenstaendig, wie contracts-tests -
# keine gemeinsame Basis mit echten Service-Dockerfiles.

FROM python:3.12-slim

WORKDIR /app

COPY scaffold/python-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Das Scaffold erwartet contracts/envelope.schema.json neben sich,
# genau wie ein daraus erzeugter echter Service es beim Build bekommt.
COPY contracts/envelope.schema.json ./contracts/envelope.schema.json
COPY scaffold/python-service/ .

CMD ["pytest", "tests/", "-v"]
