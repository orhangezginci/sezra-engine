# Nur fuer die Contract-Tests (contracts/ + tests/contracts/).
# Bewusst NICHT als gemeinsame Basis fuer andere Services gedacht -
# jeder Service bekommt sein eigenes, zweckgebundenes Dockerfile,
# solange kein echter Bedarf fuer eine gemeinsame Basis entsteht.

FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    "jsonschema[format]==4.*" \
    rfc3339-validator==0.1.* \
    pytest==8.*

COPY contracts/ ./contracts/
COPY tests/contracts/ ./tests/contracts/

CMD ["pytest", "tests/contracts/", "-v"]
