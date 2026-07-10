# Build-Context MUSS das Repo-Root sein (siehe docker-compose.yml).
# Laeuft einmalig (pull-model.sh) und beendet sich danach.

FROM alpine:3.20

RUN apk add --no-cache curl

WORKDIR /app

COPY infra/ollama-model-pull/pull-model.sh .
RUN chmod +x pull-model.sh

CMD ["./pull-model.sh"]