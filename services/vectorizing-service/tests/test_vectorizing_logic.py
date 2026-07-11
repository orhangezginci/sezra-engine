"""
Tests für vectorizing-service.

Mockt Ollama (via requests_mock-freiem monkeypatch auf requests.post)
und Qdrant (Fake-Client), damit keine echten Verbindungen fuer die
Logik-Tests noetig sind.
"""

import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parents[1]))

os.environ["RABBITMQ_HOST"] = "localhost"
os.environ["RABBITMQ_PORT"] = "5672"
os.environ["RABBITMQ_USER"] = "test"
os.environ["RABBITMQ_PASSWORD"] = "test"
os.environ["QDRANT_HOST"] = "localhost"
os.environ["QDRANT_PORT"] = "6333"
os.environ["OLLAMA_HOST"] = "localhost"
os.environ["OLLAMA_PORT"] = "11434"
os.environ["OLLAMA_EMBEDDING_MODEL"] = "nomic-embed-text"

import pytest  # noqa: E402
import requests  # noqa: E402

import main  # noqa: E402
from main import (  # noqa: E402
    DEAD_LETTER_EXCHANGE,
    QDRANT_COLLECTION_NAME,
    create_embedding,
    ensure_qdrant_collection,
    handle_message,
    write_to_qdrant,
)


class FakeChannel:
    def __init__(self):
        self.published = []
        self.acked = []

    def basic_publish(self, exchange, routing_key, body, properties=None):
        self.published.append({"exchange": exchange, "body": json.loads(body)})

    def basic_ack(self, delivery_tag):
        self.acked.append(delivery_tag)


def make_method():
    return SimpleNamespace(delivery_tag=1)


VALID_ENVELOPE = {
    "schema_version": "1.1",
    "event_id": "6f9c2b1a-4e3a-4a3a-9c1a-2b1a4e3a4a3a",
    "event_type": "SemanticEnrichmentGenerated",
    "source": "knowledge-service",
    "occurred_at": "2026-07-05T10:00:00Z",
    "project_id": "1a2b3c4d-5e6f-4a3a-9c1a-2b1a4e3a4a3a",
    "payload": {
        "metric": "test",
        "value": 42,
        "semantic_text": "metric: test; value: 42",
        "source_occurred_at": "2026-07-05T09:58:00Z",
    },
}

FAKE_VECTOR = [0.1] * 768


class TestCreateEmbedding:
    def test_returns_vector_from_ollama_response(self, monkeypatch):
        fake_response = MagicMock()
        fake_response.json.return_value = {"embedding": FAKE_VECTOR}
        fake_response.raise_for_status.return_value = None
        monkeypatch.setattr(requests, "post", lambda *a, **kw: fake_response)

        vector = create_embedding("some text")

        assert vector == FAKE_VECTOR

    def test_raises_on_ollama_http_error(self, monkeypatch):
        fake_response = MagicMock()
        fake_response.raise_for_status.side_effect = requests.HTTPError("500")
        monkeypatch.setattr(requests, "post", lambda *a, **kw: fake_response)

        with pytest.raises(requests.HTTPError):
            create_embedding("some text")


class TestEnsureQdrantCollection:
    def test_creates_collection_when_absent(self):
        client = MagicMock()
        client.get_collections.return_value = SimpleNamespace(collections=[])

        ensure_qdrant_collection(client)

        client.create_collection.assert_called_once()
        assert client.create_collection.call_args.kwargs["collection_name"] == QDRANT_COLLECTION_NAME

    def test_skips_creation_when_already_exists(self):
        client = MagicMock()
        client.get_collections.return_value = SimpleNamespace(
            collections=[SimpleNamespace(name=QDRANT_COLLECTION_NAME)]
        )

        ensure_qdrant_collection(client)

        client.create_collection.assert_not_called()


class TestWriteToQdrant:
    def test_upserts_point_with_metadata(self):
        client = MagicMock()

        write_to_qdrant(client, VALID_ENVELOPE, FAKE_VECTOR)

        client.upsert.assert_called_once()
        call_kwargs = client.upsert.call_args.kwargs
        assert call_kwargs["collection_name"] == QDRANT_COLLECTION_NAME
        point = call_kwargs["points"][0]
        assert point.vector == FAKE_VECTOR
        assert point.payload["event_id"] == VALID_ENVELOPE["event_id"]
        assert point.payload["project_id"] == VALID_ENVELOPE["project_id"]
        assert point.payload["occurred_at"] == "2026-07-05T09:58:00Z"  # source_occurred_at, nicht envelope occurred_at
        assert point.payload["semantic_text"] == "metric: test; value: 42"

    def test_falls_back_to_envelope_occurred_at_when_source_occurred_at_missing(self):
        client = MagicMock()
        envelope = dict(VALID_ENVELOPE)
        envelope["payload"] = {"metric": "test", "value": 42, "semantic_text": "text"}

        write_to_qdrant(client, envelope, FAKE_VECTOR)

        point = client.upsert.call_args.kwargs["points"][0]
        assert point.payload["occurred_at"] == VALID_ENVELOPE["occurred_at"]


class TestHandleMessage:
    def test_valid_envelope_is_vectorized_and_acked(self, monkeypatch):
        fake_response = MagicMock()
        fake_response.json.return_value = {"embedding": FAKE_VECTOR}
        fake_response.raise_for_status.return_value = None
        monkeypatch.setattr(requests, "post", lambda *a, **kw: fake_response)

        channel = FakeChannel()
        qdrant_client = MagicMock()
        body = json.dumps(VALID_ENVELOPE).encode("utf-8")

        handle_message(channel, make_method(), None, body, qdrant_client)

        qdrant_client.upsert.assert_called_once()
        assert channel.acked == [1]
        assert channel.published == []

    def test_invalid_json_goes_to_dead_letter(self):
        channel = FakeChannel()
        qdrant_client = MagicMock()
        body = b"not valid json {{{"

        handle_message(channel, make_method(), None, body, qdrant_client)

        assert channel.published[0]["exchange"] == DEAD_LETTER_EXCHANGE
        assert channel.acked == [1]
        qdrant_client.upsert.assert_not_called()

    def test_missing_semantic_text_goes_to_dead_letter(self):
        channel = FakeChannel()
        qdrant_client = MagicMock()
        envelope = dict(VALID_ENVELOPE)
        envelope["payload"] = {"metric": "test", "value": 42}  # kein semantic_text
        body = json.dumps(envelope).encode("utf-8")

        handle_message(channel, make_method(), None, body, qdrant_client)

        assert channel.published[0]["exchange"] == DEAD_LETTER_EXCHANGE
        assert channel.acked == [1]
        qdrant_client.upsert.assert_not_called()

    def test_ollama_error_is_not_acked_and_not_dead_lettered(self, monkeypatch):
        def raise_connection_error(*args, **kwargs):
            raise requests.ConnectionError("Ollama unreachable")

        monkeypatch.setattr(requests, "post", raise_connection_error)

        channel = FakeChannel()
        qdrant_client = MagicMock()
        body = json.dumps(VALID_ENVELOPE).encode("utf-8")

        handle_message(channel, make_method(), None, body, qdrant_client)

        assert channel.acked == []
        assert channel.published == []
        qdrant_client.upsert.assert_not_called()

    def test_qdrant_error_is_not_acked_and_not_dead_lettered(self, monkeypatch):
        fake_response = MagicMock()
        fake_response.json.return_value = {"embedding": FAKE_VECTOR}
        fake_response.raise_for_status.return_value = None
        monkeypatch.setattr(requests, "post", lambda *a, **kw: fake_response)

        channel = FakeChannel()
        qdrant_client = MagicMock()
        qdrant_client.upsert.side_effect = Exception("Qdrant unreachable")
        body = json.dumps(VALID_ENVELOPE).encode("utf-8")

        handle_message(channel, make_method(), None, body, qdrant_client)

        assert channel.acked == []
        assert channel.published == []