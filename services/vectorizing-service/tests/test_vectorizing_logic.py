"""
Tests für vectorizing-service.

Mockt FastEmbed (via monkeypatch auf main._embedding_model) und Qdrant
(Fake-Client), damit keine echten Modell-Downloads oder Verbindungen
fuer die Logik-Tests noetig sind.
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

import numpy as np  # noqa: E402
import pytest  # noqa: E402

import main  # noqa: E402
from main import (  # noqa: E402
    DEAD_LETTER_EXCHANGE,
    QDRANT_COLLECTION_NAME,
    build_composite_key,
    create_embedding,
    ensure_qdrant_collection,
    handle_message,
    is_observation,
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


def fake_embedding_model(vector=None, error=None):
    """
    Ersetzt main._embedding_model - liefert entweder einen festen Vektor
    (als numpy-Array, wie FastEmbed ihn tatsaechlich zurueckgibt, inkl.
    .tolist()) oder wirft einen Fehler, um Fehlerpfade zu testen.
    """
    if error:
        def embed(documents):
            raise error
    else:
        array = np.array(vector or FAKE_VECTOR)

        def embed(documents):
            return iter([array])

    return SimpleNamespace(embed=embed)


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
        "source_event_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    },
}

FAKE_VECTOR = [0.1] * 768


class TestCreateEmbedding:
    def test_returns_vector_from_embedding_model(self, monkeypatch):
        monkeypatch.setattr(main, "_embedding_model", fake_embedding_model())

        vector = create_embedding("some text")

        assert vector == FAKE_VECTOR

    def test_passes_raw_text_without_prefix(self, monkeypatch):
        """
        jina-embeddings-v2-base-de ist ein SYMMETRISCHES Modell (anders
        als nomic-embed-text) - kein "search_query:"/"search_document:"-
        Prefix noetig, der Text geht unveraendert ins Modell.
        """
        captured = {}
        array = np.array(FAKE_VECTOR)

        def fake_embed(documents):
            captured["documents"] = list(documents)
            return iter([array])

        monkeypatch.setattr(main, "_embedding_model", SimpleNamespace(embed=fake_embed))

        create_embedding("some text")

        assert captured["documents"] == ["some text"]

    def test_propagates_embedding_model_errors(self, monkeypatch):
        monkeypatch.setattr(
            main, "_embedding_model", fake_embedding_model(error=RuntimeError("model error"))
        )

        with pytest.raises(RuntimeError):
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


class TestIsObservation:
    def test_metric_and_value_present_is_observation(self):
        assert is_observation({"metric": "x", "value": 1}) is True

    def test_context_payload_is_not_observation(self):
        assert is_observation({"sender": "rektor@schule.de", "text": "..."}) is False


class TestBuildCompositeKey:
    def test_matches_deviation_detector_service_for_same_raw_fields(self):
        """
        Kritischer Konsistenz-Test: der composite_key MUSS identisch mit
        dem im deviation-detector-service berechneten sein, sonst kann
        der Analyzer eine Beobachtungsreihe nie korrekt von sich selbst
        als Ursachenkandidat ausschliessen. Simuliert hier den Aufruf mit
        dem ANGEREICHERTEN Payload (wie vectorizing-service ihn bekommt),
        muss aber trotzdem denselben Key wie aus dem ROHEN Payload liefern.
        """
        raw_payload = {"source_type": "observation", "metric": "math_test_average", "period": 1, "value": 79}
        enriched_payload = {
            **raw_payload,
            "semantic_text": "some text",
            "source_event_id": "unique-per-event-id",
            "source_event_type": "ObservationIngested",
            "source_occurred_at": "2026-07-11T08:00:00Z",
        }

        assert build_composite_key(enriched_payload) == build_composite_key(raw_payload)

    def test_metric_alone_when_no_other_dimensions(self):
        assert build_composite_key({"metric": "simple_metric", "value": 1}) == "simple_metric"


class TestWriteToQdrant:
    def test_upserts_point_with_metadata(self):
        client = MagicMock()

        write_to_qdrant(client, VALID_ENVELOPE, FAKE_VECTOR)

        client.upsert.assert_called_once()
        call_kwargs = client.upsert.call_args.kwargs
        assert call_kwargs["collection_name"] == QDRANT_COLLECTION_NAME
        point = call_kwargs["points"][0]
        assert point.vector == FAKE_VECTOR
        assert point.payload["event_id"] == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"  # source_event_id, nicht der Wrapper selbst
        assert point.payload["project_id"] == VALID_ENVELOPE["project_id"]
        assert point.payload["occurred_at"] == "2026-07-05T09:58:00Z"  # source_occurred_at, nicht envelope occurred_at

    def test_falls_back_to_envelope_event_id_when_source_event_id_missing(self):
        """
        Regressionstest: ohne diesen Fallback (und ohne den urspruenglichen
        Bug ueberhaupt zu fixen) landete envelope["event_id"] (die frisch
        generierte ID des Anreicherungs-Wrappers) in Qdrant statt der
        Original-Event-ID - dadurch griff der Selbstbezugs-Ausschluss im
        Analyzer nie, und Kreuzverweise zwischen Investigations
        (checkout_error_rate als Ursache fuer conversion_rate) wurden
        nie erkannt, weil die IDs nie uebereinstimmten.
        """
        client = MagicMock()
        envelope = dict(VALID_ENVELOPE)
        envelope["payload"] = {"metric": "test", "value": 42, "semantic_text": "text"}

        write_to_qdrant(client, envelope, FAKE_VECTOR)

        point = client.upsert.call_args.kwargs["points"][0]
        assert point.payload["event_id"] == VALID_ENVELOPE["event_id"]

    def test_falls_back_to_envelope_occurred_at_when_source_occurred_at_missing(self):
        client = MagicMock()
        envelope = dict(VALID_ENVELOPE)
        envelope["payload"] = {"metric": "test", "value": 42, "semantic_text": "text"}

        write_to_qdrant(client, envelope, FAKE_VECTOR)

        point = client.upsert.call_args.kwargs["points"][0]
        assert point.payload["occurred_at"] == VALID_ENVELOPE["occurred_at"]

    def test_composite_key_is_included_for_observations(self):
        client = MagicMock()

        write_to_qdrant(client, VALID_ENVELOPE, FAKE_VECTOR)

        point = client.upsert.call_args.kwargs["points"][0]
        assert point.payload["composite_key"] == "test"

    def test_composite_key_is_none_for_context_events(self):
        client = MagicMock()
        envelope = dict(VALID_ENVELOPE)
        envelope["payload"] = {
            "sender": "rektor@schule.de",
            "text": "...",
            "semantic_text": "sender: rektor@schule.de; text: ...",
        }

        write_to_qdrant(client, envelope, FAKE_VECTOR)

        point = client.upsert.call_args.kwargs["points"][0]
        assert point.payload["composite_key"] is None


class TestHandleMessage:
    def test_valid_envelope_is_vectorized_and_acked(self, monkeypatch):
        monkeypatch.setattr(main, "_embedding_model", fake_embedding_model())

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

    def test_embedding_error_is_not_acked_and_not_dead_lettered(self, monkeypatch):
        """
        Vorher "Ollama unreachable" (requests.ConnectionError) - jetzt
        generisch, da FastEmbed lokal im Prozess laeuft und keine
        Netzwerkfehler mehr wirft, sondern z. B. Ressourcen-Fehler.
        """
        monkeypatch.setattr(
            main, "_embedding_model", fake_embedding_model(error=RuntimeError("embedding failed"))
        )

        channel = FakeChannel()
        qdrant_client = MagicMock()
        body = json.dumps(VALID_ENVELOPE).encode("utf-8")

        handle_message(channel, make_method(), None, body, qdrant_client)

        assert channel.acked == []
        assert channel.published == []
        qdrant_client.upsert.assert_not_called()

    def test_qdrant_error_is_not_acked_and_not_dead_lettered(self, monkeypatch):
        monkeypatch.setattr(main, "_embedding_model", fake_embedding_model())

        channel = FakeChannel()
        qdrant_client = MagicMock()
        qdrant_client.upsert.side_effect = Exception("Qdrant unreachable")
        body = json.dumps(VALID_ENVELOPE).encode("utf-8")

        handle_message(channel, make_method(), None, body, qdrant_client)

        assert channel.acked == []
        assert channel.published == []