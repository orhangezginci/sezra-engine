"""
Tests für context-volume-detector-service.

Nutzt realistische, Jira-artige Ticket-Payloads (issue_key, summary,
reporter, priority) statt generischer Testtexte.
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
os.environ["VOLUME_THRESHOLD"] = "5"
os.environ["VOLUME_SIMILARITY_THRESHOLD"] = "0.85"
os.environ["VOLUME_TIME_WINDOW_MINUTES"] = "60"

import numpy as np  # noqa: E402
import pytest  # noqa: E402

import main  # noqa: E402
from main import (  # noqa: E402
    DEAD_LETTER_EXCHANGE,
    OUTPUT_EXCHANGE,
    count_similar_recent_messages,
    create_embedding,
    create_volume_anomaly_event,
    handle_message,
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
    if error:
        def embed(documents):
            raise error
    else:
        array = np.array(vector or FAKE_VECTOR)

        def embed(documents):
            return iter([array])

    return SimpleNamespace(embed=embed)


def make_point(score, event_id, occurred_at, source_event_type="ContextIngested"):
    return SimpleNamespace(
        score=score,
        payload={
            "event_id": event_id,
            "occurred_at": occurred_at,
            "source_event_type": source_event_type,
        },
    )


def make_ticket_envelope(text: str, event_id: str = "ticket-event-id") -> dict:
    return {
        "schema_version": "1.1",
        "event_id": "6f9c2b1a-4e3a-4a3a-9c1a-2b1a4e3a4a3a",
        "event_type": "SemanticEnrichmentGenerated",
        "source": "knowledge-service",
        "occurred_at": "2026-07-22T10:00:00Z",
        "project_id": "1a2b3c4d-5e6f-4a3a-9c1a-2b1a4e3a4a3a",
        "payload": {
            "issue_key": "HELP-4471",
            "summary": text,
            "reporter": "m.schmidt@company.com",
            "priority": "Medium",
            "semantic_text": f"issue key: HELP-4471; summary: {text}; reporter: m.schmidt@company.com",
            "source_event_id": event_id,
            "source_event_type": "ContextIngested",
            "source_occurred_at": "2026-07-22T09:58:00Z",
        },
    }


FAKE_VECTOR = [0.1] * 768


class TestCountSimilarRecentMessages:
    def test_counts_similar_messages_above_threshold(self):
        client = MagicMock()
        client.query_points.return_value = SimpleNamespace(
            points=[
                make_point(0.9, "t1", "2026-07-22T09:30:00Z"),
                make_point(0.88, "t2", "2026-07-22T09:40:00Z"),
                make_point(0.86, "t3", "2026-07-22T09:50:00Z"),
            ]
        )

        count = count_similar_recent_messages(
            client, FAKE_VECTOR, "project-1", "2026-07-22T10:00:00Z", "excluded-id"
        )

        assert count == 3

    def test_excludes_messages_below_similarity_threshold(self):
        client = MagicMock()
        client.query_points.return_value = SimpleNamespace(
            points=[
                make_point(0.9, "t1", "2026-07-22T09:30:00Z"),
                make_point(0.5, "t2", "2026-07-22T09:40:00Z"),  # zu unaehnlich
            ]
        )

        count = count_similar_recent_messages(
            client, FAKE_VECTOR, "project-1", "2026-07-22T10:00:00Z", "excluded-id"
        )

        assert count == 1

    def test_excludes_messages_outside_time_window(self):
        client = MagicMock()
        client.query_points.return_value = SimpleNamespace(
            points=[
                make_point(0.9, "t1", "2026-07-22T09:30:00Z"),  # innerhalb 60min
                make_point(0.9, "t2", "2026-07-22T06:00:00Z"),  # ausserhalb
            ]
        )

        count = count_similar_recent_messages(
            client, FAKE_VECTOR, "project-1", "2026-07-22T10:00:00Z", "excluded-id"
        )

        assert count == 1

    def test_excludes_the_message_itself(self):
        """
        Kern des Selbstbezug-Schutzes: die aktuelle Nachricht darf sich
        nicht selbst mitzaehlen, egal ob vectorizing-service sie zu
        diesem Zeitpunkt schon vektorisiert hat (Race Condition zwischen
        zwei unabhaengigen Consumern derselben Exchange).
        """
        client = MagicMock()
        client.query_points.return_value = SimpleNamespace(
            points=[
                make_point(0.99, "self-id", "2026-07-22T09:59:00Z"),
                make_point(0.9, "other-id", "2026-07-22T09:50:00Z"),
            ]
        )

        count = count_similar_recent_messages(
            client, FAKE_VECTOR, "project-1", "2026-07-22T10:00:00Z", "self-id"
        )

        assert count == 1


class TestCreateVolumeAnomalyEvent:
    def test_anomaly_type_is_volume(self):
        envelope = make_ticket_envelope("Passwort-Reset funktioniert nicht")
        event = create_volume_anomaly_event(envelope, "issue key: HELP-4471; summary: ...", 7)

        assert event["payload"]["anomaly_type"] == "volume"
        assert event["payload"]["similar_message_count"] == 7

    def test_correlation_and_causation_id_propagated(self):
        envelope = make_ticket_envelope("Passwort-Reset funktioniert nicht")
        event = create_volume_anomaly_event(envelope, "text", 7)

        assert event["causation_id"] == envelope["event_id"]


class TestHandleMessage:
    def test_non_context_events_are_ignored(self):
        channel = FakeChannel()
        qdrant_client = MagicMock()
        envelope = {
            "schema_version": "1.1",
            "event_id": "6f9c2b1a-4e3a-4a3a-9c1a-2b1a4e3a4a3a",
            "event_type": "SemanticEnrichmentGenerated",
            "source": "knowledge-service",
            "occurred_at": "2026-07-22T10:00:00Z",
            "payload": {
                "metric": "x",
                "value": 1,
                "semantic_text": "metric: x; value: 1",
                "source_event_type": "ObservationIngested",
            },
        }
        body = json.dumps(envelope).encode("utf-8")

        handle_message(channel, make_method(), None, body, qdrant_client)

        assert channel.published == []
        assert channel.acked == [1]
        qdrant_client.query_points.assert_not_called()

    def test_volume_above_threshold_publishes_anomaly(self, monkeypatch):
        monkeypatch.setattr(main, "_embedding_model", fake_embedding_model())

        channel = FakeChannel()
        qdrant_client = MagicMock()
        qdrant_client.query_points.return_value = SimpleNamespace(
            points=[
                make_point(0.9, f"t{i}", "2026-07-22T09:30:00Z") for i in range(5)
            ]
        )

        envelope = make_ticket_envelope("Passwort-Reset funktioniert nicht")
        body = json.dumps(envelope).encode("utf-8")

        handle_message(channel, make_method(), None, body, qdrant_client)

        assert len(channel.published) == 1
        assert channel.published[0]["exchange"] == OUTPUT_EXCHANGE
        assert channel.published[0]["body"]["payload"]["similar_message_count"] == 5
        assert channel.acked == [1]

    def test_volume_below_threshold_does_not_publish(self, monkeypatch):
        monkeypatch.setattr(main, "_embedding_model", fake_embedding_model())

        channel = FakeChannel()
        qdrant_client = MagicMock()
        qdrant_client.query_points.return_value = SimpleNamespace(
            points=[make_point(0.9, "t1", "2026-07-22T09:30:00Z")]
        )

        envelope = make_ticket_envelope("Passwort-Reset funktioniert nicht")
        body = json.dumps(envelope).encode("utf-8")

        handle_message(channel, make_method(), None, body, qdrant_client)

        assert channel.published == []
        assert channel.acked == [1]

    def test_invalid_json_goes_to_dead_letter(self):
        channel = FakeChannel()
        qdrant_client = MagicMock()
        body = b"not valid json {{{"

        handle_message(channel, make_method(), None, body, qdrant_client)

        assert channel.published[0]["exchange"] == DEAD_LETTER_EXCHANGE
        assert channel.acked == [1]

    def test_embedding_error_is_not_acked(self, monkeypatch):
        monkeypatch.setattr(
            main, "_embedding_model", fake_embedding_model(error=RuntimeError("embedding failed"))
        )

        channel = FakeChannel()
        qdrant_client = MagicMock()
        envelope = make_ticket_envelope("Passwort-Reset funktioniert nicht")
        body = json.dumps(envelope).encode("utf-8")

        handle_message(channel, make_method(), None, body, qdrant_client)

        assert channel.acked == []
        assert channel.published == []

    def test_qdrant_error_is_not_acked(self, monkeypatch):
        monkeypatch.setattr(main, "_embedding_model", fake_embedding_model())

        channel = FakeChannel()
        qdrant_client = MagicMock()
        qdrant_client.query_points.side_effect = Exception("Qdrant unreachable")
        envelope = make_ticket_envelope("Passwort-Reset funktioniert nicht")
        body = json.dumps(envelope).encode("utf-8")

        handle_message(channel, make_method(), None, body, qdrant_client)

        assert channel.acked == []
        assert channel.published == []