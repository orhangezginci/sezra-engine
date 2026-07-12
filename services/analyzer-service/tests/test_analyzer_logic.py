"""
Tests für analyzer-service.

Nutzt das School-Szenario (Notenabfall, Rektor-Mail ueber frueheren
Unterrichtsbeginn) um die drei zentralen Eigenschaften konkret zu pruefen:
Zeitfilter, sichtbarer Confidence-Score, Unsicherheits-Fallback.
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
os.environ["OLLAMA_GENERATION_MODEL"] = "qwen2.5:1.5b"
os.environ["ANALYZER_CONFIDENCE_THRESHOLD"] = "0.5"

import pytest  # noqa: E402
import requests  # noqa: E402

from main import (  # noqa: E402
    DEAD_LETTER_EXCHANGE,
    OUTPUT_EXCHANGE,
    build_anomaly_search_text,
    build_investigation_payload,
    create_embedding,
    create_investigation_event,
    generate_causal_explanation,
    handle_message,
    search_related_context,
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


def make_point(score, semantic_text, occurred_at, event_id="ctx-event", composite_key=None):
    return SimpleNamespace(
        score=score,
        payload={
            "event_id": event_id,
            "semantic_text": semantic_text,
            "occurred_at": occurred_at,
            "composite_key": composite_key,
        },
    )


ANOMALY_ENVELOPE = {
    "schema_version": "1.1",
    "event_id": "6f9c2b1a-4e3a-4a3a-9c1a-2b1a4e3a4a3a",
    "event_type": "AnomalyDetected",
    "source": "deviation-detector-service",
    "occurred_at": "2026-07-11T08:00:05Z",
    "project_id": "1a2b3c4d-5e6f-4a3a-9c1a-2b1a4e3a4a3a",
    "payload": {
        "anomaly_type": "drop",
        "metric": "math_test_average",
        "composite_key": "math_test_average|period=1",
        "previous_value": 78,
        "current_value": 45,
        "change_amount": -33,
        "reason": "value decreased significantly compared to recent history",
        "source_event_id": "obs-event-id",
        "source_occurred_at": "2026-07-11T08:00:00Z",
    },
}

REKTOR_MAIL_TEXT = (
    "sender: rektor@schule.de; subject: Neuer Unterrichtsbeginn; "
    "text: Liebe Kolleginnen und Kollegen, ab naechster Woche beginnt "
    "der Unterricht um 7:00 statt 7:30 Uhr."
)


class TestCreateEmbedding:
    def test_prepends_search_query_prefix(self, monkeypatch):
        """
        nomic-embed-text erwartet dieses Task-Prefix fuer Suchanfragen,
        unterschieden von search_document (vectorizing-service) fuer die
        gespeicherten Texte.
        """
        captured = {}

        def fake_post(url, json, timeout):
            captured["prompt"] = json["prompt"]
            response = MagicMock()
            response.json.return_value = {"embedding": [0.1] * 768}
            response.raise_for_status.return_value = None
            return response

        monkeypatch.setattr(requests, "post", fake_post)

        create_embedding("some anomaly description")

        assert captured["prompt"] == "search_query: some anomaly description"


class TestBuildAnomalySearchText:
    def test_includes_metric_and_values(self):
        text = build_anomaly_search_text(ANOMALY_ENVELOPE["payload"])

        assert "math_test_average" in text
        assert "78" in text
        assert "45" in text

    def test_includes_reason(self):
        text = build_anomaly_search_text(ANOMALY_ENVELOPE["payload"])

        assert "decreased significantly" in text


class TestSearchRelatedContext:
    def test_candidate_before_anomaly_is_kept(self):
        client = MagicMock()
        client.query_points.return_value = SimpleNamespace(
            points=[make_point(0.87, REKTOR_MAIL_TEXT, "2026-07-04T06:00:00Z")]
        )

        results = search_related_context(
            client, [0.1] * 768, ANOMALY_ENVELOPE["project_id"], "2026-07-11T08:00:00Z", None
        )

        assert len(results) == 1
        assert results[0]["occurred_before_anomaly"] is True
        assert results[0]["confidence"] == 0.87

    def test_candidate_after_anomaly_is_rejected(self):
        """
        Der Kern des Zeitfilters: eine Mail, die NACH der Anomalie
        verschickt wurde, kann nicht deren Ursache sein.
        """
        client = MagicMock()
        client.query_points.return_value = SimpleNamespace(
            points=[make_point(0.95, "eine spaetere, unrelated Mail", "2026-07-12T09:00:00Z")]
        )

        results = search_related_context(
            client, [0.1] * 768, ANOMALY_ENVELOPE["project_id"], "2026-07-11T08:00:00Z", None
        )

        assert results == []

    def test_mixed_candidates_only_before_anomaly_survive(self):
        client = MagicMock()
        client.query_points.return_value = SimpleNamespace(
            points=[
                make_point(0.90, "vor der anomalie", "2026-07-04T06:00:00Z", event_id="before"),
                make_point(0.95, "nach der anomalie", "2026-07-12T06:00:00Z", event_id="after"),
            ]
        )

        results = search_related_context(
            client, [0.1] * 768, ANOMALY_ENVELOPE["project_id"], "2026-07-11T08:00:00Z", None
        )

        assert len(results) == 1
        assert results[0]["source_event_id"] == "before"

    def test_project_id_filter_is_applied_to_qdrant_query(self):
        client = MagicMock()
        client.query_points.return_value = SimpleNamespace(points=[])

        search_related_context(client, [0.1] * 768, "some-project-id", "2026-07-11T08:00:00Z", None)

        call_kwargs = client.query_points.call_args.kwargs
        assert call_kwargs["query_filter"] is not None

    def test_same_composite_key_as_anomaly_is_excluded(self):
        """
        Der Kern des Selbst-Ausschluss-Fixes: eine Beobachtung aus
        DERSELBEN Reihe wie die Anomalie (math_test_average|period=1)
        darf nicht als "Ursache" fuer sich selbst erscheinen.
        """
        client = MagicMock()
        client.query_points.return_value = SimpleNamespace(
            points=[
                make_point(
                    0.95, "eine weitere Beobachtung derselben Reihe", "2026-07-04T06:00:00Z",
                    event_id="same-series", composite_key="math_test_average|period=1",
                ),
                make_point(
                    0.46, REKTOR_MAIL_TEXT, "2026-07-04T06:00:00Z",
                    event_id="the-email", composite_key=None,
                ),
            ]
        )

        results = search_related_context(
            client, [0.1] * 768, ANOMALY_ENVELOPE["project_id"], "2026-07-11T08:00:00Z",
            "math_test_average|period=1",
        )

        assert len(results) == 1
        assert results[0]["source_event_id"] == "the-email"

    def test_different_composite_key_is_kept(self):
        """
        Eine ANDERE Metrik-Reihe (unterschiedlicher composite_key) bleibt
        weiterhin ein legitimer Kandidat - z. B. das Manufacturing-
        Szenario, wo Metrik A eine andere Metrik B erklaeren kann.
        """
        client = MagicMock()
        client.query_points.return_value = SimpleNamespace(
            points=[
                make_point(
                    0.8, "andere Metrik-Reihe", "2026-07-04T06:00:00Z",
                    event_id="other-series", composite_key="teacher_sick_days|period=1",
                ),
            ]
        )

        results = search_related_context(
            client, [0.1] * 768, ANOMALY_ENVELOPE["project_id"], "2026-07-11T08:00:00Z",
            "math_test_average|period=1",
        )

        assert len(results) == 1

    def test_results_sorted_by_confidence_descending(self):
        client = MagicMock()
        client.query_points.return_value = SimpleNamespace(
            points=[
                make_point(0.6, "schwaecherer Treffer", "2026-07-04T06:00:00Z", event_id="a"),
                make_point(0.9, "staerkerer Treffer", "2026-07-03T06:00:00Z", event_id="b"),
            ]
        )

        results = search_related_context(
            client, [0.1] * 768, ANOMALY_ENVELOPE["project_id"], "2026-07-11T08:00:00Z", None
        )

        assert results[0]["source_event_id"] == "b"


class TestGenerateCausalExplanation:
    def test_returns_generated_text(self, monkeypatch):
        fake_response = MagicMock()
        fake_response.json.return_value = {"response": "  Der frühere Beginn könnte zu weniger Schlaf geführt haben.  "}
        fake_response.raise_for_status.return_value = None
        monkeypatch.setattr(requests, "post", lambda *a, **kw: fake_response)

        result = generate_causal_explanation("math_test_average dropped", REKTOR_MAIL_TEXT)

        assert result == "Der frühere Beginn könnte zu weniger Schlaf geführt haben."

    def test_uses_generate_endpoint_not_embeddings(self, monkeypatch):
        captured = {}

        def fake_post(url, json, timeout):
            captured["url"] = url
            response = MagicMock()
            response.json.return_value = {"response": "text"}
            response.raise_for_status.return_value = None
            return response

        monkeypatch.setattr(requests, "post", fake_post)

        generate_causal_explanation("summary", "cause")

        assert "/api/generate" in captured["url"]

    def test_returns_none_on_failure_instead_of_raising(self, monkeypatch):
        def raise_error(*args, **kwargs):
            raise requests.ConnectionError("Ollama unreachable")

        monkeypatch.setattr(requests, "post", raise_error)

        result = generate_causal_explanation("summary", "cause")

        assert result is None


class TestBuildInvestigationPayload:
    def test_confident_candidate_is_included(self, monkeypatch):
        fake_response = MagicMock()
        fake_response.json.return_value = {"response": "generated explanation"}
        fake_response.raise_for_status.return_value = None
        monkeypatch.setattr(requests, "post", lambda *a, **kw: fake_response)

        candidates = [
            {
                "semantic_text": REKTOR_MAIL_TEXT,
                "confidence": 0.87,
                "source_event_id": "ctx-1",
                "occurred_at": "2026-07-04T06:00:00Z",
                "occurred_before_anomaly": True,
            }
        ]

        result = build_investigation_payload(ANOMALY_ENVELOPE, candidates)

        assert len(result["possible_causes"]) == 1
        assert result["possible_causes"][0]["confidence"] == 0.87

    def test_confident_candidate_gets_an_explanation(self, monkeypatch):
        fake_response = MagicMock()
        fake_response.json.return_value = {"response": "generated explanation"}
        fake_response.raise_for_status.return_value = None
        monkeypatch.setattr(requests, "post", lambda *a, **kw: fake_response)

        candidates = [
            {
                "semantic_text": REKTOR_MAIL_TEXT,
                "confidence": 0.87,
                "source_event_id": "ctx-1",
                "occurred_at": "2026-07-04T06:00:00Z",
                "occurred_before_anomaly": True,
            }
        ]

        result = build_investigation_payload(ANOMALY_ENVELOPE, candidates)

        assert result["possible_causes"][0]["explanation"] == "generated explanation"

    def test_explanation_failure_does_not_break_the_investigation(self, monkeypatch):
        """
        Wenn die Generierung fehlschlaegt, bleibt die Investigation
        trotzdem nutzbar - explanation ist dann None, kein Absturz.
        """
        def raise_error(*args, **kwargs):
            raise requests.ConnectionError("Ollama unreachable")

        monkeypatch.setattr(requests, "post", raise_error)

        candidates = [
            {
                "semantic_text": REKTOR_MAIL_TEXT,
                "confidence": 0.87,
                "source_event_id": "ctx-1",
                "occurred_at": "2026-07-04T06:00:00Z",
                "occurred_before_anomaly": True,
            }
        ]

        result = build_investigation_payload(ANOMALY_ENVELOPE, candidates)

        assert len(result["possible_causes"]) == 1
        assert result["possible_causes"][0]["explanation"] is None

    def test_low_confidence_candidate_triggers_fallback(self):
        """
        Der Unsicherheits-Fallback: ein Kandidat unterhalb des
        Schwellwerts wird NICHT als Erklaerung praesentiert.
        """
        candidates = [
            {
                "semantic_text": "irgendwas schwach Verwandtes",
                "confidence": 0.2,
                "source_event_id": "ctx-2",
                "occurred_at": "2026-07-04T06:00:00Z",
                "occurred_before_anomaly": True,
            }
        ]

        result = build_investigation_payload(ANOMALY_ENVELOPE, candidates)

        assert result["possible_causes"] == []
        assert "confidence threshold" in result["confidence_note"]

    def test_no_candidates_triggers_fallback(self):
        result = build_investigation_payload(ANOMALY_ENVELOPE, [])

        assert result["possible_causes"] == []

    def test_anomaly_summary_contains_metric_and_values(self):
        result = build_investigation_payload(ANOMALY_ENVELOPE, [])

        assert "math_test_average" in result["anomaly_summary"]
        assert "78" in result["anomaly_summary"]
        assert "45" in result["anomaly_summary"]


class TestCreateInvestigationEvent:
    def test_causation_id_points_to_anomaly_event(self):
        payload = build_investigation_payload(ANOMALY_ENVELOPE, [])
        event = create_investigation_event(ANOMALY_ENVELOPE, payload)

        assert event["causation_id"] == ANOMALY_ENVELOPE["event_id"]

    def test_project_id_is_passed_through(self):
        payload = build_investigation_payload(ANOMALY_ENVELOPE, [])
        event = create_investigation_event(ANOMALY_ENVELOPE, payload)

        assert event["project_id"] == ANOMALY_ENVELOPE["project_id"]

    def test_event_type_is_investigation_generated(self):
        payload = build_investigation_payload(ANOMALY_ENVELOPE, [])
        event = create_investigation_event(ANOMALY_ENVELOPE, payload)

        assert event["event_type"] == "InvestigationGenerated"


class TestHandleMessage:
    def test_full_school_scenario_finds_the_email(self, monkeypatch):
        """
        End-to-end durch handle_message mit dem echten Szenario: die
        Rektor-Mail (vor der Anomalie, hohe Similarity) wird als
        wahrscheinliche Ursache gefunden.
        """
        fake_response = MagicMock()
        fake_response.json.return_value = {"embedding": [0.1] * 768}
        fake_response.raise_for_status.return_value = None
        monkeypatch.setattr(requests, "post", lambda *a, **kw: fake_response)

        qdrant_client = MagicMock()
        qdrant_client.query_points.return_value = SimpleNamespace(
            points=[make_point(0.87, REKTOR_MAIL_TEXT, "2026-07-04T06:00:00Z")]
        )

        channel = FakeChannel()
        body = json.dumps(ANOMALY_ENVELOPE).encode("utf-8")

        handle_message(channel, make_method(), None, body, qdrant_client)

        assert channel.acked == [1]
        published = channel.published[0]
        assert published["exchange"] == OUTPUT_EXCHANGE
        assert len(published["body"]["payload"]["possible_causes"]) == 1
        assert published["body"]["payload"]["possible_causes"][0]["semantic_text"] == REKTOR_MAIL_TEXT

    def test_invalid_json_goes_to_dead_letter(self):
        channel = FakeChannel()
        qdrant_client = MagicMock()
        body = b"not valid json {{{"

        handle_message(channel, make_method(), None, body, qdrant_client)

        assert channel.published[0]["exchange"] == DEAD_LETTER_EXCHANGE
        assert channel.acked == [1]

    def test_ollama_error_is_not_acked(self, monkeypatch):
        def raise_error(*args, **kwargs):
            raise requests.ConnectionError("Ollama unreachable")

        monkeypatch.setattr(requests, "post", raise_error)

        channel = FakeChannel()
        qdrant_client = MagicMock()
        body = json.dumps(ANOMALY_ENVELOPE).encode("utf-8")

        handle_message(channel, make_method(), None, body, qdrant_client)

        assert channel.acked == []
        assert channel.published == []