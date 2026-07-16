"""
Tests für context-severity-detector-service.

Nutzt die beiden konkreten Beispiele aus der Diskussion: "Login nicht
moeglich" (hohe Dringlichkeit, sofortiger Alarm) vs. "Seitenaufbau
teilweise langsam" (niedrige Dringlichkeit, kein sofortiger Alarm -
waere Aufgabe eines spaeteren Volumen-Detectors).
"""

import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parents[1]))

os.environ["RABBITMQ_HOST"] = "localhost"
os.environ["RABBITMQ_PORT"] = "5672"
os.environ["RABBITMQ_USER"] = "test"
os.environ["RABBITMQ_PASSWORD"] = "test"
os.environ["LLM_PROVIDER"] = "ollama"
os.environ["OLLAMA_HOST"] = "localhost"
os.environ["OLLAMA_PORT"] = "11434"
os.environ["OLLAMA_GENERATION_MODEL"] = "qwen2.5:3b"
os.environ["SEVERITY_THRESHOLD"] = "0.8"

import pytest  # noqa: E402
import requests  # noqa: E402

import main  # noqa: E402
from main import (  # noqa: E402
    DEAD_LETTER_EXCHANGE,
    OUTPUT_EXCHANGE,
    _parse_score,
    build_context_text,
    create_severity_anomaly_event,
    get_severity_score,
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
    from types import SimpleNamespace
    return SimpleNamespace(delivery_tag=1)


def make_context_envelope(text: str) -> dict:
    return {
        "schema_version": "1.1",
        "event_id": "6f9c2b1a-4e3a-4a3a-9c1a-2b1a4e3a4a3a",
        "event_type": "ContextIngested",
        "source": "api-service",
        "occurred_at": "2026-07-15T10:00:00Z",
        "correlation_id": "6f9c2b1a-4e3a-4a3a-9c1a-2b1a4e3a4a3a",
        "project_id": "1a2b3c4d-5e6f-4a3a-9c1a-2b1a4e3a4a3a",
        "payload": {"sender": "user@example.com", "text": text, "source_type": "context"},
    }


class TestParseScore:
    def test_parses_clean_float(self):
        assert _parse_score("0.9") == 0.9

    def test_strips_trailing_punctuation(self):
        assert _parse_score("0.9.") == 0.9

    def test_takes_first_token_if_model_adds_extra_words(self):
        assert _parse_score("0.9 (kritisch)") == 0.9

    def test_clamps_above_one(self):
        assert _parse_score("1.5") == 1.0

    def test_clamps_below_zero(self):
        assert _parse_score("-0.2") == 0.0


class TestBuildContextText:
    def test_includes_message_fields(self):
        text = build_context_text({"sender": "a@b.de", "text": "Login nicht moeglich"})

        assert "sender: a@b.de" in text
        assert "Login nicht moeglich" in text

    def test_excludes_source_type(self):
        text = build_context_text({"text": "x", "source_type": "context"})

        assert "source_type" not in text


class TestGetSeverityScore:
    def test_login_failure_scores_high(self, monkeypatch):
        """
        Das konkrete Beispiel aus der Diskussion: "Login nicht moeglich"
        soll hoch bewertet werden - simuliert hier direkt die LLM-Antwort,
        da wir das Modell selbst nicht in Unit-Tests aufrufen.
        """
        fake_response = MagicMock()
        fake_response.json.return_value = {"response": "0.95"}
        fake_response.raise_for_status.return_value = None
        monkeypatch.setattr(requests, "post", lambda *a, **kw: fake_response)

        score = get_severity_score("Login nicht moeglich, Weiterleitung auf Fehlerseite")

        assert score == 0.95

    def test_minor_complaint_scores_low(self, monkeypatch):
        fake_response = MagicMock()
        fake_response.json.return_value = {"response": "0.3"}
        fake_response.raise_for_status.return_value = None
        monkeypatch.setattr(requests, "post", lambda *a, **kw: fake_response)

        score = get_severity_score("Seitenaufbau teilweise langsam")

        assert score == 0.3

    def test_uses_think_false_for_ollama(self, monkeypatch):
        """
        Regressionstest: dieselbe Thinking-Budget-Falle wie bei
        analyzer-service (qwen3/gemini) - ohne think:false koennte ein
        Hybrid-Reasoning-Modell sein Antwort-Budget vor der eigentlichen
        Zahl aufbrauchen.
        """
        captured = {}

        def fake_post(url, json=None, timeout=None, headers=None, params=None):
            captured["think"] = json.get("think")
            response = MagicMock()
            response.json.return_value = {"response": "0.5"}
            response.raise_for_status.return_value = None
            return response

        monkeypatch.setattr(requests, "post", fake_post)

        get_severity_score("some text")

        assert captured["think"] is False


class TestCreateSeverityAnomalyEvent:
    def test_anomaly_type_is_severity(self):
        envelope = make_context_envelope("Login nicht moeglich")
        event = create_severity_anomaly_event(envelope, "Login nicht moeglich", 0.95)

        assert event["payload"]["anomaly_type"] == "severity"

    def test_correlation_id_and_causation_id_propagated(self):
        envelope = make_context_envelope("Login nicht moeglich")
        event = create_severity_anomaly_event(envelope, "Login nicht moeglich", 0.95)

        assert event["correlation_id"] == envelope["correlation_id"]
        assert event["causation_id"] == envelope["event_id"]

    def test_project_id_passed_through(self):
        envelope = make_context_envelope("Login nicht moeglich")
        event = create_severity_anomaly_event(envelope, "Login nicht moeglich", 0.95)

        assert event["project_id"] == envelope["project_id"]


class TestHandleMessage:
    def test_high_severity_publishes_anomaly(self, monkeypatch):
        fake_response = MagicMock()
        fake_response.json.return_value = {"response": "0.95"}
        fake_response.raise_for_status.return_value = None
        monkeypatch.setattr(requests, "post", lambda *a, **kw: fake_response)

        channel = FakeChannel()
        envelope = make_context_envelope("Login nicht moeglich, Weiterleitung auf Fehlerseite")
        body = json.dumps(envelope).encode("utf-8")

        handle_message(channel, make_method(), None, body)

        assert len(channel.published) == 1
        assert channel.published[0]["exchange"] == OUTPUT_EXCHANGE
        assert channel.published[0]["body"]["payload"]["severity_score"] == 0.95
        assert channel.acked == [1]

    def test_low_severity_does_not_publish(self, monkeypatch):
        fake_response = MagicMock()
        fake_response.json.return_value = {"response": "0.3"}
        fake_response.raise_for_status.return_value = None
        monkeypatch.setattr(requests, "post", lambda *a, **kw: fake_response)

        channel = FakeChannel()
        envelope = make_context_envelope("Seitenaufbau teilweise langsam")
        body = json.dumps(envelope).encode("utf-8")

        handle_message(channel, make_method(), None, body)

        assert channel.published == []
        assert channel.acked == [1]  # trotzdem acken, kein Fehler

    def test_non_context_events_are_ignored(self):
        channel = FakeChannel()
        envelope = {
            "schema_version": "1.1",
            "event_id": "6f9c2b1a-4e3a-4a3a-9c1a-2b1a4e3a4a3a",
            "event_type": "ObservationIngested",
            "source": "json-adapter-service",
            "occurred_at": "2026-07-15T10:00:00Z",
            "payload": {"metric": "x", "value": 1},
        }
        body = json.dumps(envelope).encode("utf-8")

        handle_message(channel, make_method(), None, body)

        assert channel.published == []
        assert channel.acked == [1]

    def test_invalid_json_goes_to_dead_letter(self):
        channel = FakeChannel()
        body = b"not valid json {{{"

        handle_message(channel, make_method(), None, body)

        assert channel.published[0]["exchange"] == DEAD_LETTER_EXCHANGE
        assert channel.acked == [1]

    def test_llm_error_is_not_acked(self, monkeypatch):
        def raise_error(*args, **kwargs):
            raise requests.ConnectionError("LLM unreachable")

        monkeypatch.setattr(requests, "post", raise_error)

        channel = FakeChannel()
        envelope = make_context_envelope("Login nicht moeglich")
        body = json.dumps(envelope).encode("utf-8")

        handle_message(channel, make_method(), None, body)

        assert channel.acked == []
        assert channel.published == []

    def test_unparseable_score_is_not_acked(self, monkeypatch):
        fake_response = MagicMock()
        fake_response.json.return_value = {"response": "keine Ahnung"}
        fake_response.raise_for_status.return_value = None
        monkeypatch.setattr(requests, "post", lambda *a, **kw: fake_response)

        channel = FakeChannel()
        envelope = make_context_envelope("Login nicht moeglich")
        body = json.dumps(envelope).encode("utf-8")

        handle_message(channel, make_method(), None, body)

        assert channel.acked == []
        assert channel.published == []