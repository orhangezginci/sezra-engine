"""
Tests für api-service.

Nutzt FastAPI's TestClient plus Mocks fuer RabbitMQ/Postgres, damit keine
echten Verbindungen fuer die Logik-Tests noetig sind.
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
os.environ["POSTGRES_HOST"] = "localhost"
os.environ["POSTGRES_PORT"] = "5432"
os.environ["POSTGRES_USER"] = "test"
os.environ["POSTGRES_PASSWORD"] = "test"
os.environ["POSTGRES_DB"] = "test"
os.environ["SEZRA_PROJECT_ID"] = "1a2b3c4d-5e6f-4a3a-9c1a-2b1a4e3a4a3a"

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
from main import app, build_envelope  # noqa: E402


class TestBuildEnvelope:
    def test_observation_gets_correct_event_type(self):
        envelope = build_envelope({"metric": "x", "value": 1}, "observation")

        assert envelope["event_type"] == "ObservationIngested"

    def test_context_gets_correct_event_type(self):
        envelope = build_envelope({"sender": "a@b.de", "text": "..."}, "context")

        assert envelope["event_type"] == "ContextIngested"

    def test_source_type_is_set_from_endpoint_not_body(self):
        """
        source_type kommt vom Endpoint-Pfad, nicht aus dem Body - auch
        wenn der Client versehentlich ein widerspruechliches source_type
        mitschickt, gewinnt der Endpoint.
        """
        envelope = build_envelope({"source_type": "context", "value": 1}, "observation")

        assert envelope["payload"]["source_type"] == "observation"

    def test_project_id_is_set(self):
        envelope = build_envelope({"value": 1}, "observation")

        assert envelope["project_id"] == "1a2b3c4d-5e6f-4a3a-9c1a-2b1a4e3a4a3a"

    def test_raw_data_is_preserved_in_payload(self):
        envelope = build_envelope({"metric": "test", "value": 42}, "observation")

        assert envelope["payload"]["metric"] == "test"
        assert envelope["payload"]["value"] == 42

    def test_correlation_id_defaults_to_own_event_id(self):
        """
        Regressionstest, analog zu json-adapter-service: ohne diese
        Selbstreferenz bleibt correlation_id null, und keine nach-
        gelagerte Kette kann jemals bis zu diesem Ursprungsevent
        zurueckverfolgt werden.
        """
        envelope = build_envelope({"value": 1}, "observation")

        assert envelope["correlation_id"] == envelope["event_id"]


class TestPostEndpoints:
    def test_post_observation_publishes_and_returns_event_id(self, monkeypatch):
        fake_channel = MagicMock()
        fake_connection = MagicMock()
        fake_connection.channel.return_value = fake_channel
        monkeypatch.setattr(main, "connect_to_rabbitmq", lambda: fake_connection)

        client = TestClient(app)
        response = client.post("/observations", json={"metric": "test", "value": 42})

        assert response.status_code == 200
        assert response.json()["event_type"] == "ObservationIngested"
        fake_channel.basic_publish.assert_called_once()

    def test_post_context_publishes_and_returns_event_id(self, monkeypatch):
        fake_channel = MagicMock()
        fake_connection = MagicMock()
        fake_connection.channel.return_value = fake_channel
        monkeypatch.setattr(main, "connect_to_rabbitmq", lambda: fake_connection)

        client = TestClient(app)
        response = client.post("/context", json={"sender": "a@b.de", "text": "..."})

        assert response.status_code == 200
        assert response.json()["event_type"] == "ContextIngested"

    def test_published_envelope_is_schema_valid(self, monkeypatch):
        captured = {}

        def fake_publish(exchange, routing_key, body, properties=None):
            captured["envelope"] = json.loads(body)

        fake_channel = MagicMock()
        fake_channel.basic_publish.side_effect = fake_publish
        fake_connection = MagicMock()
        fake_connection.channel.return_value = fake_channel
        monkeypatch.setattr(main, "connect_to_rabbitmq", lambda: fake_connection)

        client = TestClient(app)
        client.post("/observations", json={"metric": "test", "value": 42})

        assert captured["envelope"]["event_type"] == "ObservationIngested"
        assert captured["envelope"]["payload"]["metric"] == "test"

    def test_rabbitmq_connection_is_closed_after_publish(self, monkeypatch):
        fake_channel = MagicMock()
        fake_connection = MagicMock()
        fake_connection.channel.return_value = fake_channel
        monkeypatch.setattr(main, "connect_to_rabbitmq", lambda: fake_connection)

        client = TestClient(app)
        client.post("/observations", json={"value": 1})

        fake_connection.close.assert_called_once()


class TestGetEndpoints:
    def test_get_investigations_returns_rows(self, monkeypatch):
        fake_cursor = MagicMock()
        fake_cursor.fetchall.return_value = [
            {
                "event_id": "abc",
                "causation_id": "anomaly-1",
                "occurred_at": "2026-01-01T00:00:00Z",
                "received_at": "2026-01-01T00:00:01Z",
                "payload": {"anomaly_summary": "x"},
            }
        ]
        fake_cursor.__enter__ = lambda self: fake_cursor
        fake_cursor.__exit__ = lambda self, *a: None

        fake_connection = MagicMock()
        fake_connection.cursor.return_value = fake_cursor
        monkeypatch.setattr(main, "connect_to_postgres", lambda: fake_connection)

        client = TestClient(app)
        response = client.get("/investigations")

        assert response.status_code == 200

    def test_get_investigations_prioritizes_results_with_own_causes(self, monkeypatch):
        """
        Regressionstest: Investigations mit gefundenen Ursachen muessen
        VOR "keine Ursache gefunden"-Ergebnissen stehen, sonst muss der
        Nutzer (oder Studio Light) zwischen mehreren Eintraegen suchen,
        um das eigentlich interessante Ergebnis zu finden.
        """
        fake_cursor = MagicMock()
        fake_cursor.fetchall.return_value = [
            {
                "event_id": "no-cause-investigation",
                "causation_id": "anomaly-a",
                "received_at": "2026-07-17T10:00:00Z",
                "payload": {"possible_causes": []},
            },
            {
                "event_id": "has-cause-investigation",
                "causation_id": "anomaly-b",
                "received_at": "2026-07-17T09:00:00Z",  # aelter, muesste trotzdem vorne stehen
                "payload": {"possible_causes": [{"source_event_id": "some-context-event"}]},
            },
        ]
        fake_cursor.__enter__ = lambda self: fake_cursor
        fake_cursor.__exit__ = lambda self, *a: None

        fake_connection = MagicMock()
        fake_connection.cursor.return_value = fake_cursor
        monkeypatch.setattr(main, "connect_to_postgres", lambda: fake_connection)

        client = TestClient(app)
        response = client.get("/investigations")

        results = response.json()
        assert results[0]["event_id"] == "has-cause-investigation"
        assert results[1]["event_id"] == "no-cause-investigation"

    def test_get_investigations_deprioritizes_results_explained_elsewhere(self, monkeypatch):
        """
        Der Kern des Kreuzverweis-Fixes: eine Anomalie ohne eigene
        Ursache, die aber SELBST als Ursache einer anderen Investigation
        gefunden wurde (z. B. checkout_error_rate als Ursache fuer
        conversion_rate), soll nicht gleichrangig mit einem echten,
        ungeloesten Raetsel erscheinen - sie ist ja bereits erklaert,
        nur als Erklaerung fuer etwas anderes.
        """
        fake_cursor = MagicMock()
        fake_cursor.fetchall.return_value = [
            {
                "event_id": "genuine-mystery",
                "causation_id": "anomaly-x",
                "received_at": "2026-07-17T08:00:00Z",
                "payload": {"possible_causes": []},
            },
            {
                "event_id": "explained-elsewhere-investigation",
                "causation_id": "checkout-error-anomaly-event-id",
                "received_at": "2026-07-17T09:00:00Z",  # neuer, muesste trotzdem hinten stehen
                "payload": {"possible_causes": []},
            },
            {
                "event_id": "conversion-rate-investigation",
                "causation_id": "conversion-rate-anomaly-event-id",
                "received_at": "2026-07-17T10:00:00Z",
                "payload": {
                    "possible_causes": [
                        {"source_event_id": "checkout-error-anomaly-event-id"}
                    ]
                },
            },
        ]
        fake_cursor.__enter__ = lambda self: fake_cursor
        fake_cursor.__exit__ = lambda self, *a: None

        fake_connection = MagicMock()
        fake_connection.cursor.return_value = fake_cursor
        monkeypatch.setattr(main, "connect_to_postgres", lambda: fake_connection)

        client = TestClient(app)
        response = client.get("/investigations")

        results = response.json()
        ids_in_order = [r["event_id"] for r in results]

        assert ids_in_order[0] == "conversion-rate-investigation"  # eigene Ursache
        assert ids_in_order[1] == "genuine-mystery"  # echtes Raetsel
        assert ids_in_order[2] == "explained-elsewhere-investigation"  # anderswo erklaert

    def test_explained_elsewhere_investigation_is_marked(self, monkeypatch):
        fake_cursor = MagicMock()
        fake_cursor.fetchall.return_value = [
            {
                "event_id": "explained-elsewhere-investigation",
                "causation_id": "checkout-error-anomaly-event-id",
                "received_at": "2026-07-17T09:00:00Z",
                "payload": {"possible_causes": []},
            },
            {
                "event_id": "conversion-rate-investigation",
                "causation_id": "conversion-rate-anomaly-event-id",
                "received_at": "2026-07-17T10:00:00Z",
                "payload": {
                    "possible_causes": [
                        {"source_event_id": "checkout-error-anomaly-event-id"}
                    ]
                },
            },
        ]
        fake_cursor.__enter__ = lambda self: fake_cursor
        fake_cursor.__exit__ = lambda self, *a: None

        fake_connection = MagicMock()
        fake_connection.cursor.return_value = fake_cursor
        monkeypatch.setattr(main, "connect_to_postgres", lambda: fake_connection)

        client = TestClient(app)
        results = client.get("/investigations").json()

        explained = next(r for r in results if r["event_id"] == "explained-elsewhere-investigation")
        assert explained["explained_elsewhere"] is True
        assert explained["explained_by_investigation_event_id"] == "conversion-rate-investigation"

        not_explained = next(r for r in results if r["event_id"] == "conversion-rate-investigation")
        assert not_explained["explained_elsewhere"] is False
        assert not_explained["explained_by_investigation_event_id"] is None

    def test_get_investigation_by_id_not_found_returns_404(self, monkeypatch):
        fake_cursor = MagicMock()
        fake_cursor.fetchone.return_value = None
        fake_cursor.__enter__ = lambda self: fake_cursor
        fake_cursor.__exit__ = lambda self, *a: None

        fake_connection = MagicMock()
        fake_connection.cursor.return_value = fake_cursor
        monkeypatch.setattr(main, "connect_to_postgres", lambda: fake_connection)

        client = TestClient(app)
        response = client.get("/investigations/does-not-exist")

        assert response.status_code == 404

    def test_get_events_accepts_event_type_filter(self, monkeypatch):
        fake_cursor = MagicMock()
        fake_cursor.fetchall.return_value = []
        fake_cursor.__enter__ = lambda self: fake_cursor
        fake_cursor.__exit__ = lambda self, *a: None

        fake_connection = MagicMock()
        fake_connection.cursor.return_value = fake_cursor
        monkeypatch.setattr(main, "connect_to_postgres", lambda: fake_connection)

        client = TestClient(app)
        response = client.get("/events?event_type=AnomalyDetected")

        assert response.status_code == 200

    def test_get_events_accepts_correlation_id_filter(self, monkeypatch):
        """
        correlation_id erlaubt, die komplette Kette eines Vorfalls
        abzurufen (Beobachtung -> Anomalie -> Investigation) - genutzt
        sowohl von Studio Light als auch potenziell von der API direkt.
        """
        fake_cursor = MagicMock()
        fake_cursor.fetchall.return_value = []
        fake_cursor.__enter__ = lambda self: fake_cursor
        fake_cursor.__exit__ = lambda self, *a: None

        fake_connection = MagicMock()
        fake_connection.cursor.return_value = fake_cursor
        monkeypatch.setattr(main, "connect_to_postgres", lambda: fake_connection)

        client = TestClient(app)
        response = client.get("/events?correlation_id=some-correlation-id")

        assert response.status_code == 200
        call_args = fake_cursor.execute.call_args
        assert "correlation_id = %s" in call_args[0][0]
        assert "some-correlation-id" in call_args[0][1]

    def test_get_events_combines_event_type_and_correlation_id_filters(self, monkeypatch):
        fake_cursor = MagicMock()
        fake_cursor.fetchall.return_value = []
        fake_cursor.__enter__ = lambda self: fake_cursor
        fake_cursor.__exit__ = lambda self, *a: None

        fake_connection = MagicMock()
        fake_connection.cursor.return_value = fake_cursor
        monkeypatch.setattr(main, "connect_to_postgres", lambda: fake_connection)

        client = TestClient(app)
        response = client.get(
            "/events?event_type=AnomalyDetected&correlation_id=some-correlation-id"
        )

        assert response.status_code == 200
        call_args = fake_cursor.execute.call_args
        query = call_args[0][0]
        assert "event_type = %s" in query
        assert "correlation_id = %s" in query
        assert "AND" in query

    def test_get_investigations_includes_correlation_id(self, monkeypatch):
        fake_cursor = MagicMock()
        fake_cursor.fetchall.return_value = [
            {
                "event_id": "abc",
                "correlation_id": "corr-1",
                "causation_id": "anomaly-1",
                "occurred_at": "2026-01-01T00:00:00Z",
                "received_at": "2026-01-01T00:00:01Z",
                "payload": {"anomaly_summary": "x"},
            }
        ]
        fake_cursor.__enter__ = lambda self: fake_cursor
        fake_cursor.__exit__ = lambda self, *a: None

        fake_connection = MagicMock()
        fake_connection.cursor.return_value = fake_cursor
        monkeypatch.setattr(main, "connect_to_postgres", lambda: fake_connection)

        client = TestClient(app)
        response = client.get("/investigations")

        assert response.json()[0]["correlation_id"] == "corr-1"


class TestHealth:
    def test_health_endpoint_returns_ok(self):
        client = TestClient(app)
        response = client.get("/health")

        assert response.status_code == 200
        assert response.json()["status"] == "ok"