"""
Tests für persistence-service.

Nutzt eine Fake-DB-Connection statt echtem Postgres, um insert_event()
und handle_message() isoliert zu testen.
"""

import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

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

import psycopg2  # noqa: E402

from main import DEAD_LETTER_EXCHANGE, handle_message, insert_event  # noqa: E402


class FakeCursor:
    """Zeichnet execute()-Aufrufe auf, simuliert rowcount."""

    def __init__(self, rowcount=1, raise_error=None):
        self.rowcount = rowcount
        self.raise_error = raise_error
        self.executed = []

    def execute(self, sql, params):
        if self.raise_error:
            raise self.raise_error
        self.executed.append({"sql": sql, "params": params})

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class FakeDBConnection:
    def __init__(self, rowcount=1, raise_error=None):
        self._cursor = FakeCursor(rowcount=rowcount, raise_error=raise_error)

    def cursor(self):
        return self._cursor


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
    "schema_version": "1.0",
    "event_id": "6f9c2b1a-4e3a-4a3a-9c1a-2b1a4e3a4a3a",
    "event_type": "ObservationIngested",
    "source": "json-adapter-service",
    "occurred_at": "2026-07-05T10:00:00Z",
    "payload": {"metric": "test_metric", "value": 42},
}


class TestInsertEvent:
    def test_new_row_returns_true(self):
        db = FakeDBConnection(rowcount=1)

        result = insert_event(db, VALID_ENVELOPE)

        assert result is True

    def test_duplicate_returns_false(self):
        """rowcount=0 simuliert ON CONFLICT DO NOTHING bei bestehender event_id."""
        db = FakeDBConnection(rowcount=0)

        result = insert_event(db, VALID_ENVELOPE)

        assert result is False

    def test_missing_optional_fields_default_to_none(self):
        db = FakeDBConnection(rowcount=1)
        envelope = dict(VALID_ENVELOPE)  # keine correlation_id/causation_id/project_id

        insert_event(db, envelope)

        params = db._cursor.executed[0]["params"]
        # Reihenfolge lt. INSERT_EVENT_SQL: id, event_id, schema_version,
        # event_type, source, occurred_at, correlation_id, causation_id,
        # project_id, payload
        assert params[6] is None  # correlation_id
        assert params[7] is None  # causation_id
        assert params[8] is None  # project_id

    def test_project_id_is_passed_through_when_present(self):
        db = FakeDBConnection(rowcount=1)
        envelope = dict(VALID_ENVELOPE)
        envelope["project_id"] = "1a2b3c4d-5e6f-4a3a-9c1a-2b1a4e3a4a3a"

        insert_event(db, envelope)

        params = db._cursor.executed[0]["params"]
        assert params[8] == "1a2b3c4d-5e6f-4a3a-9c1a-2b1a4e3a4a3a"

    def test_payload_is_serialized_as_json(self):
        db = FakeDBConnection(rowcount=1)

        insert_event(db, VALID_ENVELOPE)

        params = db._cursor.executed[0]["params"]
        assert json.loads(params[9]) == VALID_ENVELOPE["payload"]


class TestHandleMessageValidEnvelope:
    def test_valid_envelope_is_persisted_and_acked(self):
        channel = FakeChannel()
        db = FakeDBConnection(rowcount=1)
        body = json.dumps(VALID_ENVELOPE).encode("utf-8")

        handle_message(channel, make_method(), None, body, db)

        assert len(db._cursor.executed) == 1
        assert channel.acked == [1]
        assert channel.published == []  # kein Dead-Letter bei Erfolg

    def test_duplicate_envelope_is_still_acked(self):
        """Ein Duplikat ist kein Fehler - normal ack, kein Dead-Letter."""
        channel = FakeChannel()
        db = FakeDBConnection(rowcount=0)
        body = json.dumps(VALID_ENVELOPE).encode("utf-8")

        handle_message(channel, make_method(), None, body, db)

        assert channel.acked == [1]
        assert channel.published == []


class TestHandleMessageInvalidInput:
    def test_invalid_json_goes_to_dead_letter_and_is_acked(self):
        channel = FakeChannel()
        db = FakeDBConnection(rowcount=1)
        body = b"not valid json {{{"

        handle_message(channel, make_method(), None, body, db)

        assert len(channel.published) == 1
        assert channel.published[0]["exchange"] == DEAD_LETTER_EXCHANGE
        assert channel.acked == [1]
        assert len(db._cursor.executed) == 0  # nie versucht zu inserten

    def test_invalid_envelope_goes_to_dead_letter(self):
        channel = FakeChannel()
        db = FakeDBConnection(rowcount=1)
        broken = dict(VALID_ENVELOPE)
        del broken["event_type"]
        body = json.dumps(broken).encode("utf-8")

        handle_message(channel, make_method(), None, body, db)

        assert channel.published[0]["exchange"] == DEAD_LETTER_EXCHANGE
        assert channel.acked == [1]


class TestHandleMessageDatabaseError:
    def test_database_error_is_not_acked_and_not_dead_lettered(self):
        """
        Kernverhalten: bei einem DB-Fehler bleibt die Nachricht in der
        Queue (kein ack), damit RabbitMQ sie erneut zustellt. Kein
        Dead-Letter, weil das Envelope selbst gueltig war - das Problem
        liegt an der Datenbank, nicht an der Nachricht.
        """
        channel = FakeChannel()
        db = FakeDBConnection(raise_error=psycopg2.OperationalError("connection lost"))
        body = json.dumps(VALID_ENVELOPE).encode("utf-8")

        handle_message(channel, make_method(), None, body, db)

        assert channel.acked == []
        assert channel.published == []