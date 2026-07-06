"""
Tests für ingestion-service.

Nutzt einen Fake-Channel statt einer echten RabbitMQ-Verbindung, um
handle_message() isoliert zu testen: validiert korrekt, republished
gültige Envelopes, dead-lettert ungültige.
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

from main import DEAD_LETTER_EXCHANGE, OUTPUT_EXCHANGE, handle_message  # noqa: E402


class FakeChannel:
    """Zeichnet basic_publish/basic_ack-Aufrufe auf, ohne echtes RabbitMQ."""

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


class TestHandleMessageValidEnvelope:
    def test_valid_envelope_is_republished_to_output_exchange(self):
        channel = FakeChannel()
        body = json.dumps(VALID_ENVELOPE).encode("utf-8")

        handle_message(channel, make_method(), None, body)

        assert len(channel.published) == 1
        assert channel.published[0]["exchange"] == OUTPUT_EXCHANGE

    def test_valid_envelope_content_is_preserved_unchanged(self):
        channel = FakeChannel()
        body = json.dumps(VALID_ENVELOPE).encode("utf-8")

        handle_message(channel, make_method(), None, body)

        assert channel.published[0]["body"] == VALID_ENVELOPE

    def test_valid_envelope_is_acked(self):
        channel = FakeChannel()
        body = json.dumps(VALID_ENVELOPE).encode("utf-8")

        handle_message(channel, make_method(), None, body)

        assert channel.acked == [1]


class TestHandleMessageInvalidInput:
    def test_invalid_json_goes_to_dead_letter(self):
        channel = FakeChannel()
        body = b"not valid json {{{"

        handle_message(channel, make_method(), None, body)

        assert len(channel.published) == 1
        assert channel.published[0]["exchange"] == DEAD_LETTER_EXCHANGE
        assert channel.published[0]["body"]["payload"]["failure_class"] == "permanent"

    def test_envelope_missing_required_field_goes_to_dead_letter(self):
        channel = FakeChannel()
        broken_envelope = dict(VALID_ENVELOPE)
        del broken_envelope["event_type"]
        body = json.dumps(broken_envelope).encode("utf-8")

        handle_message(channel, make_method(), None, body)

        assert len(channel.published) == 1
        assert channel.published[0]["exchange"] == DEAD_LETTER_EXCHANGE

    def test_invalid_input_is_still_acked(self):
        """
        Kein Requeue-Loop: fehlerhafte Nachrichten werden bestaetigt
        (nicht endlos wiederholt), nachdem sie dead-lettert wurden.
        """
        channel = FakeChannel()
        body = b"not valid json"

        handle_message(channel, make_method(), None, body)

        assert channel.acked == [1]