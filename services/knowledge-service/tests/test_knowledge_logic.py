"""
Tests für knowledge-service.

Prüft build_semantic_text, create_enriched_event und handle_message
mit einem Fake-Channel, ohne echte RabbitMQ-Verbindung.
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

from main import (  # noqa: E402
    DEAD_LETTER_EXCHANGE,
    OUTPUT_EXCHANGE,
    build_semantic_text,
    create_enriched_event,
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


VALID_ENVELOPE = {
    "schema_version": "1.0",
    "event_id": "6f9c2b1a-4e3a-4a3a-9c1a-2b1a4e3a4a3a",
    "event_type": "ContextIngested",
    "source": "json-adapter-service",
    "occurred_at": "2026-07-05T10:00:00Z",
    "payload": {"metric": "math_test_average", "value": 78, "source_type": "observation"},
}


class TestBuildSemanticText:
    def test_includes_all_payload_fields(self):
        text = build_semantic_text({"metric": "test", "value": 78})

        assert "metric: test" in text
        assert "value: 78" in text

    def test_underscores_become_spaces_in_field_names(self):
        text = build_semantic_text({"grade_level": 8})

        assert "grade level: 8" in text

    def test_empty_payload_produces_empty_string(self):
        assert build_semantic_text({}) == ""

    def test_excludes_technical_pipeline_fields(self):
        """
        source_event_id (UUID), composite_key etc. sind Pipeline-
        Metadaten, kein fachlicher Inhalt - wuerden den Einbettungstext
        mit bedeutungslosem Rauschen verunreinigen (relevant vor allem
        fuer AnomalyDetected-Payloads, die diese Felder fuehren).
        """
        text = build_semantic_text(
            {
                "metric": "checkout_error_rate",
                "composite_key": "checkout_error_rate",
                "source_event_id": "6f9c2b1a-4e3a-4a3a-9c1a-2b1a4e3a4a3a",
                "source_event_type": "ObservationIngested",
                "source_occurred_at": "2026-07-14T22:45:18Z",
            }
        )

        assert "metric: checkout error rate" in text
        assert "composite_key" not in text
        assert "source_event_id" not in text
        assert "source_event_type" not in text
        assert "source_occurred_at" not in text

    def test_works_identically_across_different_domains(self):
        """
        Kein 'if metric_name == ...': dieselbe Funktion muss fuer
        strukturell unterschiedliche Domaenen (School/Healthcare/
        Manufacturing) funktionieren, ohne Sonderfaelle.
        """
        school = build_semantic_text({"grade_level": 8, "metric": "math_average"})
        healthcare = build_semantic_text({"sender": "family@example.com", "subject": "complaint"})

        assert "grade level: 8" in school
        assert "sender: family@example.com" in healthcare


class TestCreateEnrichedEvent:
    def test_original_payload_fields_are_preserved(self):
        enriched = create_enriched_event(VALID_ENVELOPE, "some semantic text")

        assert enriched["payload"]["metric"] == "math_test_average"
        assert enriched["payload"]["value"] == 78

    def test_semantic_text_is_added(self):
        enriched = create_enriched_event(VALID_ENVELOPE, "some semantic text")

        assert enriched["payload"]["semantic_text"] == "some semantic text"

    def test_causation_id_points_to_original_event(self):
        enriched = create_enriched_event(VALID_ENVELOPE, "text")

        assert enriched["causation_id"] == VALID_ENVELOPE["event_id"]

    def test_correlation_id_defaults_to_original_event_id_when_absent(self):
        enriched = create_enriched_event(VALID_ENVELOPE, "text")

        assert enriched["correlation_id"] == VALID_ENVELOPE["event_id"]

    def test_existing_correlation_id_is_preserved(self):
        envelope = dict(VALID_ENVELOPE)
        envelope["correlation_id"] = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

        enriched = create_enriched_event(envelope, "text")

        assert enriched["correlation_id"] == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    def test_project_id_is_passed_through(self):
        """
        Regressionstest: create_enriched_event baut ein komplett neues
        Envelope-Dict statt das Original zu erweitern - project_id wurde
        dabei urspruenglich schlicht vergessen (gefunden beim ersten
        vollstaendigen End-to-End-Test, project_id kam als null in
        Qdrant an, obwohl json-adapter-service es korrekt gesetzt hatte).
        """
        envelope = dict(VALID_ENVELOPE)
        envelope["project_id"] = "1a2b3c4d-5e6f-4a3a-9c1a-2b1a4e3a4a3a"

        enriched = create_enriched_event(envelope, "text")

        assert enriched["project_id"] == "1a2b3c4d-5e6f-4a3a-9c1a-2b1a4e3a4a3a"

    def test_missing_project_id_defaults_to_none(self):
        enriched = create_enriched_event(VALID_ENVELOPE, "text")

        assert enriched["project_id"] is None

    def test_source_occurred_at_carries_original_timestamp(self):
        """
        occurred_at im Envelope selbst bleibt knowledge-service's eigene
        Erzeugungszeit (schema-konform) - der urspruengliche Zeitpunkt
        wandert stattdessen als source_occurred_at ins Payload, damit
        der spaetere Analyzer zeitliche Kausalitaet pruefen kann.
        """
        enriched = create_enriched_event(VALID_ENVELOPE, "text")

        assert enriched["payload"]["source_occurred_at"] == VALID_ENVELOPE["occurred_at"]
        assert enriched["occurred_at"] != VALID_ENVELOPE["occurred_at"]

    def test_event_type_is_semantic_enrichment_generated(self):
        enriched = create_enriched_event(VALID_ENVELOPE, "text")

        assert enriched["event_type"] == "SemanticEnrichmentGenerated"


class TestHandleMessage:
    def test_observation_ingested_is_skipped_without_enrichment(self):
        """
        Regressionstest fuer den Kern-Fix: eine normale Beobachtung wird
        NICHT angereichert/vektorisiert - sie beschreibt nur einen
        Normalzustand und kann keine Anomalie ausloesen. Ohne diesen
        Filter fluteten Baseline-Werte den Analyzer-Kandidatenpool mit
        bedeutungslosen, aber zeitlich zufaellig passenden "Ursachen"
        (gefunden im E-Commerce-Szenario: conversion_rate-Baseline-Werte
        erschienen faelschlich als Ursache fuer den checkout_error_rate-
        Spike, nur weil sie zeitlich davor lagen).
        """
        channel = FakeChannel()
        envelope = dict(VALID_ENVELOPE)
        envelope["event_type"] = "ObservationIngested"
        body = json.dumps(envelope).encode("utf-8")

        handle_message(channel, make_method(), None, body)

        assert channel.published == []
        assert channel.acked == [1]

    def test_anomaly_detected_is_enriched(self):
        channel = FakeChannel()
        envelope = dict(VALID_ENVELOPE)
        envelope["event_type"] = "AnomalyDetected"
        body = json.dumps(envelope).encode("utf-8")

        handle_message(channel, make_method(), None, body)

        assert len(channel.published) == 1
        assert channel.published[0]["exchange"] == OUTPUT_EXCHANGE

    def test_context_ingested_is_enriched(self):
        channel = FakeChannel()
        body = json.dumps(VALID_ENVELOPE).encode("utf-8")  # event_type bereits ContextIngested

        handle_message(channel, make_method(), None, body)

        assert len(channel.published) == 1

    def test_valid_envelope_is_published_to_enriched_exchange(self):
        channel = FakeChannel()
        body = json.dumps(VALID_ENVELOPE).encode("utf-8")

        handle_message(channel, make_method(), None, body)

        assert len(channel.published) == 1
        assert channel.published[0]["exchange"] == OUTPUT_EXCHANGE

    def test_published_envelope_contains_semantic_text(self):
        channel = FakeChannel()
        body = json.dumps(VALID_ENVELOPE).encode("utf-8")

        handle_message(channel, make_method(), None, body)

        assert "semantic_text" in channel.published[0]["body"]["payload"]

    def test_valid_envelope_is_acked(self):
        channel = FakeChannel()
        body = json.dumps(VALID_ENVELOPE).encode("utf-8")

        handle_message(channel, make_method(), None, body)

        assert channel.acked == [1]

    def test_invalid_json_goes_to_dead_letter(self):
        channel = FakeChannel()
        body = b"not valid json {{{"

        handle_message(channel, make_method(), None, body)

        assert channel.published[0]["exchange"] == DEAD_LETTER_EXCHANGE
        assert channel.acked == [1]

    def test_invalid_envelope_goes_to_dead_letter(self):
        channel = FakeChannel()
        broken = dict(VALID_ENVELOPE)
        del broken["event_type"]
        body = json.dumps(broken).encode("utf-8")

        handle_message(channel, make_method(), None, body)

        assert channel.published[0]["exchange"] == DEAD_LETTER_EXCHANGE
        assert channel.acked == [1]