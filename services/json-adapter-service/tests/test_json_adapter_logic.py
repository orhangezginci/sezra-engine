"""
Tests für die json-adapter-service-spezifische Logik.

Prüft event_type-Ableitung und Envelope-Erzeugung, ohne tatsächliche
RabbitMQ-Verbindung - main() baut die Connection erst beim Start auf,
nicht beim Import der hier getesteten Funktionen.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

# main.py ruft beim Import required_env() fuer die RabbitMQ-Variablen auf.
# Fuer isolierte Logik-Tests setzen wir Platzhalter, damit der Import
# nicht crasht - eine echte Verbindung wird hier nie aufgebaut.
#
# Bewusst erzwungen (nicht setdefault): docker-compose kann die Variablen
# bereits als LEEREN String gesetzt haben (z. B. ${RABBITMQ_USER} ohne
# vorhandene .env-Datei) - das zaehlt fuer os.environ nicht als "fehlt",
# setdefault wuerde den leeren String also stehen lassen.
os.environ["RABBITMQ_HOST"] = "localhost"
os.environ["RABBITMQ_PORT"] = "5672"
os.environ["RABBITMQ_USER"] = "test"
os.environ["RABBITMQ_PASSWORD"] = "test"
os.environ["SEZRA_PROJECT_ID"] = "1a2b3c4d-5e6f-4a3a-9c1a-2b1a4e3a4a3a"

from main import build_envelope, derive_event_type  # noqa: E402


class TestDeriveEventType:
    def test_observation_maps_to_observation_ingested(self):
        assert derive_event_type({"source_type": "observation"}) == "ObservationIngested"

    def test_context_maps_to_context_ingested(self):
        assert derive_event_type({"source_type": "context"}) == "ContextIngested"

    def test_unknown_source_type_falls_back(self):
        assert derive_event_type({"source_type": "something_new"}) == "RawDataIngested"

    def test_missing_source_type_falls_back(self):
        assert derive_event_type({}) == "RawDataIngested"


class TestBuildEnvelope:
    def test_envelope_contains_required_fields(self):
        raw = {"source_type": "observation", "metric": "math_test_average", "value": 78}
        envelope = build_envelope(raw)

        assert envelope["schema_version"] == "1.1"
        assert envelope["event_type"] == "ObservationIngested"
        assert envelope["source"] == "json-adapter-service"
        assert "event_id" in envelope
        assert "occurred_at" in envelope

    def test_envelope_contains_project_id_from_environment(self):
        raw = {"source_type": "observation", "value": 1}
        envelope = build_envelope(raw)

        assert envelope["project_id"] == "1a2b3c4d-5e6f-4a3a-9c1a-2b1a4e3a4a3a"

    def test_raw_data_is_preserved_unchanged_in_payload(self):
        raw = {"source_type": "context", "sender": "principal@school.org", "text": "..."}
        envelope = build_envelope(raw)

        assert envelope["payload"] == raw

    def test_each_call_produces_a_unique_event_id(self):
        raw = {"source_type": "observation", "value": 1}
        first = build_envelope(raw)
        second = build_envelope(raw)

        assert first["event_id"] != second["event_id"]