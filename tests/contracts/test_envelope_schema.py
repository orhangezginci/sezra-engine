"""
Tests für den Event-Envelope-Vertrag (Brick 1).

Diese Tests validieren nicht Business-Logik, sondern ausschließlich den
Envelope-Vertrag selbst: welche Events sind gültig, welche nicht, und warum.
"""

import json
from copy import deepcopy
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker, ValidationError

SCHEMA_PATH = Path(__file__).parents[2] / "contracts" / "envelope.schema.json"


@pytest.fixture(scope="module")
def schema() -> dict:
    with open(SCHEMA_PATH, "r") as file:
        return json.load(file)


@pytest.fixture(scope="module")
def validator(schema: dict) -> Draft202012Validator:
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


@pytest.fixture
def minimal_valid_event() -> dict:
    """Nur die Pflichtfelder, sonst nichts."""
    return {
        "schema_version": "1.0",
        "event_id": "6f9c2b1a-4e3a-4a3a-9c1a-2b1a4e3a4a3a",
        "event_type": "AnomalyDetected",
        "source": "deviation-detector-service",
        "occurred_at": "2026-07-03T10:15:00Z",
        "payload": {},
    }


@pytest.fixture
def full_valid_event(minimal_valid_event: dict) -> dict:
    """Alle Felder inklusive Optionalen, plus payload-Konventionsfelder."""
    event = deepcopy(minimal_valid_event)
    event.update(
        {
            "correlation_id": "1a2b3c4d-5e6f-4a3a-9c1a-2b1a4e3a4a3a",
            "causation_id": "9f8e7d6c-5b4a-4a3a-9c1a-2b1a4e3a4a3a",
        }
    )
    event["payload"] = {
        "metric": "math_test_average",
        "confidence": 0.82,
        "measured_at": "2026-07-01T08:00:00Z",
    }
    return event


class TestValidEvents:
    def test_minimal_event_is_valid(self, validator, minimal_valid_event):
        validator.validate(minimal_valid_event)

    def test_full_event_is_valid(self, validator, full_valid_event):
        validator.validate(full_valid_event)

    def test_null_correlation_and_causation_are_valid(
        self, validator, minimal_valid_event
    ):
        event = deepcopy(minimal_valid_event)
        event["correlation_id"] = None
        event["causation_id"] = None
        validator.validate(event)

    def test_arbitrary_payload_content_is_valid(self, validator, minimal_valid_event):
        event = deepcopy(minimal_valid_event)
        event["payload"] = {
            "anything": "goes",
            "nested": {"deeply": {"too": True}},
            "numbers": [1, 2, 3],
        }
        validator.validate(event)


class TestMissingRequiredFields:
    @pytest.mark.parametrize(
        "field",
        [
            "schema_version",
            "event_id",
            "event_type",
            "source",
            "occurred_at",
            "payload",
        ],
    )
    def test_missing_required_field_is_rejected(
        self, validator, minimal_valid_event, field
    ):
        event = deepcopy(minimal_valid_event)
        del event[field]

        with pytest.raises(ValidationError):
            validator.validate(event)


class TestFieldFormatViolations:
    def test_invalid_event_id_format_is_rejected(self, validator, minimal_valid_event):
        event = deepcopy(minimal_valid_event)
        event["event_id"] = "not-a-uuid"

        with pytest.raises(ValidationError):
            validator.validate(event)

    def test_invalid_occurred_at_format_is_rejected(
        self, validator, minimal_valid_event
    ):
        event = deepcopy(minimal_valid_event)
        event["occurred_at"] = "yesterday afternoon"

        with pytest.raises(ValidationError):
            validator.validate(event)

    def test_lowercase_event_type_is_rejected(self, validator, minimal_valid_event):
        event = deepcopy(minimal_valid_event)
        event["event_type"] = "anomalyDetected"

        with pytest.raises(ValidationError):
            validator.validate(event)

    def test_malformed_schema_version_is_rejected(
        self, validator, minimal_valid_event
    ):
        event = deepcopy(minimal_valid_event)
        event["schema_version"] = "v1"

        with pytest.raises(ValidationError):
            validator.validate(event)

    def test_non_object_payload_is_rejected(self, validator, minimal_valid_event):
        event = deepcopy(minimal_valid_event)
        event["payload"] = "not an object"

        with pytest.raises(ValidationError):
            validator.validate(event)


class TestTopLevelStrictness:
    def test_unknown_top_level_field_is_rejected(
        self, validator, minimal_valid_event
    ):
        """
        Bewusst strikt: Felder, die eigentlich ins payload gehören
        (z. B. confidence), duerfen NICHT auf oberster Envelope-Ebene
        landen. Das verhindert das schleichende Aufweichen des Vertrags,
        das wir am ursprünglichen SEZRA-Envelope beobachtet hatten.
        """
        event = deepcopy(minimal_valid_event)
        event["confidence"] = 0.9

        with pytest.raises(ValidationError):
            validator.validate(event)

    def test_lifecycle_field_on_envelope_is_rejected(
        self, validator, minimal_valid_event
    ):
        """
        lifecycle wurde bewusst NICHT ins Envelope aufgenommen (siehe
        Diskussion Brick 1) - es ist ein abgeleitetes Konzept ueber
        correlation_id-Ketten, kein Feld einzelner Events.
        """
        event = deepcopy(minimal_valid_event)
        event["lifecycle"] = "open"

        with pytest.raises(ValidationError):
            validator.validate(event)


class TestProjectId:
    """
    project_id isoliert Daten unterschiedlicher Einsatzszenarien/
    Mandanten (z. B. beim Vektorisieren in Qdrant). Optional, damit
    aeltere Envelopes (schema_version 1.0) weiterhin gueltig bleiben.
    """

    def test_event_without_project_id_is_still_valid(
        self, validator, minimal_valid_event
    ):
        # minimal_valid_event enthaelt bewusst kein project_id
        validator.validate(minimal_valid_event)

    def test_valid_project_id_is_accepted(self, validator, minimal_valid_event):
        event = deepcopy(minimal_valid_event)
        event["project_id"] = "1a2b3c4d-5e6f-4a3a-9c1a-2b1a4e3a4a3a"

        validator.validate(event)

    def test_null_project_id_is_valid(self, validator, minimal_valid_event):
        event = deepcopy(minimal_valid_event)
        event["project_id"] = None

        validator.validate(event)

    def test_invalid_project_id_format_is_rejected(
        self, validator, minimal_valid_event
    ):
        event = deepcopy(minimal_valid_event)
        event["project_id"] = "not-a-uuid"

        with pytest.raises(ValidationError):
            validator.validate(event)