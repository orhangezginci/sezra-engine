"""
Grundgerüst-Test für vectorizing-service.

Prüft nur, dass die kopierte envelope_validation.py korrekt gegen das
mitkopierte Schema validiert. Ersetze/ergänze mit echten Tests für die
Business-Logik dieses Service.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

import pytest  # noqa: E402

from envelope_validation import InvalidEnvelopeError, validate_envelope  # noqa: E402


def test_valid_minimal_envelope_passes():
    envelope = {
        "schema_version": "1.0",
        "event_id": "6f9c2b1a-4e3a-4a3a-9c1a-2b1a4e3a4a3a",
        "event_type": "SomeEvent",
        "source": "vectorizing-service",
        "occurred_at": "2026-07-05T10:00:00Z",
        "payload": {},
    }

    validate_envelope(envelope)  # wirft nicht -> Erfolg


def test_missing_required_field_is_rejected():
    envelope = {
        "schema_version": "1.0",
        "event_id": "6f9c2b1a-4e3a-4a3a-9c1a-2b1a4e3a4a3a",
        "source": "vectorizing-service",
        "occurred_at": "2026-07-05T10:00:00Z",
        "payload": {},
    }

    with pytest.raises(InvalidEnvelopeError):
        validate_envelope(envelope)