"""
Envelope-Validierung fuer api-service.

Diese Datei ist eine EIGENSTAENDIGE KOPIE, keine geteilte Bibliothek.
Sie gehoert vollstaendig diesem Service - Aenderungen hier wirken sich
auf keinen anderen Service aus, und Aenderungen an anderen Services
wirken sich nicht auf diese Datei aus.

Einzige Quelle der Wahrheit fuer den Vertrag selbst ist
contracts/envelope.schema.json (wird beim Docker-Build in dieses
Image kopiert, siehe Dockerfile).
"""

import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker, ValidationError

_SCHEMA_PATH = Path(__file__).parent / "contracts" / "envelope.schema.json"

with open(_SCHEMA_PATH, "r") as _file:
    _SCHEMA = json.load(_file)

Draft202012Validator.check_schema(_SCHEMA)
_VALIDATOR = Draft202012Validator(_SCHEMA, format_checker=FormatChecker())


class InvalidEnvelopeError(Exception):
    """Wird geworfen, wenn ein Envelope gegen das Schema verstoesst."""

    def __init__(self, message: str, original_error: ValidationError):
        super().__init__(message)
        self.original_error = original_error


def validate_envelope(envelope: dict) -> None:
    """
    Validiert ein Envelope-Dict gegen envelope.schema.json.

    Wirft InvalidEnvelopeError bei Verstoss - der Aufrufer entscheidet,
    ob das ein "permanent"-Fehler ist (siehe contracts/README.md,
    Abschnitt 3 - Dead-Letter-Handling).
    """
    try:
        _VALIDATOR.validate(envelope)
    except ValidationError as error:
        raise InvalidEnvelopeError(
            f"Envelope verletzt das Schema: {error.message}", error
        ) from error