import json
from pathlib import Path

import jsonschema


class InvalidEnvelopeError(Exception):
    pass


_SCHEMA_PATH = Path(__file__).parent / "contracts" / "envelope.schema.json"
_schema = json.loads(_SCHEMA_PATH.read_text())
_validator = jsonschema.Draft7Validator(_schema, format_checker=jsonschema.FormatChecker())


def validate_envelope(envelope: dict) -> None:
    errors = sorted(_validator.iter_errors(envelope), key=lambda e: e.path)
    if errors:
        messages = "; ".join(f"{list(e.path)}: {e.message}" for e in errors)
        raise InvalidEnvelopeError(messages)