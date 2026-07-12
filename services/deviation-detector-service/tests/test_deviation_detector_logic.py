"""
Tests für deviation-detector-service.

Nutzt das School-Grade-Szenario (math_test_average, grade_level) statt
abstrakter Metrik-Namen - praxisnaeher und testet gleichzeitig den
Composite-Key-Fix an einem konkreten Fall: Grade 7 und Grade 8 duerfen
sich trotz gleichem "metric"-Namen keine Baseline teilen.
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
os.environ["DETECTOR_MIN_HISTORY_SIZE"] = "3"
os.environ["DETECTOR_STDDEV_MULTIPLIER"] = "2.0"

import main  # noqa: E402
from main import (  # noqa: E402
    DEAD_LETTER_EXCHANGE,
    DeviationType,
    OUTPUT_EXCHANGE,
    build_composite_key,
    create_anomaly_event,
    detect_deviation,
    handle_message,
    is_observation,
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


def make_grade_envelope(grade_level: int, value: float, event_id: str = None) -> dict:
    from uuid import uuid4

    return {
        "schema_version": "1.1",
        "event_id": event_id or str(uuid4()),
        "event_type": "ObservationIngested",
        "source": "json-adapter-service",
        "occurred_at": "2026-07-10T10:00:00Z",
        "project_id": "1a2b3c4d-5e6f-4a3a-9c1a-2b1a4e3a4a3a",
        "payload": {
            "source_type": "observation",
            "metric": "math_test_average",
            "grade_level": grade_level,
            "value": value,
        },
    }


class TestIsObservation:
    def test_metric_and_value_present_is_observation(self):
        assert is_observation({"metric": "x", "value": 1}) is True

    def test_missing_metric_is_not_observation(self):
        assert is_observation({"value": 1}) is False

    def test_context_payload_is_not_observation(self):
        assert is_observation({"sender": "principal@school.org", "text": "..."}) is False


class TestBuildCompositeKey:
    def test_same_metric_different_grade_level_produce_different_keys(self):
        key_grade_7 = build_composite_key({"metric": "math_test_average", "grade_level": 7, "value": 78})
        key_grade_8 = build_composite_key({"metric": "math_test_average", "grade_level": 8, "value": 78})

        assert key_grade_7 != key_grade_8

    def test_value_field_does_not_affect_the_key(self):
        key_a = build_composite_key({"metric": "math_test_average", "grade_level": 8, "value": 78})
        key_b = build_composite_key({"metric": "math_test_average", "grade_level": 8, "value": 62})

        assert key_a == key_b

    def test_source_type_does_not_affect_the_key(self):
        key_a = build_composite_key(
            {"metric": "x", "grade_level": 8, "value": 1, "source_type": "observation"}
        )
        key_b = build_composite_key({"metric": "x", "grade_level": 8, "value": 1})

        assert key_a == key_b

    def test_metric_alone_when_no_other_dimensions(self):
        assert build_composite_key({"metric": "simple_metric", "value": 1}) == "simple_metric"


class TestDetectDeviation:
    def setup_method(self):
        main.metric_history.clear()

    def test_no_deviation_flagged_during_history_warmup(self):
        for value in [78, 79, 77]:
            deviation, _ = detect_deviation("grade8-math", value)
            assert deviation is None

    def test_significant_drop_is_detected_after_warmup(self):
        for value in [78, 79, 77, 78]:
            detect_deviation("grade8-math", value)

        deviation, previous = detect_deviation("grade8-math", 20)

        assert deviation == DeviationType.DROP
        assert previous == 78

    def test_grade_7_and_grade_8_baselines_are_independent(self):
        """
        Der eigentliche Regressionstest fuer den Composite-Key-Fix:
        Grade 7 hat eine konstant hohe Baseline, Grade 8 eine niedrige.
        Ein Grade-8-Wert darf NICHT als Abweichung von Grade 7's
        Baseline behandelt werden (und umgekehrt) - sie muessen
        getrennte Historien fuehren.
        """
        for value in [95, 96, 94, 95]:
            detect_deviation("math_test_average|grade_level=7", value)

        for value in [60, 61, 59, 60]:
            detect_deviation("math_test_average|grade_level=8", value)

        # Ein normaler Grade-8-Wert (60) darf keine Anomalie sein,
        # obwohl er weit von Grade 7's Baseline (95) entfernt liegt -
        # er wird ja gegen Grade 8's eigene Baseline geprueft.
        deviation, _ = detect_deviation("math_test_average|grade_level=8", 61)
        assert deviation is None

    def test_no_false_positive_on_stable_values(self):
        for value in [78, 78, 78, 78]:
            deviation, _ = detect_deviation("grade8-math", value)
        assert deviation is None


class TestCreateAnomalyEvent:
    def test_project_id_is_passed_through(self):
        envelope = make_grade_envelope(grade_level=8, value=62)
        event = create_anomaly_event(envelope, "math_test_average|grade_level=8", 78, 62, DeviationType.DROP)

        assert event["project_id"] == envelope["project_id"]

    def test_causation_id_points_to_source_event(self):
        envelope = make_grade_envelope(grade_level=8, value=62)
        event = create_anomaly_event(envelope, "key", 78, 62, DeviationType.DROP)

        assert event["causation_id"] == envelope["event_id"]

    def test_payload_contains_composite_key(self):
        envelope = make_grade_envelope(grade_level=8, value=62)
        event = create_anomaly_event(
            envelope, "math_test_average|grade_level=8", 78, 62, DeviationType.DROP
        )

        assert event["payload"]["composite_key"] == "math_test_average|grade_level=8"

    def test_source_occurred_at_carries_original_observation_timestamp(self):
        """
        occurred_at im Envelope selbst bleibt deviation-detector-service's
        eigene Erzeugungszeit - der Zeitpunkt der urspruenglichen
        Beobachtung (die der Detector unveraendert von ingestion-service
        erhaelt) wandert als payload.source_occurred_at durch, damit der
        Analyzer spaeter zeitliche Kausalitaet pruefen kann.
        """
        envelope = make_grade_envelope(grade_level=8, value=62)
        event = create_anomaly_event(envelope, "key", 78, 62, DeviationType.DROP)

        assert event["payload"]["source_occurred_at"] == envelope["occurred_at"]


class TestHandleMessageWithGradeScenario:
    def setup_method(self):
        main.metric_history.clear()

    def test_context_events_are_ignored_without_error(self):
        channel = FakeChannel()
        context_envelope = {
            "schema_version": "1.1",
            "event_id": "6f9c2b1a-4e3a-4a3a-9c1a-2b1a4e3a4a3a",
            "event_type": "ContextIngested",
            "source": "json-adapter-service",
            "occurred_at": "2026-07-10T10:00:00Z",
            "payload": {"source_type": "context", "sender": "principal@school.org", "text": "..."},
        }
        body = json.dumps(context_envelope).encode("utf-8")

        handle_message(channel, make_method(), None, body)

        assert channel.published == []
        assert channel.acked == [1]

    def test_grade_drop_after_warmup_publishes_anomaly(self):
        channel = FakeChannel()

        for value in [78, 79, 77, 78]:
            envelope = make_grade_envelope(grade_level=8, value=value)
            handle_message(channel, make_method(), None, json.dumps(envelope).encode("utf-8"))

        channel.published.clear()  # Warmup-Nachrichten nicht mitzaehlen

        drop_envelope = make_grade_envelope(grade_level=8, value=20)
        handle_message(channel, make_method(), None, json.dumps(drop_envelope).encode("utf-8"))

        assert len(channel.published) == 1
        assert channel.published[0]["exchange"] == OUTPUT_EXCHANGE
        assert channel.published[0]["body"]["payload"]["anomaly_type"] == "drop"

    def test_grade_7_warmup_does_not_trigger_on_grade_8_first_value(self):
        """
        End-to-end durch handle_message: Grade-7-Beobachtungen duerfen
        Grade 8's allerersten Wert nicht als Anomalie erscheinen lassen.
        """
        channel = FakeChannel()

        for value in [95, 96, 94, 95]:
            envelope = make_grade_envelope(grade_level=7, value=value)
            handle_message(channel, make_method(), None, json.dumps(envelope).encode("utf-8"))

        channel.published.clear()

        first_grade_8_envelope = make_grade_envelope(grade_level=8, value=60)
        handle_message(channel, make_method(), None, json.dumps(first_grade_8_envelope).encode("utf-8"))

        # Erster Grade-8-Wert -> Historie-Warmup, keine Anomalie moeglich
        assert channel.published == []