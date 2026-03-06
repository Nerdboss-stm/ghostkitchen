"""
Tests for kitchen sensor generator.
Mirrors test_order_generator.py structure.

Run: pytest tests/test_sensor_generator.py -v
"""

import sys, os, re, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from data_generators.sensor_generator import (
    generate_sensor_reading, init_sensor_states, sensor_states, KITCHENS, SENSOR_CONFIGS
)

VALID_SENSOR_TYPES = {"temperature", "humidity", "fryer_timer", "cooler_door"}
VALID_UNITS        = {"fahrenheit", "seconds", "binary", "percent"}
VALID_ZONES        = {z["zone"] for z in SENSOR_CONFIGS}


class TestSensorEventStructure:

    def setup_method(self):
        init_sensor_states()

    def test_event_has_required_fields(self):
        """Every sensor event must have: reading_id, sensor_id, kitchen_id,
        sensor_type, value, unit, event_timestamp."""
        sensor_id = list(sensor_states.keys())[0]
        event, _ = generate_sensor_reading(sensor_id)
        required = ["reading_id", "sensor_id", "kitchen_id", "sensor_type",
                    "value", "unit", "event_timestamp"]
        for field in required:
            assert field in event, f"Missing required field: {field}"

    def test_sensor_id_format(self):
        """Sensor IDs must match format S-{CITY}{NUM}-{SENSOR}."""
        sensor_id = list(sensor_states.keys())[0]
        event, _ = generate_sensor_reading(sensor_id)
        assert re.match(r'^S-[A-Z]+-\d+-\S+$', event["sensor_id"]), \
            f"Sensor ID format wrong: {event['sensor_id']}"

    def test_kitchen_id_is_valid(self):
        """kitchen_id must be one of the defined kitchens."""
        sensor_id = list(sensor_states.keys())[0]
        event, _ = generate_sensor_reading(sensor_id)
        assert event["kitchen_id"] in KITCHENS

    def test_sensor_type_is_valid(self):
        """sensor_type must be one of the known types."""
        for sid in list(sensor_states.keys()):
            event, _ = generate_sensor_reading(sid)
            assert event["sensor_type"] in VALID_SENSOR_TYPES, \
                f"Unknown sensor_type: {event['sensor_type']}"

    def test_unit_is_valid(self):
        """unit must be one of: fahrenheit, seconds, binary, percent."""
        for sid in list(sensor_states.keys()):
            event, _ = generate_sensor_reading(sid)
            assert event["unit"] in VALID_UNITS, f"Unknown unit: {event['unit']}"

    def test_zone_is_valid(self):
        """zone must be one of the defined kitchen zones."""
        for sid in list(sensor_states.keys()):
            event, _ = generate_sensor_reading(sid)
            assert event["zone"] in VALID_ZONES, f"Unknown zone: {event['zone']}"

    def test_timestamp_format(self):
        """event_timestamp must be ISO 8601 ending with Z."""
        sensor_id = list(sensor_states.keys())[0]
        for _ in range(20):
            event, _ = generate_sensor_reading(sensor_id)
            ts = event["event_timestamp"]
            assert ts.endswith("Z"), f"Timestamp missing Z suffix: {ts}"
            assert "T" in ts, f"Timestamp not ISO format: {ts}"

    def test_reading_id_is_unique(self):
        """Each reading must have a unique reading_id."""
        sensor_id = list(sensor_states.keys())[0]
        ids = [generate_sensor_reading(sensor_id)[0]["reading_id"] for _ in range(50)]
        assert len(set(ids)) == 50, "Duplicate reading_ids detected"

    def test_value_drifts_not_jumps(self):
        """Consecutive readings from same sensor should drift, not jump randomly."""
        sensor_id = list(sensor_states.keys())[0]
        event1, _ = generate_sensor_reading(sensor_id)
        event2, _ = generate_sensor_reading(sensor_id)
        if event1["value"] is not None and event2["value"] is not None:
            if event1["sensor_type"] not in ("cooler_door", "fryer_timer"):
                diff = abs(event2["value"] - event1["value"])
                assert diff < 20, f"Values jumped too much: {event1['value']} → {event2['value']}"


class TestSensorDataQuality:

    def setup_method(self):
        init_sensor_states()

    def test_some_null_values_exist(self):
        """~1% of readings should have null values (sensor malfunction)."""
        null_count = 0
        sensor_ids = list(sensor_states.keys())
        for i in range(500):
            event, _ = generate_sensor_reading(sensor_ids[i % len(sensor_ids)])
            if event["value"] is None:
                null_count += 1
        assert null_count > 0, "Expected some null values for data quality testing"

    def test_duplicate_rate_approximately_correct(self):
        """~3% of events should be duplicates (within reasonable variance)."""
        dup_count = 0
        total = 1000
        sensor_ids = list(sensor_states.keys())
        for i in range(total):
            _, is_dup = generate_sensor_reading(sensor_ids[i % len(sensor_ids)])
            if is_dup:
                dup_count += 1
        dup_rate = dup_count / total
        assert 0.005 < dup_rate < 0.10, \
            f"Duplicate rate {dup_rate:.1%} outside expected range. Expected ~3%."

    def test_some_anomalies_exist(self):
        """~0.5% of readings should be anomalous (equipment issues)."""
        sensor_ids = list(sensor_states.keys())
        anomaly_count = sum(
            1 for i in range(2000)
            if generate_sensor_reading(sensor_ids[i % len(sensor_ids)])[0]
               .get("metadata", {}).get("is_anomaly_injected")
        )
        assert anomaly_count > 0, "Expected some anomaly-injected readings in 2000 samples"

    def test_cooler_door_is_binary(self):
        """cooler_door sensor value must be 0 or 1 only."""
        door_sensors = [sid for sid, st in sensor_states.items()
                        if st["config"]["type"] == "cooler_door"]
        for sid in door_sensors:
            for _ in range(20):
                event, _ = generate_sensor_reading(sid)
                if event["value"] is not None:
                    assert event["value"] in (0, 1), \
                        f"cooler_door value must be 0 or 1, got {event['value']}"

    def test_fryer_timer_non_negative(self):
        """fryer_timer value must never go negative."""
        timer_sensors = [sid for sid, st in sensor_states.items()
                         if st["config"]["type"] == "fryer_timer"]
        for sid in timer_sensors:
            for _ in range(30):
                event, _ = generate_sensor_reading(sid)
                if event["value"] is not None:
                    assert event["value"] >= 0, \
                        f"fryer_timer went negative: {event['value']}"


class TestSensorReadingSerializable:

    def setup_method(self):
        init_sensor_states()

    def test_json_serializable(self):
        """Every event must be JSON-serializable for Kafka."""
        sensor_ids = list(sensor_states.keys())
        for i in range(100):
            event, _ = generate_sensor_reading(sensor_ids[i % len(sensor_ids)])
            try:
                parsed = json.loads(json.dumps(event))
                assert parsed["sensor_id"] == event["sensor_id"]
            except (TypeError, ValueError) as e:
                assert False, f"Event not JSON-serializable: {e}\nEvent: {event}"
