"""
Tests for the order event generator.

These verify that generated events have the correct structure and data quality
properties. This is NOT the same as data quality checks on warehouse tables —
this tests the SOURCE CODE that produces the data.

PDF Connection: Data Contracts (DM pages 21-23) — we're testing that our
"producer" adheres to the expected schema, which is the essence of a data contract.
"""

import sys
import os
import json

# Add parent directory to path so we can import the generator
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from data_generators.order_generator import generate_order_event, PLATFORMS, KITCHENS


class TestOrderEventStructure:
    """Test that generated events have all required fields."""
    
    def test_event_has_order_id(self):
        """Every event must have an order_id, regardless of platform."""
        event, _ = generate_order_event()
        assert "order_id" in event, "Event missing order_id"
        assert event["order_id"] is not None, "order_id should not be None"
    
    def test_event_has_platform(self):
        """Every event must identify which platform it came from."""
        event, _ = generate_order_event()
        assert "platform" in event
        assert event["platform"] in PLATFORMS
    
    def test_uber_eats_schema(self):
        """Uber Eats events must use Uber-specific field names.
        
        This tests the DELIBERATE schema misalignment — Uber uses
        'total_amount' not 'order_total'. Your Silver layer must normalize this.
        """
        # Generate events until we get an Uber one
        for _ in range(100):
            event, _ = generate_order_event()
            if event["platform"] == "uber_eats":
                assert "total_amount" in event, "Uber events must use 'total_amount'"
                assert "customer_uid" in event, "Uber events must use 'customer_uid'"
                assert "order_timestamp" in event, "Uber events must use 'order_timestamp'"
                assert isinstance(event["total_amount"], float), "total_amount must be float"
                return
        assert False, "Failed to generate an Uber event in 100 attempts"
    
    def test_doordash_schema(self):
        """DoorDash events must use DoorDash-specific field names."""
        for _ in range(100):
            event, _ = generate_order_event()
            if event["platform"] == "doordash":
                assert "order_value" in event, "DoorDash events must use 'order_value'"
                assert "dasher_customer_id" in event, "DoorDash must use 'dasher_customer_id'"
                assert "store_id" in event, "DoorDash must use 'store_id' (not kitchen_id)"
                assert "created_at" in event, "DoorDash must use 'created_at' (not order_timestamp)"
                return
        assert False, "Failed to generate a DoorDash event in 100 attempts"
    
    def test_ownapp_schema(self):
        """OwnApp events use 'amount_cents' (integer, not float)."""
        for _ in range(100):
            event, _ = generate_order_event()
            if event["platform"] == "own_app":
                assert "amount_cents" in event, "OwnApp must use 'amount_cents'"
                assert isinstance(event["amount_cents"], int), "amount_cents must be integer!"
                assert "user_id" in event, "OwnApp must use 'user_id'"
                assert "timestamp" in event, "OwnApp must use 'timestamp'"
                return
        assert False, "Failed to generate an OwnApp event in 100 attempts"


class TestOrderEventQuality:
    """Test the data quality properties of generated events."""
    
    def test_order_total_is_positive(self):
        """No order should have zero or negative total."""
        for _ in range(200):
            event, _ = generate_order_event()
            # Get total regardless of platform-specific field name
            total = (event.get("total_amount") or 
                     event.get("order_value") or 
                     (event.get("amount_cents", 0) / 100))
            assert total > 0, f"Order total must be positive, got {total}"
    
    def test_kitchen_id_format(self):
        """Kitchen IDs must match format K-{CITY}-{NUM}."""
        import re
        pattern = r'^K-[A-Z]{3}-\d{2}$'
        for _ in range(200):
            event, _ = generate_order_event()
            kid = event.get("kitchen_id") or event.get("store_id")
            assert re.match(pattern, kid), f"Kitchen ID '{kid}' doesn't match pattern K-XXX-NN"
    
    def test_items_not_empty(self):
        """Every order must have at least one item."""
        for _ in range(200):
            event, _ = generate_order_event()
            items = event.get("items") or event.get("line_items")
            assert items is not None, "Items/line_items should exist"
            assert len(items) > 0, "Order must have at least 1 item"
    
    def test_duplicate_rate_approximately_correct(self):
        """~5% of events should be duplicates (within reasonable variance).
        
        This test verifies our deliberate data quality issue injection.
        We test that duplicates EXIST because our pipeline must handle them.
        """
        duplicate_count = 0
        total = 1000
        for _ in range(total):
            _, is_dup = generate_order_event()
            if is_dup:
                duplicate_count += 1
        
        dup_rate = duplicate_count / total
        # Allow wide range (1% to 10%) because of randomness with 1000 samples
        assert 0.01 < dup_rate < 0.10, (
            f"Duplicate rate {dup_rate:.1%} outside expected range 1-10%. "
            f"Expected ~5%."
        )
    
    def test_some_events_have_null_email(self):
        """~2% of events should have null customer email.
        
        This verifies our identity resolution challenge exists:
        not all events have email for matching across platforms.
        """
        null_count = 0
        total = 1000
        for _ in range(total):
            event, _ = generate_order_event()
            email = event.get("customer_email") or event.get("email")
            if email is None:
                null_count += 1
        
        # At least SOME nulls should exist (may not hit exactly 2% with 1000 samples)
        assert null_count > 0, "Expected some null emails for identity resolution testing"


class TestOrderEventSerializable:
    """Test that events can be serialized to JSON (required for Kafka)."""
    
    def test_json_serializable(self):
        """Every event must be JSON-serializable for Kafka."""
        for _ in range(100):
            event, _ = generate_order_event()
            try:
                json_str = json.dumps(event)
                parsed = json.loads(json_str)
                assert parsed["order_id"] == event["order_id"]
            except (TypeError, ValueError) as e:
                assert False, f"Event not JSON-serializable: {e}"


# Run with: pytest tests/test_order_generator.py -v