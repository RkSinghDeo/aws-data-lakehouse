"""Unit tests for PySpark transform logic (run without a live Spark session)."""

import pytest


class TestDeduplication:
    def test_dedup_keeps_latest_row(self):
        """Simulate dedup logic: row with latest timestamp survives."""
        rows = [
            {"event_id": "evt-1", "value": 10, "_ingested_at": "2025-01-01T10:00:00"},
            {"event_id": "evt-1", "value": 20, "_ingested_at": "2025-01-01T12:00:00"},
            {"event_id": "evt-2", "value": 5, "_ingested_at": "2025-01-01T09:00:00"},
        ]
        # Group by event_id, keep latest
        deduped: dict[str, dict] = {}
        for row in rows:
            eid = row["event_id"]
            if eid not in deduped or row["_ingested_at"] > deduped[eid]["_ingested_at"]:
                deduped[eid] = row

        assert len(deduped) == 2
        assert deduped["evt-1"]["value"] == 20  # latest wins


class TestSchemaValidation:
    def test_null_required_field_is_invalid(self):
        required_cols = ["event_id", "event_time"]
        rows = [
            {"event_id": "e1", "event_time": "2025-01-01", "amount": 100},
            {"event_id": None, "event_time": "2025-01-01", "amount": 50},
            {"event_id": "e3", "event_time": None, "amount": 75},
        ]

        valid = [r for r in rows if all(r.get(c) is not None for c in required_cols)]
        invalid = [r for r in rows if any(r.get(c) is None for c in required_cols)]

        assert len(valid) == 1
        assert len(invalid) == 2

    def test_validation_rate_calculation(self):
        total = 1000
        invalid = 23
        pct_valid = (total - invalid) / total * 100
        assert round(pct_valid, 1) == 97.7


class TestGoldAggregation:
    def test_daily_summary_groups_correctly(self):
        events = [
            {"event_date": "2025-01-01", "event_type": "purchase", "amount": 100},
            {"event_date": "2025-01-01", "event_type": "purchase", "amount": 200},
            {"event_date": "2025-01-01", "event_type": "refund", "amount": -50},
            {"event_date": "2025-01-02", "event_type": "purchase", "amount": 150},
        ]

        from collections import defaultdict

        summary: dict[tuple, dict] = defaultdict(lambda: {"event_count": 0, "total_amount": 0})
        for e in events:
            key = (e["event_date"], e["event_type"])
            summary[key]["event_count"] += 1
            summary[key]["total_amount"] += e["amount"]

        assert summary[("2025-01-01", "purchase")]["event_count"] == 2
        assert summary[("2025-01-01", "purchase")]["total_amount"] == 300
        assert summary[("2025-01-01", "refund")]["total_amount"] == -50

    def test_iceberg_merge_keys_cover_partition(self):
        """Merge keys must include the partition column to avoid full table rewrites."""
        merge_keys = ["event_date", "event_type"]
        partition_cols = ["event_date"]
        assert all(p in merge_keys for p in partition_cols), \
            "All partition columns must be in merge keys for efficient CDC"
