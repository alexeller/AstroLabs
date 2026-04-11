from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from plugins.operators.open_meteo_surf_operator import GeoPoint, OpenMeteoSurfDataToS3Operator


def test_build_geocode_queries_rewrites_state_abbrev():
    op = OpenMeteoSurfDataToS3Operator(
        task_id="t",
        point_of_interest="Tiverton, RI",
        s3_bucket="bucket",
    )

    queries = op._build_geocode_queries("Tiverton, RI")
    assert queries[0] == "Tiverton, RI"
    # Should include Rhode Island rewrite
    assert any("Rhode Island" in q for q in queries)


def test_execute_writes_json_ready_object_and_returns_s3_uri(monkeypatch):
    fixed_now = datetime(2026, 4, 8, 15, 30, 0, tzinfo=timezone.utc)

    op = OpenMeteoSurfDataToS3Operator(
        task_id="t",
        point_of_interest="Barrington Beach, RI",
        s3_bucket="my-bucket",
        s3_prefix="data/surf_data_raw",
        aws_conn_id="aws",
        run_date="2026040815",
    )

    # Avoid external calls.
    monkeypatch.setattr(op, "_geocode", lambda: GeoPoint("X", 41.0, -71.0))
    monkeypatch.setattr(op, "_fetch_weather", lambda lat, lon: {"current": {"temperature_2m": 11.1, "wind_speed_10m": 2.2}})
    monkeypatch.setattr(op, "_fetch_marine", lambda lat, lon: {"hourly": {"time": ["2026-04-08T15:00"], "wave_height": [1.5]}})

    # Make datetime.now() deterministic for retrieved_at_utc.
    import plugins.operators.open_meteo_surf_operator as mod

    class _FakeDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(mod, "datetime", _FakeDateTime)

    captured: dict[str, object] = {}

    class FakeS3Hook:
        def __init__(self, aws_conn_id=None):
            assert aws_conn_id == "aws"

        def load_bytes(self, *, bytes_data, key, bucket_name, replace):
            captured["bytes_data"] = bytes_data
            captured["key"] = key
            captured["bucket_name"] = bucket_name
            captured["replace"] = replace

    monkeypatch.setattr(mod, "S3Hook", FakeS3Hook)

    uri = op.execute(context={"logical_date": datetime(2026, 4, 8, 15, tzinfo=timezone.utc)})

    assert uri == "s3://my-bucket/data/surf_data_raw/barrington-beach-ri/2026040815.json.ready"
    assert captured["bucket_name"] == "my-bucket"
    assert captured["key"] == "data/surf_data_raw/barrington-beach-ri/2026040815.json.ready"
    assert captured["replace"] is True

    doc = json.loads(captured["bytes_data"].decode("utf-8"))
    assert doc["point_of_interest"] == "Barrington Beach, RI"
    assert doc["run_date"] == "2026040815"
    assert doc["weather"]["current"]["temperature_2m"] == 11.1
    assert doc["weather"]["current"]["wind_speed_10m"] == 2.2
    assert doc["marine"]["hourly"]["wave_height"] == [1.5]

    # retrieved_at_utc should be ISO-parseable.
    parsed = datetime.fromisoformat(doc["retrieved_at_utc"])
    assert parsed.tzinfo is not None


def test_execute_uses_context_logical_date_when_run_date_missing(monkeypatch):
    op = OpenMeteoSurfDataToS3Operator(
        task_id="t",
        point_of_interest="Tiverton, RI",
        s3_bucket="bucket",
        aws_conn_id="aws",
        run_date=None,
    )

    monkeypatch.setattr(op, "_geocode", lambda: GeoPoint("X", 41.0, -71.0))
    monkeypatch.setattr(op, "_fetch_weather", lambda lat, lon: {})
    monkeypatch.setattr(op, "_fetch_marine", lambda lat, lon: {})

    import plugins.operators.open_meteo_surf_operator as mod

    class FakeS3Hook:
        def __init__(self, aws_conn_id=None):
            pass

        def load_bytes(self, *, bytes_data, key, bucket_name, replace):
            # Key must include formatted run_date from context.
            assert "/2026040815.json.ready" in key

    monkeypatch.setattr(mod, "S3Hook", FakeS3Hook)

    logical_date = datetime(2026, 4, 8, 15, tzinfo=timezone.utc)
    uri = op.execute(context={"logical_date": logical_date})
    assert uri.endswith("/2026040815.json.ready")


@pytest.mark.parametrize(
    "poi,expected",
    [
        ("Barrington Beach, RI", "barrington-beach-ri"),
        (" Easton Beach, Newport, RI ", "easton-beach-newport-ri"),
    ],
)
def test_slug_in_output_key(monkeypatch, poi, expected):
    op = OpenMeteoSurfDataToS3Operator(
        task_id="t",
        point_of_interest=poi,
        s3_bucket="bucket",
        aws_conn_id="aws",
        run_date="2026040815",
    )

    monkeypatch.setattr(op, "_geocode", lambda: GeoPoint("X", 41.0, -71.0))
    monkeypatch.setattr(op, "_fetch_weather", lambda lat, lon: {})
    monkeypatch.setattr(op, "_fetch_marine", lambda lat, lon: {})

    import plugins.operators.open_meteo_surf_operator as mod

    captured_key = {}

    class FakeS3Hook:
        def __init__(self, aws_conn_id=None):
            pass

        def load_bytes(self, *, bytes_data, key, bucket_name, replace):
            captured_key["key"] = key

    monkeypatch.setattr(mod, "S3Hook", FakeS3Hook)

    op.execute(context={})
    assert captured_key["key"].split("/")[-2] == expected
