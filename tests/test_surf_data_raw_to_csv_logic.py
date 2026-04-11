from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


def _load_module_from_path(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    assert spec is not None
    assert spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    # dataclasses may need the module in sys.modules when resolving annotations
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_find_complete_ready_batch_picks_latest_timestamp_and_ready_only():
    repo_root = Path(__file__).resolve().parents[1]
    dag_path = repo_root / "dags" / "surf_data_raw_to_csv.py"
    mod = _load_module_from_path("surf_data_raw_to_csv", dag_path)

    class FakeS3:
        def __init__(self, keys: list[str]):
            self._keys = keys

        def list_keys(self, bucket_name=None, prefix=None, **kwargs):
            assert bucket_name == "bucket"
            assert prefix == "data/surf_data_raw/"
            return list(self._keys)

    keys = [
        # Old complete set (should not be chosen)
        "data/surf_data_raw/a/2026040814.json.ready",
        "data/surf_data_raw/b/2026040814.json.ready",
        "data/surf_data_raw/c/2026040814.json.ready",
        "data/surf_data_raw/d/2026040814.json.ready",
        # New complete set (should be chosen)
        "data/surf_data_raw/a/2026040815.json.ready",
        "data/surf_data_raw/b/2026040815.json.ready",
        "data/surf_data_raw/c/2026040815.json.ready",
        "data/surf_data_raw/d/2026040815.json.ready",
        # Not ready (should be ignored)
        "data/surf_data_raw/e/2026040815.json",
        # Processed (should be ignored)
        "data/surf_data_raw/a/2026040815.json.processed",
    ]

    batch = mod._find_complete_ready_batch(
        s3=FakeS3(keys),
        bucket="bucket",
        raw_prefix="data/surf_data_raw",
        expected_poi_count=4,
    )

    assert batch is not None
    assert batch.timestamp == "2026040815"
    # raw_keys are deterministic by poi slug sort
    assert batch.raw_keys == [
        "data/surf_data_raw/a/2026040815.json.ready",
        "data/surf_data_raw/b/2026040815.json.ready",
        "data/surf_data_raw/c/2026040815.json.ready",
        "data/surf_data_raw/d/2026040815.json.ready",
    ]


def test_wave_height_for_run_date_uses_matching_hour():
    repo_root = Path(__file__).resolve().parents[1]
    dag_path = repo_root / "dags" / "surf_data_raw_to_csv.py"
    mod = _load_module_from_path("surf_data_raw_to_csv_wave", dag_path)

    doc = {
        "run_date": "2026040815",
        "marine": {
            "hourly": {
                "time": [
                    "2026-04-08T14:00",
                    "2026-04-08T15:00",
                    "2026-04-08T16:00",
                ],
                "wave_height": [0.5, 1.25, 0.75],
            }
        },
    }

    assert mod._wave_height_for_run_date(doc) == 1.25


def test_extract_fields_smoke():
    repo_root = Path(__file__).resolve().parents[1]
    dag_path = repo_root / "dags" / "surf_data_raw_to_csv.py"
    mod = _load_module_from_path("surf_data_raw_to_csv_fields", dag_path)

    doc = {
        "point_of_interest": "Barrington Beach, RI",
        "retrieved_at_utc": "2026040815",
        "run_date": "2026040815",
        "weather": {"current": {"temperature_2m": 12.3, "wind_speed_10m": 4.5}},
        "marine": {"hourly": {"time": ["2026-04-08T15:00"], "wave_height": [2.0]}},
    }

    row = mod._extract_fields(doc)
    assert row["point_of_interest"] == "Barrington Beach, RI"
    assert row["retrieved_at_utc"] == "2026040815"
    assert row["temperature_2m"] == 12.3
    assert row["wind_speed_10m"] == 4.5
    assert row["wave_height"] == 2.0
