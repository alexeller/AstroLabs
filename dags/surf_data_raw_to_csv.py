"""## S3 surf raw (.ready) → CSV (.ready)

Polls S3 for newly-arrived raw surf data files, extracts a small set of fields,
writes CSV outputs, then marks inputs as processed and deletes old raw files.

Expected input layout (S3 keys):

        s3://<bucket>/<RAW_PREFIX>/<poi_slug>/<RUN_TIMESTAMP>.json.ready

The DAG waits until *all four* POI subfolders contain a `.ready` file for the
same timestamp before proceeding.

Outputs are written to a parallel prefix:

    s3://<bucket>/<CSV_PREFIX>/<RUN_TIMESTAMP>.csv.ready

Compatibility
-------------
Some earlier/raw datasets may have keys ending in `.json` (no `.ready`). This
DAG treats `<ts>.json` as a ready input as well, but the normal/expected format
is `<ts>.json.ready`.

Notes
-----
- Timestamp parsing is based on a numeric token embedded in the filename.
    Supported formats: YYYYMMDDHH, YYYYMMDDHHMM, YYYYMMDDHHMMSS.
- The raw object body is expected to be JSON in the shape emitted by
    `OpenMeteoSurfDataToS3Operator`.
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.exceptions import AirflowException
from airflow.sdk import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pendulum import datetime as pdatetime

try:  # Airflow 2.9+ (standard provider)
    from airflow.providers.standard.sensors.python import PythonSensor
except Exception:  # pragma: no cover
    from airflow.sensors.python import PythonSensor


@dataclass(frozen=True)
class ReadyBatch:
    """A complete set of ready files for a single timestamp."""

    timestamp: str
    raw_keys: list[str]


def _slugify(value: str) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "poi"


_TS_RE = re.compile(r"(\d{14}|\d{12}|\d{10})")


def _extract_timestamp_token(filename: str) -> str | None:
    """Return timestamp token (YYYYMMDDHH[MM[SS]]) from filename, if present."""

    m = _TS_RE.search(filename)
    if not m:
        return None
    return m.group(1)


def _parse_timestamp_utc(ts: str) -> datetime:
    if len(ts) == 10:
        fmt = "%Y%m%d%H"
    elif len(ts) == 12:
        fmt = "%Y%m%d%H%M"
    elif len(ts) == 14:
        fmt = "%Y%m%d%H%M%S"
    else:
        raise ValueError(f"Unsupported timestamp length: {ts}")
    return datetime.strptime(ts, fmt).replace(tzinfo=timezone.utc)


def _is_candidate_ready_key(key: str) -> bool:
    if not key:
        return False
    if ".processed" in key:
        return False
    # Expected: <ts>.json.ready; compat: <ts>.json
    return key.endswith(".json.ready") or key.endswith(".json")


def _split_raw_key(key: str, raw_prefix: str) -> tuple[str, str] | None:
    """Return (poi_slug, ts_token) for a raw key, or None if not parseable."""

    raw_prefix = raw_prefix.strip("/")
    if not key.startswith(raw_prefix + "/"):
        return None

    rel = key[len(raw_prefix) + 1 :]
    parts = rel.split("/", 1)
    if len(parts) != 2:
        return None
    poi_slug, filename = parts[0], parts[1]
    if not poi_slug or not filename:
        return None

    ts = _extract_timestamp_token(filename)
    if not ts:
        return None
    return poi_slug, ts


def _find_complete_ready_batch(
    *,
    s3: S3Hook,
    bucket: str,
    raw_prefix: str,
    expected_poi_count: int,
) -> ReadyBatch | None:
    """Find a timestamp for which we have `expected_poi_count` ready objects.

    We count distinct POI subfolders for each timestamp and return the *oldest*
    complete timestamp to keep processing order stable.
    """

    keys = s3.list_keys(bucket_name=bucket, prefix=raw_prefix.strip("/") + "/") or []

    # Map: timestamp -> {poi_slug -> key}
    grouped: dict[str, dict[str, str]] = {}
    for key in keys:
        if not _is_candidate_ready_key(key):
            continue

        split = _split_raw_key(key, raw_prefix)
        if split is None:
            continue
        poi_slug, ts = split
        by_poi = grouped.setdefault(ts, {})
        existing = by_poi.get(poi_slug)
        # Prefer the marker-style key if both exist.
        if existing is None:
            by_poi[poi_slug] = key
        elif (not existing.endswith(".ready")) and key.endswith(".ready"):
            by_poi[poi_slug] = key

    complete_ts = [ts for ts, by_poi in grouped.items() if len(by_poi) >= expected_poi_count]
    if not complete_ts:
        return None

    chosen = sorted(complete_ts)[0]
    by_poi = grouped[chosen]

    # Deterministic order in XCom payload.
    raw_keys = [by_poi[poi] for poi in sorted(by_poi.keys())[:expected_poi_count]]
    return ReadyBatch(timestamp=chosen, raw_keys=raw_keys)


def _first_non_null(values: Any) -> float | None:
    if isinstance(values, list):
        for v in values:
            if v is None:
                continue
            try:
                return float(v)
            except Exception:
                continue
    try:
        return float(values)
    except Exception:
        return None


def _wave_height_for_run_date(doc: dict[str, Any]) -> float | None:
    run_date = str(doc.get("run_date") or "").strip()
    if run_date:
        try:
            target_dt = _parse_timestamp_utc(run_date)
            target_prefix = target_dt.strftime("%Y-%m-%dT%H")
            marine_hourly = (doc.get("marine") or {}).get("hourly") or {}
            times = marine_hourly.get("time") or []
            heights = marine_hourly.get("wave_height") or []
            if isinstance(times, list) and isinstance(heights, list) and len(times) == len(heights):
                for idx, t in enumerate(times):
                    if isinstance(t, str) and t.startswith(target_prefix):
                        try:
                            return float(heights[idx])
                        except Exception:
                            break
        except Exception:
            pass

    marine_hourly = (doc.get("marine") or {}).get("hourly") or {}
    return _first_non_null(marine_hourly.get("wave_height"))


def _extract_fields(doc: dict[str, Any]) -> dict[str, Any]:
    poi = doc.get("point_of_interest")
    retrieved_at_utc = doc.get("retrieved_at_utc")

    weather_current = (doc.get("weather") or {}).get("current") or {}
    temperature_2m = weather_current.get("temperature_2m")
    wind_speed_10m = weather_current.get("wind_speed_10m")

    wave_height = _wave_height_for_run_date(doc)

    return {
        "point_of_interest": poi,
        "retrieved_at_utc": retrieved_at_utc,
        "temperature_2m": temperature_2m,
        "wind_speed_10m": wind_speed_10m,
        "wave_height": wave_height,
    }


EXPECTED_POI_COUNT = int(os.environ.get("SURF_EXPECTED_FILES", "4"))

S3_BUCKET = os.environ.get("S3_BUCKET", "astrolabs-634236767178-us-east-1-an")
AWS_CONN_ID = os.environ.get("AWS_CONN_ID", "astro_s3_conn")

RAW_PREFIX = os.environ.get("SURF_DATA_RAW_PREFIX", "data/surf_data_raw").strip("/")
CSV_PREFIX = os.environ.get("SURF_DATA_CSV_PREFIX", "data/surf_data_csv").strip("/")


@dag(
    start_date=pdatetime(2025, 4, 22),
    schedule=None,
    catchup=False,
    default_args={"owner": "Astro", "retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["s3", "surf", "csv"],
    doc_md=__doc__,
)
def surf_data_raw_to_csv():
    def _poke_for_ready_batch() -> PokeReturnValue:
        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
        batch = _find_complete_ready_batch(
            s3=s3,
            bucket=S3_BUCKET,
            raw_prefix=RAW_PREFIX,
            expected_poi_count=EXPECTED_POI_COUNT,
        )
        if batch is None:
            return PokeReturnValue(is_done=False)

        return PokeReturnValue(
            is_done=True,
            xcom_value={"timestamp": batch.timestamp, "raw_keys": batch.raw_keys},
        )

    poll_for_ready_batch = PythonSensor(
        task_id="poll_for_ready_batch",
        python_callable=_poke_for_ready_batch,
        poke_interval=int(os.environ.get("SURF_POKE_INTERVAL_SECONDS", "60")),
        timeout=int(os.environ.get("SURF_POKE_TIMEOUT_SECONDS", str(6 * 60 * 60))),
        mode="reschedule",
    )

    @task
    def extract_to_csv(batch: dict[str, Any]) -> dict[str, Any]:
        raw_keys = [str(k) for k in (batch.get("raw_keys") or [])]
        ts = str(batch.get("timestamp") or "").strip()
        if not raw_keys or not ts:
            raise AirflowException(f"Invalid batch payload: {batch}")

        # Validate the timestamp early.
        _parse_timestamp_utc(ts)
        # Per requirement: set retrieved_at_utc equal to the timestamp in the filename.
        retrieved_at_utc = ts

        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)

        rows: list[dict[str, Any]] = []
        for raw_key in raw_keys:
            doc = json.loads(s3.read_key(key=raw_key, bucket_name=S3_BUCKET))
            row = _extract_fields(doc)
            row["retrieved_at_utc"] = retrieved_at_utc
            rows.append(row)

        out_key = f"{CSV_PREFIX}/{ts}.csv.ready"

        buf = io.StringIO()
        writer = csv.DictWriter(
            buf,
            fieldnames=[
                "point_of_interest",
                "retrieved_at_utc",
                "temperature_2m",
                "wind_speed_10m",
                "wave_height",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

#        S3_BUCKET = "sfquickstarts"
#        out_key = "tasty-bytes-builder-education/raw_pos/surfing" + out_key

        s3.load_string(
            string_data=buf.getvalue(),
            key=out_key,
            bucket_name=S3_BUCKET,
            replace=True,
        )

        return {"timestamp": ts, "raw_keys": raw_keys, "csv_key": out_key}

    @task
    def mark_raw_processed(payload: dict[str, Any]) -> dict[str, Any]:
        raw_keys = [str(k) for k in (payload.get("raw_keys") or [])]
        moved: list[str] = []

        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
        for raw_key in raw_keys:
            if raw_key.endswith(".ready"):
                processed_key = raw_key[: -len(".ready")] + ".processed"
            else:
                processed_key = raw_key + ".processed"

            s3.copy_object(
                source_bucket_key=raw_key,
                dest_bucket_key=processed_key,
                source_bucket_name=S3_BUCKET,
                dest_bucket_name=S3_BUCKET,
            )
            s3.delete_objects(bucket=S3_BUCKET, keys=raw_key)
            moved.append(processed_key)

        payload = dict(payload)
        payload["processed_raw_keys"] = moved
        return payload

    @task
    def delete_old_raw_files() -> int:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        deleted = 0

        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
        keys = s3.list_keys(bucket_name=S3_BUCKET, prefix=RAW_PREFIX.strip("/") + "/") or []

        to_delete: list[str] = []
        for key in keys:
            ts = _extract_timestamp_token(key)
            if not ts:
                continue

            try:
                file_dt = _parse_timestamp_utc(ts)
            except Exception:
                continue

            if file_dt < cutoff:
                to_delete.append(key)

        # Delete in chunks for safety (S3 API limit is 1000 objects/request).
        for i in range(0, len(to_delete), 1000):
            chunk = to_delete[i : i + 1000]
            try:
                s3.delete_objects(bucket=S3_BUCKET, keys=chunk)
                deleted += len(chunk)
            except Exception as e:
                raise AirflowException(f"Failed deleting old raw keys (sample={chunk[:3]})") from e

        return deleted

    extracted = extract_to_csv(poll_for_ready_batch.output)
    processed = mark_raw_processed(extracted)
    cleaned = delete_old_raw_files()

    poll_for_ready_batch >> extracted >> processed >> cleaned


surf_data_raw_to_csv()
