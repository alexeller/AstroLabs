"""## Surf data (weather + waves) to S3

Retrieves current weather (temperature + wind) and hourly wave height for a set
of points of interest, then writes raw JSON to:

    s3://<bucket>/data/surf_data_raw/<poi_slug>/<RUN_DATE>.json

Locations:
1) Tiverton, RI
2) Barrington Beach, RI
3) Easton Beach, Newport, RI
4) Narragansett Town Beach, Narragansett, RI
"""

from __future__ import annotations

from datetime import timedelta
import os
import re
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pendulum import datetime

from plugins.operators.open_meteo_surf_operator import OpenMeteoSurfDataToS3Operator


S3_BUCKET = os.environ.get("S3_BUCKET", "astrolabs-634236767178-us-east-1-an")
AWS_CONN_ID = os.environ.get("AWS_CONN_ID", "astro_s3_conn")

LOCATIONS: list[str] = [
    "Tiverton, RI",
    "Barrington Beach, RI",
    "Easton Beach, Newport, RI",
    "Narragansett Town Beach, Narragansett, RI",
]


def _poi_slug(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "poi"


@dag(
    start_date=datetime(2025, 4, 22),
    schedule='5 * * * *',
    catchup=False,
    default_args={"owner": "Astro", "retries": 2, 'retry_delay': timedelta(minutes=1)},
    tags=["s3", "weather", "marine", "surf"],
    doc_md=__doc__,
)
def surf_data_to_s3():
    @task
    def build_run_date() -> str:
        """Return an hourly run key derived from this DAG run's logical_date in UTC."""
        try:
            from airflow.decorators import get_current_context
        except Exception:  # pragma: no cover
            from airflow.operators.python import get_current_context

        context = get_current_context()
        logical_date_utc = context["logical_date"].in_timezone("UTC")
        return logical_date_utc.strftime("%Y%m%d%H")

    @task
    def validate_published(
        locations: list[str],
        run_date: str | None = None,
    ) -> None:
        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)

        if not run_date:
            raise AirflowException("Missing run_date")

        missing: list[str] = []
        empty: list[str] = []

        for location in locations:
            poi_slug = _poi_slug(location)
            key_base = f"data/surf_data_raw/{poi_slug}/{run_date}.json"
            key_ready = key_base + ".ready"

            # Operator writes `.json.ready`; accept `.json` as a backward-compatible fallback.
            obj: Any = s3.get_key(key=key_ready, bucket_name=S3_BUCKET)
            key = key_ready
            if obj is None:
                obj = s3.get_key(key=key_base, bucket_name=S3_BUCKET)
                key = key_base

            if obj is None:
                missing.append(f"s3://{S3_BUCKET}/{key_ready}")
                continue

            # `content_length` is present on boto3's ObjectSummary; be defensive.
            size = getattr(obj, "content_length", None)
            if size == 0:
                empty.append(f"s3://{S3_BUCKET}/{key}")

        if missing or empty:
            details = []
            if missing:
                details.append("Missing: " + ", ".join(missing))
            if empty:
                details.append("Empty: " + ", ".join(empty))
            raise AirflowException("Surf data validation failed. " + " | ".join(details))

    fork = EmptyOperator(task_id="fork")
    join = EmptyOperator(task_id="join")

    run_date = build_run_date()
    run_date >> fork

    tiverton = OpenMeteoSurfDataToS3Operator(
        task_id="surf_raw__tiverton_ri",
        point_of_interest="Tiverton, RI",
        s3_bucket=S3_BUCKET,
        s3_prefix="data/surf_data_raw",
        aws_conn_id=AWS_CONN_ID,
        run_date=run_date,
    )

    barrington = OpenMeteoSurfDataToS3Operator(
        task_id="surf_raw__barrington_beach_ri",
        point_of_interest="Barrington Beach, RI",
        s3_bucket=S3_BUCKET,
        s3_prefix="data/surf_data_raw",
        aws_conn_id=AWS_CONN_ID,
        run_date=run_date,
    )

    easton = OpenMeteoSurfDataToS3Operator(
        task_id="surf_raw__easton_beach_newport_ri",
        point_of_interest="Easton Beach, Newport, RI",
        s3_bucket=S3_BUCKET,
        s3_prefix="data/surf_data_raw",
        aws_conn_id=AWS_CONN_ID,
        run_date=run_date,
    )

    narragansett = OpenMeteoSurfDataToS3Operator(
        task_id="surf_raw__narragansett_town_beach_ri",
        point_of_interest="Narragansett Town Beach, Narragansett, RI",
        s3_bucket=S3_BUCKET,
        s3_prefix="data/surf_data_raw",
        aws_conn_id=AWS_CONN_ID,
        run_date=run_date,
        queue="dbt"
    )

    validate_task = validate_published(
        locations=LOCATIONS,
        run_date=run_date,
    )

    fork >> [tiverton, barrington, easton, narragansett] >> join >> validate_task


surf_data_to_s3()
