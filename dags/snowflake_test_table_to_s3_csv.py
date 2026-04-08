"""## Snowflake → S3 CSV export DAG

Reads all rows from `TEST_DATABASE.TEST_SCHEMA.TEST_TABLE`, writes them to a CSV,
then uploads the file to S3 as `test_table.csv`.

Prereqs (Airflow Connections):
- Snowflake connection: `astrosnowflake`
- AWS connection for S3: `astro_s3_conn`

Note: For large tables, exporting the entire table into a single CSV can be
slow/expensive. Consider adding a WHERE clause or partitioning if needed.
"""

from __future__ import annotations

import csv
import os
from pathlib import Path

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import datetime


SNOWFLAKE_CONN_ID = os.environ.get("SNOWFLAKE_CONN_ID", "astrosnowflake")
AWS_CONN_ID = os.environ.get("AWS_CONN_ID", "astro_s3_conn")

S3_BUCKET = os.environ.get("S3_BUCKET", "astrolabs-634236767178-us-east-1-an")
S3_KEY = os.environ.get("S3_KEY", "test_table.csv")

SNOWFLAKE_TABLE_FQN = os.environ.get(
    "SNOWFLAKE_TABLE_FQN", "TEST_DATABASE.TEST_SCHEMA.TEST_TABLE"
)


@dag(
    start_date=datetime(2025, 4, 22),
    schedule=None,
    catchup=False,
    default_args={"owner": "Astro", "retries": 2},
    tags=["snowflake", "s3", "export"],
    doc_md=__doc__,
)
def snowflake_test_table_to_s3_csv():
    @task
    def export_table_to_s3() -> str:
        sql = f"SELECT * FROM {SNOWFLAKE_TABLE_FQN}"

        local_path = Path("/tmp") / "test_table.csv"

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()

        with conn.cursor() as cur:
            cur.execute(sql)

            column_names = [d[0] for d in (cur.description or [])]
            with local_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                if column_names:
                    writer.writerow(column_names)

                while True:
                    rows = cur.fetchmany(10_000)
                    if not rows:
                        break
                    writer.writerows(rows)

        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3.load_file(
            filename=str(local_path),
            key=S3_KEY,
            bucket_name=S3_BUCKET,
            replace=True,
        )

        return f"s3://{S3_BUCKET}/{S3_KEY}"

    export_table_to_s3()


snowflake_test_table_to_s3_csv()
