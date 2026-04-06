"""## S3 upload example DAG

Creates/overwrites an object in S3 using the AWS provider's
`S3CreateObjectOperator`.
"""

from airflow.sdk import dag
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from pendulum import datetime


@dag(
    start_date=datetime(2025, 4, 22),
    schedule=None,
    catchup=False,
    default_args={"owner": "Astro", "retries": 0},
    tags=["example", "aws", "s3"],
    doc_md=__doc__,
)
def test_upload():
    S3CreateObjectOperator(
        task_id="test_upload",
        s3_bucket="astrolabs-634236767178-us-east-1-an",
        s3_key="data/my_file.txt",
        data="Hello from Astronomer!",
        aws_conn_id="astro_s3_conn",  # Match the Connection ID in Airflow
        replace=True,
    )


test_upload()

