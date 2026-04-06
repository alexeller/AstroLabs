from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

upload_file = S3CreateObjectOperator(
    task_id="test_upload",
    s3_bucket="astrolabs-634236767178-us-east-1-an",
    s3_key="data/my_file.txt",
    data="Hello from Astronomer!",
    aws_conn_id="astro_s3_conn", # Match the ID from Step 1
)

