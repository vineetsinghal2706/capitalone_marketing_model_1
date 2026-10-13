import pendulum

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator


local_tz = pendulum.timezone("Asia/Kolkata")
APP_DIR = "/opt/capitalone_marketing_model"
AWS_REGION = "us-east-1"
BUCKET = "bank-marketing-ml-poc-vineet"
MLFLOW_TRACKING_URI = (
    "arn:aws:sagemaker:us-east-1:783495713343:"
    "mlflow-tracking-server/marketing-mlflow-server"
)


with DAG(
    dag_id="03_batch_scoring",
    start_date=pendulum.datetime(2026, 10, 1, tz=local_tz),
    schedule=None,
    catchup=False,
    tags=["marketing", "ec2", "ssh", "scoring", "mlflow"],
) as dag:

    score = SSHOperator(
        task_id="run_batch_scoring",
        ssh_conn_id="ec2_ssh",
        command=(
            "set -e && "
            f"cd {APP_DIR} && "
            f"export AWS_DEFAULT_REGION={AWS_REGION} && "
            f'export MLFLOW_TRACKING_URI="{MLFLOW_TRACKING_URI}" && '
            'RUN_DATE="{{ dag_run.conf.get(\'run_date\', ds) }}" && '
            ".venv/bin/python jobs/batch_score.py "
            '--run-date "$RUN_DATE" '
            f'--input-s3 "s3://{BUCKET}/processed/marketing_features/$RUN_DATE/" '
            f'--output-s3 "s3://{BUCKET}/output/model_v1/$RUN_DATE/" '
            f'--metrics-s3 "s3://{BUCKET}/output/model_v1/$RUN_DATE/metrics.json"'
        ),
        cmd_timeout=3600,
    )
