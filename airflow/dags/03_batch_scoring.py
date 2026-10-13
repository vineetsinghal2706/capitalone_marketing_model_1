import pendulum

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator


APP_DIR = "/opt/capitalone_marketing_model"
BUCKET = "bank-marketing-ml-poc-vineet"
local_tz = pendulum.timezone("Asia/Kolkata")


with DAG(
    dag_id="03_batch_scoring",
    start_date=pendulum.datetime(2026, 10, 1, tz=local_tz),
    schedule=None,
    catchup=False,
    tags=["marketing", "ec2", "scoring", "mlflow"],
) as dag:

    score = SSHOperator(
        task_id="run_batch_scoring",
        ssh_conn_id="ec2_ssh",
        command=(
            'RUN_DATE="{{ dag_run.conf.get(\'run_date\', ds) }}" && '
            f"cd {APP_DIR} && "
            ".venv/bin/python jobs/batch_score.py "
            '--run-date "$RUN_DATE" '
            f'--input-s3 "s3://{BUCKET}/processed/marketing_features/$RUN_DATE/" '
            f'--output-s3 "s3://{BUCKET}/output/model_v1/$RUN_DATE/" '
            f'--metrics-s3 "s3://{BUCKET}/output/model_v1/$RUN_DATE/metrics.json"'
        ),
        cmd_timeout=3600,
    )
