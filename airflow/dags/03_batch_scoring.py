import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


APP_DIR = "/opt/capitalone_marketing_model"
PYTHON = f"{APP_DIR}/.venv/bin/python"
BUCKET = "bank-marketing-ml-poc-vineet"
local_tz = pendulum.timezone("Asia/Kolkata")


def run_scoring(**context):
    import subprocess

    run_date = context["dag_run"].conf.get("run_date", context["ds"])

    input_s3 = f"s3://{BUCKET}/processed/marketing_features/{run_date}/"
    output_s3 = f"s3://{BUCKET}/output/model_v1/{run_date}/"
    metrics_s3 = f"{output_s3}metrics.json"

    subprocess.run(
        [
            PYTHON,
            f"{APP_DIR}/jobs/batch_score.py",
            "--run-date",
            run_date,
            "--input-s3",
            input_s3,
            "--output-s3",
            output_s3,
            "--metrics-s3",
            metrics_s3,
        ],
        check=True,
    )


with DAG(
    dag_id="03_batch_scoring",
    start_date=pendulum.datetime(2026, 10, 1, tz=local_tz),
    schedule=None,
    catchup=False,
    tags=["marketing", "ec2", "scoring", "mlflow"],
) as dag:

    score = PythonOperator(
        task_id="run_batch_scoring",
        python_callable=run_scoring,
    )
