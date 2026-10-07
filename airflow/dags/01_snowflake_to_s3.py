import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


APP_DIR = "/opt/capitalone_marketing_model"
PYTHON = f"{APP_DIR}/.venv/bin/python"
local_tz = pendulum.timezone("Asia/Kolkata")


def run_snowflake_to_s3(**context):
    import subprocess

    run_date = context["ds"]

    subprocess.run(
        [
            PYTHON,
            f"{APP_DIR}/jobs/snowflake_to_s3.py",
            "--run-date",
            run_date,
        ],
        check=True,
    )


with DAG(
    dag_id="01_snowflake_to_s3",
    start_date=pendulum.datetime(2026, 10, 1, tz=local_tz),
    schedule="0 14 * * 1",
    catchup=False,
    tags=["marketing", "ec2", "snowflake", "s3"],
) as dag:

    extract = PythonOperator(
        task_id="extract_snowflake_to_s3",
        python_callable=run_snowflake_to_s3,
    )

    trigger_preprocessing = TriggerDagRunOperator(
        task_id="trigger_preprocessing",
        trigger_dag_id="02_preprocessing",
        conf={"run_date": "{{ ds }}"},
        wait_for_completion=False,
    )

    extract >> trigger_preprocessing
