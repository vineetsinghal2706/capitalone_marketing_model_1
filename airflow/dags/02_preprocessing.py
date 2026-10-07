import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


APP_DIR = "/opt/capitalone_marketing_model"
PYTHON = f"{APP_DIR}/.venv/bin/python"
local_tz = pendulum.timezone("Asia/Kolkata")


def run_preprocessing(**context):
    import subprocess

    run_date = context["dag_run"].conf.get("run_date", context["ds"])

    subprocess.run(
        [
            PYTHON,
            f"{APP_DIR}/jobs/preprocessing.py",
            "--run-date",
            run_date,
        ],
        check=True,
    )


with DAG(
    dag_id="02_preprocessing",
    start_date=pendulum.datetime(2026, 10, 1, tz=local_tz),
    schedule=None,
    catchup=False,
    tags=["marketing", "ec2", "pyspark", "preprocessing"],
) as dag:

    preprocess = PythonOperator(
        task_id="run_preprocessing",
        python_callable=run_preprocessing,
    )

    trigger_scoring = TriggerDagRunOperator(
        task_id="trigger_scoring",
        trigger_dag_id="03_batch_scoring",
        conf={"run_date": "{{ dag_run.conf.get('run_date', ds) }}"},
        wait_for_completion=False,
    )

    preprocess >> trigger_scoring
