import pendulum

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


APP_DIR = "/opt/capitalone_marketing_model"
local_tz = pendulum.timezone("Asia/Kolkata")


with DAG(
    dag_id="01_snowflake_to_s3",
    start_date=pendulum.datetime(2026, 10, 1, tz=local_tz),
    schedule=None,
    catchup=False,
    tags=["marketing", "ec2", "snowflake", "s3"],
) as dag:

    extract = SSHOperator(
        task_id="extract_snowflake_to_s3",
        ssh_conn_id="ec2_ssh",
        command=(
            f"cd {APP_DIR} && "
            f".venv/bin/python jobs/snowflake_to_s3.py "
            "--run-date {{ ds }}"
        ),
        cmd_timeout=3600,
    )

    trigger_preprocessing = TriggerDagRunOperator(
        task_id="trigger_preprocessing",
        trigger_dag_id="02_preprocessing",
        conf={"run_date": "{{ ds }}"},
        wait_for_completion=False,
    )

    extract >> trigger_preprocessing
