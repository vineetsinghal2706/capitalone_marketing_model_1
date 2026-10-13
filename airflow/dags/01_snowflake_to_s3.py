import pendulum

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


local_tz = pendulum.timezone("Asia/Kolkata")
GLUE_JOB_NAME = "snowflake-to-s3-bank-marketing"


with DAG(
    dag_id="01_snowflake_to_s3",
    start_date=pendulum.datetime(2026, 10, 1, tz=local_tz),
    schedule=None,
    catchup=False,
    tags=["marketing", "glue", "snowflake", "s3"],
) as dag:

    extract = GlueJobOperator(
        task_id="extract_snowflake_to_s3",
        job_name=GLUE_JOB_NAME,
        wait_for_completion=True,
        verbose=True,
        script_args={"--run_date": "{{ ds }}"},
    )

    trigger_preprocessing = TriggerDagRunOperator(
        task_id="trigger_preprocessing",
        trigger_dag_id="02_preprocessing",
        conf={"run_date": "{{ ds }}"},
        wait_for_completion=False,
    )

    extract >> trigger_preprocessing
