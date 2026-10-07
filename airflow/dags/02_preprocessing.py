import pendulum

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


local_tz = pendulum.timezone("Asia/Kolkata")

GLUE_JOB_NAME = "marketing-model-preprocessing"


def run_date_from_conf():
    return "{{ dag_run.conf.get('run_date', ds) }}"


with DAG(
    dag_id="02_preprocessing",
    start_date=pendulum.datetime(2026, 10, 1, tz=local_tz),
    schedule=None,
    catchup=False,
    tags=["marketing", "glue", "preprocessing"],
) as dag:

    preprocess = GlueJobOperator(
        task_id="run_preprocessing_glue",
        job_name=GLUE_JOB_NAME,
        wait_for_completion=True,
        verbose=True,
        script_args={
            "--run_date": run_date_from_conf(),
        },
    )

    trigger_scoring = TriggerDagRunOperator(
        task_id="trigger_scoring",
        trigger_dag_id="03_batch_scoring",
        conf={"run_date": run_date_from_conf()},
        wait_for_completion=False,
    )

    preprocess >> trigger_scoring
