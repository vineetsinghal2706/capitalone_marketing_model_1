import pendulum

from airflow import DAG
from airflow.providers.amazon.aws.operators.ssm import SsmRunCommandOperator


local_tz = pendulum.timezone("Asia/Kolkata")
AWS_REGION = "us-east-1"
EC2_INSTANCE_ID = "i-06ea3e388448b991c"
ECR_REGISTRY = "783495713343.dkr.ecr.us-east-1.amazonaws.com"
ECR_IMAGE = f"{ECR_REGISTRY}/bank-marketing-batch-scoring:latest"
BUCKET = "bank-marketing-ml-poc-vineet"
MLFLOW_TRACKING_URI = (
    "arn:aws:sagemaker:us-east-1:783495713343:"
    "mlflow-tracking-server/marketing-mlflow-server"
)
RUN_DATE = "{{ dag_run.conf.get('run_date', ds) }}"


with DAG(
    dag_id="03_batch_scoring",
    start_date=pendulum.datetime(2026, 10, 1, tz=local_tz),
    schedule=None,
    catchup=False,
    tags=["marketing", "ec2", "docker", "ecr", "scoring", "mlflow"],
) as dag:

    score = SsmRunCommandOperator(
        task_id="run_batch_scoring",
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        document_name="AWS-RunShellScript",
        wait_for_completion=True,
        run_command_kwargs={
            "InstanceIds": [EC2_INSTANCE_ID],
            "Parameters": {
                "commands": [
                    "set -euo pipefail",
                    (
                        f"aws ecr get-login-password --region {AWS_REGION} | "
                        "docker login --username AWS --password-stdin "
                        f"{ECR_REGISTRY}"
                    ),
                    f"docker pull {ECR_IMAGE}",
                    (
                        "docker run --rm "
                        f"-e AWS_DEFAULT_REGION={AWS_REGION} "
                        f'-e MLFLOW_TRACKING_URI="{MLFLOW_TRACKING_URI}" '
                        f"{ECR_IMAGE} "
                        f"--run-date '{RUN_DATE}' "
                        f'--input-s3 "s3://{BUCKET}/processed/marketing_features/{RUN_DATE}/" '
                        f'--output-s3 "s3://{BUCKET}/output/model_v1/{RUN_DATE}/" '
                        f'--metrics-s3 "s3://{BUCKET}/output/model_v1/{RUN_DATE}/metrics.json"'
                    ),
                ]
            },
        },
    )
