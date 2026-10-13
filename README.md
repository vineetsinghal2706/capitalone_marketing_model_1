# Capital One Marketing Model — Local Airflow, AWS Glue, EC2 Scoring

This repository orchestrates the marketing-model batch pipeline. Airflow may run locally; Snowflake extraction and preprocessing run in AWS Glue, while batch scoring runs on EC2. The scoring deployment does not require Docker or ECR.

## Architecture

```text
Local Airflow (manual trigger; no schedule)
        |
        v
DAG 1: Glue job snowflake-to-s3-bank-marketing
        |  waits for successful completion
        v
DAG 2: Glue job marketing-model-preprocessing
        |  waits for successful completion
        v
DAG 3: SSH to EC2 and run jobs/batch_score.py
        |
        v
S3: scores and metrics
```

## DAG behavior

- `airflow/dags/01_snowflake_to_s3.py`: launches the existing Glue job `snowflake-to-s3-bank-marketing`, passes `--run_date`, and waits for it to finish. Only after success does it trigger DAG 2.
- `airflow/dags/02_preprocessing.py`: launches `marketing-model-preprocessing`, passes `--run_date`, waits for Glue to finish, and then triggers DAG 3.
- `airflow/dags/03_batch_scoring.py`: connects to EC2 over SSH using the Airflow connection `ec2_ssh` and runs the batch scoring Python script remotely.
- All three DAGs use `schedule=None`; trigger DAG 1 manually in the local Airflow UI.

## Configure local Airflow

Install provider packages compatible with your installed Airflow version:

```bash
pip install apache-airflow-providers-amazon apache-airflow-providers-ssh
```

If Airflow runs in Docker, install the providers in the Airflow image/container and persist them through your image or dependency configuration. Ensure the relevant DAG files are available in the local Airflow DAGs folder or mounted into it.

### AWS connection

Local Airflow needs AWS credentials to start and monitor Glue jobs. Configure an Airflow AWS connection (commonly `aws_default`) or provide AWS credentials through the environment/standard AWS credential chain available to the Airflow runtime. Grant permission to start and monitor these two Glue jobs:

- `snowflake-to-s3-bank-marketing`
- `marketing-model-preprocessing`

The Glue jobs themselves must have the appropriate Glue execution role, Snowflake connection/credentials, and S3 permissions. Keep Snowflake passwords in a managed secret store, not in source code.

### EC2 SSH connection

Create an Airflow connection under **Admin → Connections** with:

- **Connection Id:** `ec2_ssh`
- **Connection Type:** `SSH`
- **Host:** EC2 public IP/DNS, or a private endpoint reachable from the Airflow host
- **Port:** `22`
- **Username:** commonly `ec2-user` (Amazon Linux) or `ubuntu` (Ubuntu)
- **Authentication:** private key for the EC2 instance

The SSH private key must be available to the Airflow runtime. For Docker-based local Airflow, mount it securely into the container and use the container-visible path in the connection's `key_file` extra. Do not commit private keys to GitHub. The EC2 security group/network must allow SSH from the machine running Airflow.

The EC2 deployment must have this application path and Python environment:

```text
/opt/capitalone_marketing_model/
├── .venv/bin/python
├── jobs/batch_score.py
└── scoring/
```

The EC2 instance role or its runtime credentials must be allowed to read processed input data from S3 and write scoring output and metrics to S3.

## S3 paths used by batch scoring

Input:

```text
s3://bank-marketing-ml-poc-vineet/processed/marketing_features/{run_date}/
```

Scores and metrics:

```text
s3://bank-marketing-ml-poc-vineet/output/model_v1/{run_date}/
```

DAG 3 passes the run date from the Airflow DAG-run configuration (falling back to the logical date). Ensure the Glue preprocessing job writes to the matching input prefix.

## Deployment and tests

GitHub Actions can deploy the code to EC2 using the configured SSM/OIDC setup. After pulling the repository on EC2, the basic application checks are:

```bash
cd /opt/capitalone_marketing_model
.venv/bin/python -m compileall -q jobs scoring airflow
pytest -q
```

DAG files executed by **local Airflow** must also be updated in its local DAG folder; updating GitHub alone does not guarantee that the local scheduler has loaded the new versions.

## Important distinctions

- Glue Job 1 owns Snowflake extraction and writing raw data to S3; it must have a working Glue Snowflake connection and correct `--run_date` argument handling.
- Glue Job 2 owns preprocessing and writing feature data to S3.
- EC2 owns only the batch scoring execution in this flow.
- Airflow is the orchestrator and does not need a local Snowflake connector or access to EC2's filesystem. It needs AWS access for Glue and SSH connectivity for EC2.
