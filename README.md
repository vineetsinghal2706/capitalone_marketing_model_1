# Capital One Marketing Model - EC2 + Airflow + Glue

This repository is the EC2 execution version of the marketing-model pipeline.

## Architecture

GitHub -> GitHub Actions -> EC2 -> Airflow -> Snowflake/S3/Glue -> EC2 scoring -> S3

The project intentionally does **not** build Docker images and does **not** use ECR for scoring.

Pipeline:

1. `01_snowflake_to_s3` - extracts Snowflake data and writes raw Parquet to S3 from EC2.
2. `02_preprocessing` - triggers the existing AWS Glue job `marketing-model-preprocessing`. The Glue job performs PySpark feature engineering and writes processed Parquet to S3.
3. `03_batch_scoring` - runs on EC2, reads processed Parquet, runs the scoring logic, and writes scores and metrics to S3.

DAG 1 triggers DAG 2 after successful completion. DAG 2 waits for the Glue preprocessing job to finish successfully before triggering DAG 3.

## EC2 directory

The deployment workflow creates:

```text
/opt/capitalone_marketing_model/
├── .venv/
├── airflow/
│   └── dags/
├── jobs/
│   ├── snowflake_to_s3.py
│   ├── preprocessing.py
│   └── batch_score.py
├── scoring/
│   └── score_if_else.py
├── tests/
├── requirements.txt
└── README.md
```

Airflow DAGs are also copied to:

```text
/opt/airflow/dags/
```

If your Airflow installation uses another DAG directory, update `deploy-to-ec2.yml`.

## GitHub Actions

The workflow:

- runs Python syntax validation
- runs unit/smoke tests
- assumes the AWS deployment role through GitHub OIDC
- uses AWS Systems Manager (SSM) to deploy to EC2
- explicitly syncs EC2 to `capitalone_marketing_model_1`
- creates/reuses the EC2 virtual environment
- installs application runtime dependencies without PySpark
- copies the DAGs into Airflow
- waits for the deployment SSM command before verification
- does not build Docker
- does not push to ECR

Required GitHub repository variable:

```text
AWS_DEPLOY_ROLE_ARN
```

## EC2 prerequisites

The EC2 instance should have:

- Python
- Git
- AWS CLI
- AWS Systems Manager Agent
- IAM instance profile with access to required S3 resources
- network access to Snowflake
- enough storage for the application runtime and Airflow
- Airflow installed and running

PySpark is **not required on the EC2 instance for the Airflow pipeline** because preprocessing runs in AWS Glue.

## Snowflake environment variables

The EC2 runtime needs these environment variables for the Snowflake extraction job:

```text
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
SNOWFLAKE_SCHEMA
SNOWFLAKE_ROLE   # optional
```

Do not put the password in GitHub source code.

Prefer AWS Secrets Manager or SSM Parameter Store and load the values into the runtime environment.

## AWS permissions

The EC2 IAM role needs the S3 permissions required for:

- reading processed/output data
- writing raw/output data
- listing relevant prefixes

The Airflow AWS connection/role must be able to:

- start and inspect the Glue job `marketing-model-preprocessing`
- read the Glue job status until completion
- trigger the downstream scoring DAG

If MLflow is enabled, the EC2 role also needs the permissions required by the MLflow tracking configuration.

## Manual deployment test

On EC2:

```bash
cd /opt/capitalone_marketing_model

git remote -v
git pull origin main

.venv/bin/pip install -r requirements.txt

python -m compileall -q jobs scoring airflow
pytest -q
```

Test Snowflake extraction:

```bash
.venv/bin/python jobs/snowflake_to_s3.py --run-date 2026-10-07
```

Preprocessing is normally executed through Airflow -> Glue. The standalone `jobs/preprocessing.py` is retained as a native PySpark reference implementation and is not installed as part of the EC2 runtime environment.

Test scoring:

```bash
.venv/bin/python jobs/batch_score.py \
  --run-date 2026-10-07 \
  --input-s3 s3://bank-marketing-ml-poc-vineet/processed/marketing_features/2026-10-07/ \
  --output-s3 s3://bank-marketing-ml-poc-vineet/output/model_v1/2026-10-07/ \
  --metrics-s3 s3://bank-marketing-ml-poc-vineet/output/model_v1/2026-10-07/metrics.json
```

## Scheduling

DAG 1 currently runs every Monday at 2:00 PM Asia/Kolkata:

```text
0 14 * * 1
```

DAG 2 and DAG 3 are externally triggered by their upstream DAGs.

## Glue preprocessing

Airflow DAG 2 invokes the existing Glue job:

```text
marketing-model-preprocessing
```

The Glue job is expected to receive the run date as:

```text
--run_date
```

It must write its processed output to the S3 location consumed by DAG 3:

```text
s3://bank-marketing-ml-poc-vineet/processed/marketing_features/{run_date}/
```

Because DAG 2 uses `wait_for_completion=True`, DAG 3 starts only after Glue preprocessing completes successfully.

## No Docker / ECR

This repository intentionally has no Docker deployment path.

The execution chain is:

```text
GitHub
  |
  v
GitHub Actions
  |
  v
EC2
  |
  v
Airflow
  |
  +--> Snowflake -> S3
  |
  +--> AWS Glue -> PySpark preprocessing -> S3
  |
  +--> EC2 -> batch scoring -> S3
```
