# Capital One Marketing Model - EC2 + Airflow

This repository is the EC2 execution version of the marketing-model pipeline.

## Architecture

GitHub -> GitHub Actions -> EC2 -> Airflow -> Python/PySpark jobs -> S3

The project intentionally does **not** build Docker images and does **not** use ECR for scoring.

Pipeline:

1. `01_snowflake_to_s3` - extracts Snowflake data and writes raw Parquet to S3.
2. `02_preprocessing` - reads raw S3 data, performs PySpark feature engineering, and writes processed Parquet.
3. `03_batch_scoring` - reads processed Parquet, runs the scoring logic, and writes scores and metrics to S3.

DAG 1 triggers DAG 2 after successful completion. DAG 2 triggers DAG 3 after successful completion.

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

Airflow DAGs are also copied to the configured Airflow DAG directory. The current workflow uses:

```text
/opt/airflow/dags/
```

If your EC2 Airflow installation uses another DAG directory, update `deploy-to-ec2.yml`.

## GitHub Actions

The workflow:

- runs Python syntax validation
- runs unit/smoke tests
- assumes the AWS deployment role through GitHub OIDC
- uses AWS Systems Manager (SSM) to deploy to EC2
- clones/pulls the repository on EC2
- creates/updates the EC2 virtual environment
- installs application dependencies
- copies the DAGs into Airflow
- does not build Docker
- does not push to ECR

Required GitHub repository variable:

```text
AWS_DEPLOY_ROLE_ARN
```

The AWS role must allow the GitHub OIDC principal to assume it and must allow SSM commands against the EC2 instance.

## EC2 prerequisites

The EC2 instance should have:

- Python 3.11
- Git
- AWS CLI
- AWS Systems Manager Agent
- IAM instance profile with access to required S3 resources
- network access to Snowflake
- enough CPU/memory/storage for PySpark and scoring
- Airflow installed and running

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

Prefer an EC2 secret-management mechanism such as AWS Secrets Manager/SSM Parameter Store and load the values into the runtime environment.

## AWS permissions

The EC2 IAM role needs the S3 permissions required for:

- reading raw/processed input
- writing raw/processed/output data
- listing relevant prefixes

If MLflow is enabled, the EC2 role also needs the permissions required by the SageMaker-managed MLflow tracking server.

## Manual deployment test

On EC2:

```bash
cd /opt/capitalone_marketing_model

git pull origin main

.venv/bin/pip install -r requirements.txt

python -m compileall -q jobs scoring airflow

pytest -q
```

Test preprocessing:

```bash
.venv/bin/python jobs/preprocessing.py --run-date 2026-10-07
```

Test scoring:

```bash
.venv/bin/python jobs/batch_score.py \
  --run-date 2026-10-07 \
  --input-s3 s3://bank-marketing-ml-poc-vineet/processed/marketing_features/2026-10-07/ \
  --output-s3 s3://bank-marketing-ml-poc-vineet/output/model_v1/2026-10-07/ \
  --metrics-s3 s3://bank-marketing-ml-poc-vineet/output/model_v1/2026-10-07/metrics.json
```

## Important

The original Glue scripts used `GlueContext`, `Job`, and `getResolvedOptions`. Those are Glue-runtime APIs and are not used by the new EC2 jobs.

The EC2 versions use:

- Snowflake Connector for Python
- boto3
- native PySpark
- pandas/pyarrow for batch scoring
- the existing scoring implementation

The scoring implementation itself is copied from the original repository.

## Scheduling

DAG 1 currently runs every Monday at 2:00 PM Asia/Kolkata:

```text
0 14 * * 1
```

DAG 2 and DAG 3 are externally triggered by their upstream DAGs.

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
  +--> S3 -> PySpark preprocessing -> S3
  |
  +--> S3 -> scoring -> S3
```
