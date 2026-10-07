import argparse
import os
import snowflake.connector
import pandas as pd


BUCKET = "bank-marketing-ml-poc-vineet"


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True)
    return parser.parse_args()


def get_connection():
    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
    ]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError(
            "Missing Snowflake environment variables: "
            + ", ".join(missing)
        )

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def main():
    args = get_args()
    output_path = f"s3://{BUCKET}/raw/{args.run_date}/"

    query = """
        SELECT *
        FROM INGEST_DATA.PUBLIC.CUSTOMERS
        WHERE CUSTOMER_ID IS NOT NULL
    """

    print("Starting Snowflake read...")
    conn = get_connection()

    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    print(f"Total records extracted: {len(df):,}")
    print(f"Writing data to S3: {output_path}")

    # Pandas requires s3fs for direct S3 writes. To avoid that dependency,
    # write a temporary parquet file and upload it with boto3.
    import boto3

    local_file = f"/tmp/snowflake_{args.run_date}.parquet"
    df.to_parquet(local_file, index=False)

    bucket = BUCKET
    key = f"raw/{args.run_date}/part-00000.parquet"

    boto3.client("s3").upload_file(local_file, bucket, key)
    os.remove(local_file)

    print(f"Data successfully written to s3://{bucket}/{key}")


if __name__ == "__main__":
    main()
