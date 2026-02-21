import os
from pathlib import Path
from io import BytesIO
from typing import List


import boto3 
import pandas as pd


def to_snake_case(col: str) -> str:
    return col.strip().lower().replace(" ", "_").replace("-", "_")

def clean_generic(df:pd.DataFrame)-> pd.DataFrame:
    df = df.copy()
    df.columns = [to_snake_case(col) for col in df.columns]
    df = df.drop_duplicates()

    object_cols = df.select_dtypes(include=["object"]).columns
    for col in object_cols:
        df[col] = df[col].where(df[col].isna(),df[col].astype(str).str.strip())
    
    return df

def transform_orders(df:pd.DataFrame)-> pd.DataFrame:
    df = df.copy()
    date_cols:List[str] = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if "order_status" in df.columns and "order_delivered_date" in df.columns:
        anomaly_count = ((df["order_status"] == "delivered") & df["order_delivered_date"].isna()).sum()
        if anomaly_count > 0:
            print(f"{anomaly_count} delivered withou delivered date.")
        
    return df


def read_csv_from_s3(s3_client, bucket:str, key:str)-> pd.DataFrame:
    obj = s3_client.get_object(Bucket = bucket, Key = key)
    return pd.read_csv(obj["Body"])

def write_parquet_to_s3(s3_client, bucket:str,key:str, df:pd.DataFrame)->None:
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())

def main()-> None:
    bucket_name = os.getenv("S3_BUCKET_NAME")
    run_date = os.getenv("RUN_DATE","2026-02-19")

    raw_prefix = f"raw/{run_date}"
    staging_prefix = f"staging/{run_date}"
    s3 = boto3.client("s3")
    
    resp = s3.list_objects_v2(Bucket=bucket_name, Prefix=raw_prefix)
    contents = resp.get("Contents", [])
    csv_keys = [c["Key"] for c in contents if c["Key"].endswith(".csv")]
    if not csv_keys:
        raise RuntimeError(f"No CSV files found in s3://{bucket_name}/{raw_prefix}")
    
    for key in sorted(csv_keys):
        filename = Path(key).name
        table_name = filename.replace(".csv", "")
        staging_key = f"{staging_prefix}/{table_name}.parquet"

        print(f"Processing {key} -> {staging_key}")
        df =read_csv_from_s3(s3, bucket_name, key)

        df = clean_generic(df)
        
        if filename == "olist_orders_dataset.csv":
            df = transform_orders(df)

        write_parquet_to_s3(s3, bucket_name, staging_key, df)
        print(f"Finished processing {key} -> {staging_key}")

if __name__ == "__main__":
    main()
