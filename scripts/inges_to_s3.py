import os
from datetime import date, datetime
from pathlib import Path

import boto3
from botocore.exceptions  import ClientError


def upload_file(s3_client, file_path:Path, bucket_name:str, object_name:str) ->None :
    try:
        s3_client.upload_file(str(file_path),bucket_name,object_name)
    except ClientError as e:
        raise RuntimeError(f"Failed to upload {file_path} -> s3://{bucket_name}/{object_name}") from e
    
def main() -> None:
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME environment variable is not set.")
    

    project_root = Path(__file__).resolve().parent.parent
    data_dir = project_root / "data"

    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found at {data_dir}")
    
    files_csv = sorted(data_dir.glob("*.csv"))

    if not files_csv:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")
    
    run_date = date.today().isoformat()
    raw_prefix = f"raw/{run_date}"

    s3 = boto3.client("s3")

    for file_path in files_csv:
        object_name = f"{raw_prefix}/{file_path.name}"
        print(f"Uploading {file_path} to s3://{bucket_name}/{object_name}")
        upload_file(s3, file_path, bucket_name, object_name)
    
    print("All files uploaded successfully.")


if __name__ == "__main__":
    main()