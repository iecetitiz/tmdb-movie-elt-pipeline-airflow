import argparse
import os
import sys
from io import BytesIO, StringIO
import boto3
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

print("SCRIPT STARTED")

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("-buc", "--bucket", required=True)
    parser.add_argument("-k", "--key", required=True)
    # File URL
    parser.add_argument("-i", "--input_file", required=True, help="GitHub Raw CSV URL")
    parser.add_argument("-ofp", "--overwrite", default="True")
    parser.add_argument("-z", "--chunk_size", type=int, default=500)
    parser.add_argument("-b", "--buffer_ratio", type=float, default=0.1)

    return parser.parse_args()


def main():
    args = parse_args()

    # Initialize S3 client (MinIO)
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        endpoint_url=os.getenv("MINIO_ENDPOINT")
    )

    # ---------------------------------------------------------
    # UPDATE: Fetch Data from GitHub (Requests + StringIO)
    # ---------------------------------------------------------
    github_url = args.input_file
    print(f"Fetching data from GitHub: {github_url}")

    try:
        response = requests.get(github_url)
        # Check if the request was successful (200 OK)
        if response.status_code == 200:
            print("✅ Data downloaded successfully from GitHub.")
            
            # Use StringIO to read the string data as if it were a file for pandas
            csv_data = StringIO(response.text)
            df = pd.read_csv(csv_data)
        else:
            print(f"❌ Error fetching data! Status Code: {response.status_code}")
            print(f"Response: {response.text[:200]}") # Show the beginning of the error response
            sys.exit(1) # Exit with error status
            
    except Exception as e:
        print(f"❌ Critical Error: {e}")
        sys.exit(1)

    # ---------------------------------------------------------
    
    chunk_size = args.chunk_size
    total_rows = len(df)

    print(f"Total rows: {total_rows}")
    print(f"Chunk size: {chunk_size}")

    # Split into chunks and write to S3
    for i in range(0, total_rows, chunk_size):
        chunk_df = df.iloc[i:i + chunk_size]

        buffer = BytesIO()
        chunk_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        object_key = f"{args.key}_{i//chunk_size}.parquet"

        s3.put_object(
            Bucket=args.bucket,
            Key=object_key,
            Body=buffer.getvalue()
        )

        print(f"Uploaded: s3://{args.bucket}/{object_key}")

    print("✅ Data ingestion completed.")


if __name__ == "__main__":
    main()