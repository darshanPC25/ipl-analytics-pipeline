import os 
import boto3
import kaggle
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
BRONZE_BUCKET = os.getenv("S3_BRONZE_BUCKET")
AWS_REGION = os.getenv("AWS_REGION")
TODAY = datetime.now()
PARTITION = f"year={TODAY.year}/month={TODAY.month:02d}/day={TODAY.day:02d}"

# Dataset to download from Kaggle
KAGGLE_DATASET = "chaitu20/ipl-dataset2008-2025"
# def download_from_kaggle():
#     """ Download IPL dataset from Kaggle """
#     print("Downloading dataset from Kaggle...")
#     os.makedirs("./data", exist_ok=True)
#     kaggle.api.authenticate()
#     kaggle.api.dataset_download_files(
#         KAGGLE_DATASET,
#         path="./data",
#         unzip=True
#     )

#     print("Download complete!")
#     print(f"Files downloaded: {os.listdir('./data')}")

def download_from_kaggle():
    """Download IPL dataset from Kaggle with proper checks"""
    print("Downloading dataset from Kaggle...")

    # Verify credentials exist
    if not os.getenv("KAGGLE_USERNAME") or not os.getenv("KAGGLE_KEY"):
        raise ValueError("Kaggle credentials missing in .env file!")

    # Set Kaggle credentials explicitly
    os.environ["KAGGLE_USERNAME"] = os.getenv("KAGGLE_USERNAME")
    os.environ["KAGGLE_KEY"] = os.getenv("KAGGLE_KEY")

    # Create data directory
    os.makedirs("./data", exist_ok=True)

    try:
        # Authenticate
        kaggle.api.authenticate()
        print("Kaggle authentication successful!")

        # Download dataset
        kaggle.api.dataset_download_files(
            KAGGLE_DATASET,
            path="./data",
            unzip=True
        )
        print("Download complete!")

        # Check 4 - Verify files actually downloaded
        files = os.listdir("./data")
        if not files:
            raise FileNotFoundError("No files found after download!")

        csv_files = [f for f in files if f.endswith(".csv")]
        if not csv_files:
            raise FileNotFoundError("No CSV files found after download!")

        print(f"Files downloaded: {csv_files}")
        return csv_files

    except Exception as e:
        print(f"Download failed: {str(e)}")
        raise

def upload_to_s3(file_name):
    """Upload file to S3 Bronze with partitioning"""
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

    local_path = f"./data/{file_name}"

    # Check if file exists
    if not os.path.exists(local_path):
        print(f"File not found: {local_path} — skipping")
        return
    
    table_name = file_name.replace(".csv", "")
    s3_key = f"ipl/{table_name}/{PARTITION}/{file_name}"

    print(f"Uploading {file_name} to s3://{BRONZE_BUCKET}/{s3_key}")
    s3.upload_file(local_path, BRONZE_BUCKET, s3_key)
    print(f"Uploaded {file_name} successfully!")

def get_csv_files():
    """Get all CSV files from data folder"""
    files = os.listdir("./data")
    csv_files = [f for f in files if f.endswith(".csv")]
    print(f"Found CSV files: {csv_files}")
    return csv_files

def main():
    print("=" * 50)
    print("IPL Analytics Pipeline - Bronze Ingestion")
    print("=" * 50)

    # Download from Kaggle
    download_from_kaggle()

    # Get CSV files
    csv_files = get_csv_files()

    if not csv_files:
        print("No CSV files found in data folder. Exiting.")
        return
    
    # Upload each CSV file to S3 Bronze
    for file in csv_files:
        upload_to_s3(file)
        print("=" * 50)

    print("Bronze Ingestion Complete!")
    print(f"S3 Bucket: {BRONZE_BUCKET}")
    print(f"Partition: {PARTITION}")
    print("=" * 50)

if __name__ == "__main__":
    main()