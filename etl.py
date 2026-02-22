import os
import pandas as pd
import requests
from datetime import datetime
from google.cloud import storage, bigquery

print("🚀 Starting Cloud ETL")

# Step 1: Extract
print("📥 Fetching data from API...")
response = requests.get("https://randomuser.me/api/?results=5")
users = response.json()["results"]
df = pd.json_normalize(users)[["name.first", "name.last", "email", "location.city", "dob.age"]]


# Step 2: Save local CSV
os.makedirs("output", exist_ok=True)
csv_path = "output/api_users.csv"
df['lod_timestamp'] = datetime.now()
df.columns = ["first_name", "last_name", "email", "city", "age", "lod_timestamp"]
df.to_csv(csv_path, index=False)


# Step 3: Upload to GCS
print("☁️ Uploading CSV to GCS...")
bucket_name = "etl-data-bucket-krishna"  # change this
destination_blob_name = "api_users.csv"

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(csv_path)
print("✅ Uploaded to GCS")

# Step 4: Load to BigQuery
print("📊 Loading to BigQuery...")
project_id = "etl-project-463010"  # change this
dataset_id = "user_etl"
table_id = "api_users_1"

bq_client = bigquery.Client()
table_ref = f"{project_id}.{dataset_id}.{table_id}"

job = bq_client.load_table_from_dataframe(df, table_ref,)
job.result()

print("✅ Data loaded to BigQuery")
print("🏁 Cloud ETL Finished.")
