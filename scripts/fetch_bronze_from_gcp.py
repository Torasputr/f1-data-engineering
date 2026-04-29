import os
from google.cloud import storage

BUCKET = "trial-f1"
PREFIX = "bronze/laps/season=2026/"
LOCAL_ROOT = r"../data/bronze/laps/season=2026/"

os.makedirs(LOCAL_ROOT, exist_ok=True)

client = storage.Client()
bucket = client.bucket(BUCKET)

for blob in bucket.list_blobs(prefix=PREFIX):
    rel = blob.name[len(PREFIX):]
    print(f"rel = {rel}")
    local_path = os.path.join(LOCAL_ROOT, rel)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    blob.download_to_filename(local_path)
    print(f"Downloaded {blob.name} to {local_path}")