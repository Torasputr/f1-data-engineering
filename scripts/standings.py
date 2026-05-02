from google.cloud import storage
from dotenv import load_dotenv
import os
import pandas as pd
from io import BytesIO

load_dotenv()

GCS_BUCKET = os.getenv("GCS_BUCKET", "").strip()
SEASON = int(os.getenv("SEASON", ""))

GOLD_PATH = f"gold/season={SEASON}"

def main():
    print("[INFO] Init GCP")
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    blobs = [
        b for b in client.list_blobs(GCS_BUCKET, prefix=GOLD_PATH)
        if b.name.endswith("report.parquet")
    ]

    cols = ["Driver", "DriverNumber", "Team", "season", "Points"]

    parts = []

    for blob in blobs:
        df = pd.read_parquet(BytesIO(blob.download_as_bytes()))
        parts.append(df[cols].copy())

    print("[INFO] Concatting all gold round data")
    all_rounds = pd.concat(parts, ignore_index=True)

    print("[INFO] Making Driver Standings")
    standings = (
        all_rounds.groupby("DriverNumber", as_index=False)
        .agg(
            Points = ("Points", "sum"),
            Driver = ("Driver", "first"),
            Team = ("Team", "first"),
            season = ("season", "first"),
        )
        .sort_values("Points", ascending=False)
        .reset_index(drop=True)
    )

    stan_buf = BytesIO()
    standings.to_parquet(stan_buf, index=False)
    stan_buf.seek(0)

    print("[INFO] Uploading to GCP")
    bucket.blob(f"{GOLD_PATH}/standings.parquet").upload_from_file(stan_buf)

    print("[INFO] Making Team Standings")
    team_standings = (
        standings.groupby("Team", as_index=False)
        .agg(
            Points = ("Points", "sum"),
            Season = ("season", "first"),
            
        )
        .sort_values("Points", ascending=False)
        .reset_index(drop=True)
    )

    team_buf = BytesIO()
    team_standings.to_parquet(team_buf, index=False)
    team_buf.seek(0)
    print("[INFO] Uploading to GCP")
    bucket.blob(f"{GOLD_PATH}/team_standings.parquet").upload_from_file(team_buf)
    print("[INFO] Process Completed")

if __name__ == "__main__":
    main()