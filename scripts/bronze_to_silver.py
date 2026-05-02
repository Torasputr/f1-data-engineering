from pathlib import Path
import pandas as pd
import re
import os
from dotenv import load_dotenv
from google.cloud import storage
from io import BytesIO

load_dotenv()

# CONFIG
GCS_BUCKET = os.getenv("GCS_BUCKET", "trial-f1").strip()
BRONZE_PREFIX = os.getenv("GCS_PREFIX", "").strip().strip("/")

SEASON = int(os.getenv("SEASON", "2026"))

SILVER_PREFIX = os.getenv("SILVER_PREFIX", "").strip().strip("/")
SILVER_FILENAME = os.getenv("SILVER_FILENAME", "").strip().strip("/")

client = storage.Client()
bucket = client.bucket(GCS_BUCKET)



# functions
def list_bronze_blobs_for_season(season):
    client = storage.Client()
    prefix = f"{BRONZE_PREFIX}/season={season}/"
    blobs = list(client.list_blobs(GCS_BUCKET, prefix=prefix))
    return sorted(
        [b for b in blobs if b.name.endswith("laps.parquet")], key=lambda x: x.name
    )


def read_parquet_blob(blob):
    raw = blob.download_as_bytes()
    return pd.read_parquet(BytesIO(raw))


def strip_string(column):
    print(f"Stripping string for {column.name}")
    column = column.str.strip()

    return column


def upper_string(column):
    print(f"Uppercasing string for {column.name}")
    column = column.str.upper()

    return column


def convert_to_int(column):
    print(f"Converting to int for {column.name}")
    column = pd.to_numeric(column, errors="coerce")

    return column


def clean_df_silver(df):
    print("[INFO] Dropping unneeded columns")
    df = df.drop(
        columns=[
            "FastF1Generated",
            "ingested_at_utc",
            "IsAccurate",
            "Sector1SessionTime",
            "Sector2SessionTime",
            "Sector3SessionTime",
            "LapStartTime",
            "LapStartDate",
            "Deleted",
            "DeletedReason",
        ]
    )

    print("[INFO] Drop duplicated rows")
    df = df.drop_duplicates(subset=["season", "round_number", "Driver", "LapNumber"])

    # DRIVER
    df["Driver"] = strip_string(df["Driver"])
    df["Driver"] = upper_string(df["Driver"])

    # DRIVER NUMBER
    df["DriverNumber"] = convert_to_int(df["DriverNumber"]).astype(int)

    # LAP TIME
    df["is_LapTime_not_na"] = df["LapTime"].notna()

    # LAP NUMBER
    df["LapNumber"] = convert_to_int(df["LapNumber"]).astype(int)

    # STINT
    df["Stint"] = convert_to_int(df["Stint"]).astype(int)

    # PIT OUT TIME
    df["is_pit_out_time_not_na"] = df["PitOutTime"].notna()

    # PIT IN TIME
    df["is_pit_in_time_not_na"] = df["PitInTime"].notna()

    # SECTORS
    df["is_s1_notna"] = df["Sector1Time"].notna()
    df["is_s2_notna"] = df["Sector2Time"].notna()
    df["is_s3_notna"] = df["Sector3Time"].notna()
    df["is_sector_complete"] = df["is_s1_notna"] & df["is_s2_notna"] & df["is_s3_notna"]

    # SPEED
    df["SpeedI1"] = convert_to_int(df["SpeedI1"])
    df["SpeedI2"] = convert_to_int(df["SpeedI2"])
    df["SpeedST"] = convert_to_int(df["SpeedST"])
    df["SpeedFL"] = convert_to_int(df["SpeedFL"])

    speed_cols = ["SpeedI1", "SpeedI2", "SpeedST", "SpeedFL"]
    df["is_speed_complete"] = df[speed_cols].notna().all(axis=1)

    # IS PERSONAL BEST
    df["IsPersonalBest"] = df["IsPersonalBest"].astype("boolean")

    # COMPOUND
    df["Compound"] = strip_string(df["Compound"])
    df["Compound"] = upper_string(df["Compound"])

    # TYRE LIFE
    df["TyreLife"] = convert_to_int(df["TyreLife"]).astype(int)

    # TEAM
    df["Team"] = strip_string(df["Team"])

    # TRACK STATUS
    df["TrackStatus"] = convert_to_int(df["TrackStatus"]).astype(int)
    df["is_green_flag"] = df["TrackStatus"].eq(1)

    # POSITION
    df["is_position_not_na"] = df["Position"].notna()

    # EVENT NAME
    df["event_name"] = strip_string(df["event_name"])
    df["event_name"] = upper_string(df["event_name"])

    # SESSION TYPE
    df["session_type"] = strip_string(df["session_type"])
    df["session_type"] = upper_string(df["session_type"])

    print("Data cleaning done")
    return df

def upload_parquet_df(bucket, df, key):
    buf = BytesIO()
    print(f"[INFO] Uploading parquet to {key}")
    df.to_parquet(buf, index=False)
    buf.seek(0)
    bucket.blob(key).upload_from_file(buf, content_type="application/octet-stream")
    print(f"[INFO] Uploaded parquet to {key}")


def main():
    print(f"[INFO] Running bronze->silver for season={SEASON}")

    print(f"[INFO] Checking blob list")
    blobs = list_bronze_blobs_for_season(SEASON)
    if not blobs:
        raise ValueError(f"No bronze files found")
    print(f"[INFO] Found bronze files: {len(blobs)}")
    dfs = []
    for b in blobs:
        print(f" - {b.name}")
        d = read_parquet_blob(b)
        dfs.append(d)

    df_silver = pd.concat(dfs, ignore_index=True)
    print(f"[INFO] Bronze shape: {df_silver.shape}")
    print(f"[INFO] Cleaning silver dataframe")
    df_silver = clean_df_silver(df_silver)

    silver_key = f"{SILVER_PREFIX}/season={SEASON}/{SILVER_FILENAME}"
    upload_parquet_df(bucket, df_silver, silver_key)

if __name__ == "__main__":
    main()
