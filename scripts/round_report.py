from google.cloud import storage
from dotenv import load_dotenv
import os
from io import BytesIO
import pandas as pd
import numpy as np

load_dotenv()

# ENVIRONMENT
GCS_BUCKET = os.getenv("GCS_BUCKET", "").strip()
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "").strip().strip("/")
SEASON = int(os.getenv("SEASON", ""))
SILVER_FILENAME = os.getenv("SILVER_FILENAME", "").strip()

SILVER_FILEPATH = f"{SILVER_PREFIX}/season={SEASON}/{SILVER_FILENAME}"

POINTS_BY_PLACE = {
    1: 25,
    2: 18,
    3: 15,
    4: 12,
    5: 10,
    6: 8,
    7: 6,
    8: 4,
    9: 2,
    10: 1,
}

print("[INFO] Initializing GCS")
client = storage.Client()
blob = client.bucket(GCS_BUCKET).blob(SILVER_FILEPATH)

print("[INFO] Initializing dataframe")
df = pd.read_parquet(BytesIO(blob.download_as_bytes()))
columns_to_drop = [
    "Stint",
    "PitOutTime",
    "PitInTime",
    "Sector1Time",
    "Sector2Time",
    "Sector3Time",
    "SpeedI1",
    "SpeedI2",
    "SpeedFL",
    "SpeedST",
    "Compound",
    "TyreLife",
    "FreshTyre",
    "TrackStatus",
    "session_type",
    "is_LapTime_not_na",
    "is_pit_out_time_not_na",
    "is_pit_in_time_not_na",
    "is_s1_notna",
    "is_s2_notna",
    "is_s3_notna",
    "is_sector_complete",
    "is_speed_complete",
    "is_green_flag",
    "is_position_not_na",
    "IsPersonalBest"
]

# FUNCTIONS
def filter_column_for_driver(keys, data):
    drivers = (
        data[keys].drop_duplicates(subset=keys).assign(
            Driver = lambda d: d["Driver"],
            DriverNumber = lambda d: d["DriverNumber"].astype(int),
            Team = lambda d: d["Team"],
        ).sort_values(by="DriverNumber", ignore_index=True, ascending=True)
    )
    return drivers

def insert_driver(overall, one):
    keys = ["Driver", "DriverNumber", "Team"]
    all_drivers = filter_column_for_driver(keys, overall)
    current_drivers = filter_column_for_driver(keys, one)

    miss = all_drivers.merge(current_drivers, on=keys, how="left", indicator=True)
    miss = miss.loc[miss["_merge"] == "left_only", keys].reset_index(drop=True)

    if miss.empty:
        print("[INFO] No missing drivers")
        return one.copy()
    
    stub = pd.DataFrame({c: np.nan for c in one.columns}, index=miss.index)
    print(f"Inserting Driver Number: {miss['DriverNumber'].values} to the dataframe")
    stub["DriverNumber"] = miss["DriverNumber"].values
    stub["Driver"] = miss["Driver"].values
    stub["Team"] = miss["Team"].values
    stub["season"] = one["season"].iloc[0]
    stub["round_number"] = one["round_number"].iloc[0]
    stub["event_name"] = one["event_name"].iloc[0]
    stub["LapNumber"] = 0

    return pd.concat([one.reset_index(drop=True), stub], ignore_index=True)

def assign_points(position):
    points = position.map(POINTS_BY_PLACE).fillna(0).astype(int)
    return points

print("[INFO] Dropping Unneeded Columns")
df = df.drop(columns=columns_to_drop, errors="coerce")

round = df["round_number"].unique()
print(f"[INFO] Rounds check: {round}")

for r in round:
    print(f"Currently Transforming: Round {r}")
    race_base = df[(df["round_number"]) == r].copy()
    
    print("[INFO] Dropping unnecessary columns: LapTime")
    report = race_base.drop(columns="LapTime")
    report["LapNumber"] = report["LapNumber"].astype(int)

    print("[INFO] Sorting by DriverNumber and Time")
    report = report.sort_values(
        by=["DriverNumber", "Time"],
        ascending=[True, False]
    )

    print("[INFO] Dropping Everything except the last lap of each driver")
    report = report.drop_duplicates(subset="DriverNumber", keep="first")
    
    print("[INFO] Inserting unrecorded drivers")
    overall_roster = df
    report = insert_driver(overall_roster, report)

    print("[INFO] Sorting by position")
    report["Position"] = report["Position"].fillna(100)
    report["Position"] = pd.to_numeric(report["Position"], errors="coerce").astype(int)
    report = report.sort_values(
        by=["Position", "Time"],
        ascending=[True, False]
    )

    print("[INFO] Assigning points for each driver")
    report["Points"] = 0
    report["Points"] = assign_points(report["Position"])

    print("[INFO] Looking for the Personal Best for Each Driver")
    pb = race_base.sort_values(
        by=["DriverNumber", "LapTime"],
        ascending=[True, True]
    )
    pb = pb.drop_duplicates(subset="DriverNumber", keep="first")
    pb = pb[["DriverNumber", "LapTime"]].rename(columns={"LapTime": "PersonalBest"})
    report = report.merge(pb, on=["DriverNumber"], how="left")
    
    print("[INFO] Adding Fastest Lap Indicator")
    fastest_lap_index = pb["PersonalBest"].idxmin()
    fastest = pb.loc[[fastest_lap_index]]
    fastest_driver = int(fastest["DriverNumber"].iloc[0])
    print(f"[INFO] Fastest Driver: {fastest_driver}")
    report["IsFastestLap"] = False
    report.loc[report["DriverNumber"] == fastest_driver, "IsFastestLap"] = True

    print("[INFO] Assigning DNFs and DNSes")
    pos = report["Position"]
    lap = report["PersonalBest"]

    report.loc[(pos == 100) & lap.isna(), "Position"] = "DNS"
    report.loc[(pos == 100) & lap.notna(), "Position"] = "DNF"

    report = report.drop(columns=["Time"])

    print("[INFO] Uploading to GCS")
    report["Position"] = report["Position"].astype(str)
    buf = BytesIO()
    report.to_parquet(buf, index=False)
    buf.seek(0)

    bucket = client.bucket(GCS_BUCKET)
    key = f"gold/season={SEASON}/round={r:02d}/report.parquet"
    bucket.blob(key).upload_from_file(buf, content_type="application/octet-stream")
    print(f"[INFO] Report uploaded to GCS in {key}")