from dotenv import load_dotenv
import os
from pathlib import Path
import fastf1
import pandas as pd
from google.cloud import storage
from datetime import datetime, timezone

load_dotenv()

# ENV
TMP_ROOT = Path(os.getenv("TMP_ROOT", "/tmp/f1"))
CACHE_DIR = TMP_ROOT / "cache_fastf1"
LOCAL_OUT_ROOT = TMP_ROOT / "bronze_out"

SEASON = int(os.getenv("SEASON", "2026"))
SESSION_TYPE = os.getenv("SESSION_TYPE", "R")

SKIP_IF_EXISTS = os.getenv("SKIP_IF_EXISTS", "true").lower() == "true"
FORCE_RELOAD = os.getenv("FORCE_RELOAD", "false").lower() == "true" 
N_NEWEST_RACES = int(os.getenv("N_NEWEST_RACES", "0"))

GCS_BUCKET = os.getenv("GCS_BUCKET", "").strip()
GCS_PREFIX = os.getenv("GCS_PREFIX", "bronze/laps").strip().strip("/")

# FUNCTIONS
def ensure_dirs():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    LOCAL_OUT_ROOT.mkdir(parents=True, exist_ok=True)

def init_fastf1_cache():
    fastf1.Cache.enable_cache(str(CACHE_DIR))

def bronze_rel_path(season, round_number):
    return f"season={season}/round={round_number:02d}/laps.parquet"

def bronze_local_path(season, round_number):
    return LOCAL_OUT_ROOT / bronze_rel_path(season, round_number)

def bronze_gcs_key(season, round_number):
    return f"{GCS_PREFIX}/{bronze_rel_path(season, round_number)}"

def gcs_blob_exists(bucket, key):
    return bucket.blob(key).exists()

def build_completed_schedule(season):  
    now_utc = pd.Timestamp.now(tz="UTC")
    schedule = fastf1.get_event_schedule(season, include_testing=False).copy()
    schedule = schedule[schedule["RoundNumber"].notna()].copy()
    schedule["RoundNumber"] = schedule["RoundNumber"].astype(int)
    schedule["EventDate"] = pd.to_datetime(schedule["EventDate"], utc=True, errors="coerce")

    done = schedule[schedule["EventDate"] <= now_utc].copy()
    return done.sort_values("RoundNumber")

def pick_one_missing_event(schedule_done, bucket):
    candidates = schedule_done.sort_values("RoundNumber", ascending=False)

    for _, ev in candidates.iterrows():
        rnd = int(ev["RoundNumber"])

        key = bronze_gcs_key(SEASON, rnd)
        exists = gcs_blob_exists(bucket, key)

        if FORCE_RELOAD:
            return pd.DataFrame([ev])
        
        if not exists:
            return pd.DataFrame([ev])
        
        if exists and SKIP_IF_EXISTS:
            continue

        if not SKIP_IF_EXISTS:
            return pd.DataFrame([ev])
    
    return pd.DataFrame

def save_parquet_local(df, out_path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)

def upload_file_to_gcs(bucket, local_out_file, gcs_key):
    blob = bucket.blob(gcs_key)
    blob.upload_from_filename(str(local_out_file))

def manifest_gcs_key(season, session_type):
    return f"bronze/fastf1/_meta/ingest_manifest_season_{season}_{session_type}.parquet"

def main():
    run_ts = datetime.now(timezone.utc).isoformat()
    manifest_rows = []

    print(f"Run timestamp = {run_ts}")
    
    print("Ensuring directories")
    ensure_dirs()
    print("Directories all good")

    print("Initializing cache")
    init_fastf1_cache()
    print("Cache all good")

    print(f"[INFO] SEASON={SEASON}, SESSION TYPE={SESSION_TYPE}")
    print(f"[INFO] SKIP IF EXISTS={SKIP_IF_EXISTS}, FORCE RELOAD={FORCE_RELOAD}, N NEWEST RACES={N_NEWEST_RACES}")

    print("Setting up GCS")
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    print(f"Saving on {bucket} on {storage_client}")

    schedule_done = build_completed_schedule(SEASON)
    if schedule_done.empty:
        raise ValueError(f"No completed race for the {SEASON} season")
    
    target_df = pick_one_missing_event(schedule_done, bucket)

    if target_df.empty:
        print("[INFO] All completed races is already in storage. Nothing to ingest")
        return 
    else:
        print("[INFO] Picked Target Dataframe: ")
        print(target_df)

    target_row = target_df.iloc[0]
    rnd = int(target_row["RoundNumber"])
    event_name = target_row["EventName"]

    run_ts = datetime.now(timezone.utc).isoformat()
    manifest_rows = []

    local_out_file = bronze_local_path(SEASON, rnd)
    gcs_key = bronze_gcs_key(SEASON, rnd)

    already_exists = gcs_blob_exists(bucket, gcs_key)

    try:
        print(f"[INFO] Loading season={SEASON}, round={rnd:02d}, event={event_name}")
        session = fastf1.get_session(SEASON, rnd, SESSION_TYPE)
        session.load(telemetry=False, weather=False, messages=False)

        laps = session.laps.copy()
        laps["season"] = SEASON
        laps["round_number"] = rnd
        laps["event_name"] = event_name
        laps["session_type"] = SESSION_TYPE
        laps["ingested_at_utc"] = run_ts

        save_parquet_local(laps, local_out_file)

        upload_file_to_gcs(bucket, local_out_file, gcs_key)
        out_path = f"gs://{GCS_BUCKET}/{gcs_key}"

        manifest_rows.append(
            {
                "season": SEASON,
                "round_number": rnd,
                "event_name": event_name,
                "rows_written": int(len(laps)),
                "status": "success",
                "path": out_path,
                "ingested_at_utc": run_ts,
            }
        )
        print(f"[OK] rows={len(laps)} -> {out_path}")
    except Exception as e:
        manifest_rows.append(
            {
                "season": SEASON,
                "round_number": rnd,
                "event_name": event_name,
                "rows_written": 0,
                "status": f"failed: {e}",
                "path": None,
                "ingested_at_utc": run_ts,
            }
        )
        print(f"[ERR] round={rnd:02d}: {e}")

    manifest = pd.DataFrame(manifest_rows)

    local_manifest_file = TMP_ROOT / f"ingest_manifest_season_{SEASON}_{SESSION_TYPE}.parquet"
    manifest.to_parquet(local_manifest_file, index=False)

    meta_key = manifest_gcs_key(SEASON, SESSION_TYPE)
    failed = (manifest["status"].astype(str).str.startswith("failed")).sum()
    if failed > 0:
            raise RuntimeError(f"Ingest selesai tapi ada failure: {failed}")
    
if __name__ == "__main__":
    main()
