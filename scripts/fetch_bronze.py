import io
import os
from datetime import datetime, timezone
from pathlib import Path

import fastf1
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

# ---------------------------------------------------------------------------
# ENV (same knobs as fetch_bronze.py where applicable)
# ---------------------------------------------------------------------------
TMP_ROOT = Path(os.getenv("TMP_ROOT", "/tmp/f1"))
CACHE_DIR = TMP_ROOT / "cache_fastf1"

SEASON = int(os.getenv("SEASON", "2026"))
GCS_BUCKET = os.getenv("GCS_BUCKET", "").strip()
GCS_PREFIX = os.getenv("GCS_PREFIX", "bronze/laps").strip().strip("/")

SKIP_IF_EXISTS = os.getenv("SKIP_IF_EXISTS", "true").lower() == "true"
FORCE_RELOAD = os.getenv("FORCE_RELOAD", "false").lower() == "true"


# ---------------------------------------------------------------------------
# FUNCTIONS
# ---------------------------------------------------------------------------
def parquet_name(session_type: str) -> str:
    st = session_type.upper()
    if st == "R":
        return "laps.parquet"
    if st == "S":
        return "sprint_laps.parquet"
    raise ValueError("session_type must be R or S")


def gcs_object_path(round_no: int, session_type: str) -> str:
    return (
        f"{GCS_PREFIX}/season={SEASON}/round={round_no:02d}/"
        f"{parquet_name(session_type)}"
    )


def laps_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    return buf.getvalue()


def ensure_cache_dir():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def init_fastf1_cache():
    fastf1.Cache.enable_cache(str(CACHE_DIR))


def build_completed_schedule(season: int) -> pd.DataFrame:
    now_utc = pd.Timestamp.now(tz="UTC")
    schedule = fastf1.get_event_schedule(season, include_testing=False).copy()
    schedule = schedule[schedule["RoundNumber"].notna()].copy()
    schedule["RoundNumber"] = schedule["RoundNumber"].astype(int)
    schedule["EventDate"] = pd.to_datetime(
        schedule["EventDate"], utc=True, errors="coerce"
    )
    done = schedule[schedule["EventDate"] <= now_utc].copy()
    return done.sort_values("RoundNumber")


def pick_next_round_for_session(
    schedule_done: pd.DataFrame,
    bucket: storage.Bucket,
    session_type: str,
):

    candidates = schedule_done.sort_values("RoundNumber", ascending=False)

    for _, row in candidates.iterrows():
        rnd = int(row["RoundNumber"])
        key = gcs_object_path(rnd, session_type)
        exists = bucket.blob(key).exists()

        if FORCE_RELOAD:
            return rnd, str(row["EventName"]), key

        if not exists:
            return rnd, str(row["EventName"]), key

        if exists and SKIP_IF_EXISTS:
            continue

        if not SKIP_IF_EXISTS:
            return rnd, str(row["EventName"]), key

    return None


def main() -> None:
    print(f"[INFO] SEASON={SEASON}")
    print(
        f"[INFO] SKIP_IF_EXISTS={SKIP_IF_EXISTS}, "
        f"FORCE_RELOAD={FORCE_RELOAD}"
    )

    print("Ensuring FastF1 cache directory")
    ensure_cache_dir()
    print("Initializing FastF1 cache")
    init_fastf1_cache()
    print("Cache ready")

    print("Setting up GCS")
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    print(f"[INFO] bucket={GCS_BUCKET}")

    schedule_done = build_completed_schedule(SEASON)
    if schedule_done.empty:
        raise SystemExit(f"No completed events for season {SEASON}")

    # Ingest at most one missing round per session type (R then S).
    for session_type in ("R", "S"):
        print(
            f"\n[INFO] --- session_type={session_type} "
            f"({parquet_name(session_type)}) ---\n"
        )

        picked = pick_next_round_for_session(schedule_done, bucket, session_type)
        if picked is None:
            print(
                "[INFO] Nothing to ingest for this session type "
                "(blob already present or schedule exhausted)."
            )
            continue

        rnd, event_name, object_path = picked
        ingested_at = datetime.now(timezone.utc).isoformat()

        print(
            f"[INFO] Loading season={SEASON}, round={rnd:02d}, "
            f"event={event_name}, session_type={session_type}"
        )
        session = fastf1.get_session(SEASON, rnd, session_type)
        session.load(telemetry=False, weather=False, messages=False)

        laps = session.laps.copy()
        laps["season"] = SEASON
        laps["round_number"] = rnd
        laps["event_name"] = event_name
        laps["session_type"] = session_type
        laps["ingested_at_utc"] = ingested_at

        payload = laps_to_parquet_bytes(laps)
        bucket.blob(object_path).upload_from_string(
            payload,
            content_type="application/vnd.apache.parquet",
        )
        out = f"gs://{GCS_BUCKET}/{object_path}"
        print(f"[OK] rows={len(laps)} bytes={len(payload)} -> {out}")


if __name__ == "__main__":
    main()