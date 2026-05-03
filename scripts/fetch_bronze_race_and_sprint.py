import io
import os
from datetime import datetime, timezone
from pathlib import Path

import fastf1
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage
from fastf1.core import DataNotLoadedError

load_dotenv()

# ---------------------------------------------------------------------------
# ENV (aligned with fetch_bronze.py where applicable)
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


def should_try_round(exists: bool) -> bool:
    if FORCE_RELOAD:
        return True
    if not exists:
        return True
    if exists and SKIP_IF_EXISTS:
        return False
    return True


def is_missing_session_error(err: ValueError) -> bool:
    """True when FastF1 says this session type does not exist for the event."""
    msg = str(err).lower()
    return "session type" in msg and "does not exist" in msg


def main() -> None:
    print(f"[INFO] SEASON={SEASON}")
    print(f"[INFO] SKIP_IF_EXISTS={SKIP_IF_EXISTS}, FORCE_RELOAD={FORCE_RELOAD}")

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

    candidates = schedule_done.sort_values("RoundNumber", ascending=False)

    for session_type in ("R", "S"):
        print(
            f"\n[INFO] --- session_type={session_type} "
            f"({parquet_name(session_type)}) ---\n"
        )

        uploaded = False

        for i, (_, row) in enumerate(candidates.iterrows()):
            if FORCE_RELOAD and i > 0:
                # Match fetch_bronze: only the newest round when forcing
                break

            rnd = int(row["RoundNumber"])
            event_name = str(row["EventName"])
            object_path = gcs_object_path(rnd, session_type)
            exists = bucket.blob(object_path).exists()

            if not should_try_round(exists):
                continue

            print(
                f"[INFO] Loading season={SEASON}, round={rnd:02d}, "
                f"event={event_name}, session_type={session_type}"
            )
            try:
                session = fastf1.get_session(SEASON, rnd, session_type)
                session.load(telemetry=False, weather=False, messages=False)
                laps = session.laps.copy()
            except ValueError as e:
                if is_missing_session_error(e):
                    print(
                        f"[WARN] No {session_type} session for this event "
                        f"(round={rnd:02d}); trying next candidate."
                    )
                    if FORCE_RELOAD:
                        break
                    continue
                raise
            except DataNotLoadedError:
                print(
                    f"[WARN] No lap data in FastF1 for this session yet "
                    f"(round={rnd:02d}); trying next candidate."
                )
                if FORCE_RELOAD:
                    break
                continue

            if laps.empty:
                print(f"[WARN] Laps empty for round={rnd:02d}; trying next candidate.")
                if FORCE_RELOAD:
                    break
                continue

            ingested_at = datetime.now(timezone.utc).isoformat()
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
            uploaded = True
            break

        if not uploaded:
            print(
                "[INFO] Nothing ingested for this session type "
                "(no eligible round, no session, or no lap data for tried rounds)."
            )


if __name__ == "__main__":
    main()
