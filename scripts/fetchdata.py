import os
from pathlib import Path
from datetime import datetime, timezone

import fastf1
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv


load_dotenv()  # baca .env


# =========================
# Config
# =========================
SEASON = int(os.getenv("SEASON", "2026"))
SESSION_TYPE = os.getenv("SESSION_TYPE", "R")
SOURCE = "fastf1"

RUN_MODE = os.getenv("RUN_MODE", "gcs").lower()  # gcs | local
GCS_BUCKET = os.getenv("GCS_BUCKET", "").strip()
GCS_PREFIX = os.getenv("GCS_PREFIX", "bronze/fastf1/laps").strip().strip("/")

SKIP_IF_EXISTS = os.getenv("SKIP_IF_EXISTS", "true").lower() == "true"
FORCE_RELOAD = os.getenv("FORCE_RELOAD", "false").lower() == "true"

TMP_ROOT = Path(os.getenv("TMP_ROOT", "/tmp/f1"))
CACHE_DIR = TMP_ROOT / "cache_fastf1"
LOCAL_OUT_ROOT = TMP_ROOT / "bronze_out"


# =========================
# Helpers
# =========================
def bronze_rel_path(season: int, round_number: int) -> str:
    return f"season={season}/round={round_number:02d}/laps.parquet"


def bronze_local_path(season: int, round_number: int) -> Path:
    return LOCAL_OUT_ROOT / bronze_rel_path(season, round_number)


def bronze_gcs_key(season: int, round_number: int) -> str:
    return f"{GCS_PREFIX}/{bronze_rel_path(season, round_number)}"


def manifest_gcs_key(season: int, session_type: str) -> str:
    return f"bronze/fastf1/_meta/ingest_manifest_season_{season}_{session_type}.parquet"


def ensure_tmp_dirs() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    LOCAL_OUT_ROOT.mkdir(parents=True, exist_ok=True)


def init_fastf1_cache() -> None:
    fastf1.Cache.enable_cache(str(CACHE_DIR))


def gcs_blob_exists(bucket: storage.Bucket, key: str) -> bool:
    return bucket.blob(key).exists()


def upload_file_to_gcs(bucket: storage.Bucket, local_path: Path, key: str) -> None:
    blob = bucket.blob(key)
    blob.upload_from_filename(str(local_path))


def save_parquet_local(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)


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


def pick_one_missing_event(
    schedule_done: pd.DataFrame, bucket: storage.Bucket | None
) -> pd.DataFrame:
    """
    Cari 1 event yang belum ada datanya.
    Prioritas: dari event terbaru -> lama.
    """
    candidates = schedule_done.sort_values("RoundNumber", ascending=False)

    for _, ev in candidates.iterrows():
        rnd = int(ev["RoundNumber"])

        if RUN_MODE == "local":
            exists = bronze_local_path(SEASON, rnd).exists()
        else:
            key = bronze_gcs_key(SEASON, rnd)
            exists = gcs_blob_exists(bucket, key)

        if FORCE_RELOAD:
            return pd.DataFrame([ev])

        if not exists:
            return pd.DataFrame([ev])

        # kalau exists dan skip aktif, cek event sebelumnya
        if exists and SKIP_IF_EXISTS:
            continue

        # fallback behavior
        if not SKIP_IF_EXISTS:
            return pd.DataFrame([ev])

    return pd.DataFrame()


def main() -> None:
    if RUN_MODE == "gcs" and not GCS_BUCKET:
        raise ValueError("GCS_BUCKET wajib diisi kalau RUN_MODE='gcs'.")

    ensure_tmp_dirs()
    init_fastf1_cache()

    print(f"[INFO] SEASON={SEASON}, SESSION_TYPE={SESSION_TYPE}, RUN_MODE={RUN_MODE}")
    print(f"[INFO] SKIP_IF_EXISTS={SKIP_IF_EXISTS}, FORCE_RELOAD={FORCE_RELOAD}")

    storage_client = None
    bucket = None
    if RUN_MODE == "gcs":
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET)

    schedule_done = build_completed_schedule(SEASON)
    if schedule_done.empty:
        raise ValueError(f"Tidak ada completed race untuk season {SEASON}.")

    target_df = pick_one_missing_event(schedule_done, bucket)

    if target_df.empty:
        print("[INFO] Semua completed events sudah ada di storage. Nothing to ingest.")
        return

    target_row = target_df.iloc[0]
    rnd = int(target_row["RoundNumber"])
    event_name = target_row["EventName"]

    print(f"[INFO] Selected event -> round={rnd:02d}, event={event_name}")

    run_ts = datetime.now(timezone.utc).isoformat()
    manifest_rows = []

    local_out_file = bronze_local_path(SEASON, rnd)
    gcs_key = bronze_gcs_key(SEASON, rnd)

    # Safety check lagi sebelum fetch
    if RUN_MODE == "local":
        already_exists = local_out_file.exists()
    else:
        already_exists = gcs_blob_exists(bucket, gcs_key)

    if already_exists and SKIP_IF_EXISTS and not FORCE_RELOAD:
        msg_path = (
            str(local_out_file)
            if RUN_MODE == "local"
            else f"gs://{GCS_BUCKET}/{gcs_key}"
        )
        print(f"[SKIP] season={SEASON} round={rnd:02d} exists: {msg_path}")
        manifest_rows.append(
            {
                "season": SEASON,
                "round_number": rnd,
                "event_name": event_name,
                "rows_written": 0,
                "status": "skipped_exists",
                "path": msg_path,
                "ingested_at_utc": run_ts,
            }
        )
    else:
        try:
            print(
                f"[INFO] Loading season={SEASON}, round={rnd:02d}, event={event_name}"
            )
            session = fastf1.get_session(SEASON, rnd, SESSION_TYPE)
            session.load(telemetry=False, weather=False, messages=False)

            laps = session.laps.copy()
            laps["season"] = SEASON
            laps["round_number"] = rnd
            laps["event_name"] = event_name
            laps["session_type"] = SESSION_TYPE
            laps["source"] = SOURCE
            laps["ingested_at_utc"] = run_ts

            save_parquet_local(laps, local_out_file)

            if RUN_MODE == "gcs":
                upload_file_to_gcs(bucket, local_out_file, gcs_key)
                out_path = f"gs://{GCS_BUCKET}/{gcs_key}"
            else:
                out_path = str(local_out_file)

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

    # Save manifest local temp
    local_manifest_file = (
        TMP_ROOT / f"ingest_manifest_season_{SEASON}_{SESSION_TYPE}.parquet"
    )
    manifest.to_parquet(local_manifest_file, index=False)

    # Upload manifest kalau mode GCS
    if RUN_MODE == "gcs":
        meta_key = manifest_gcs_key(SEASON, SESSION_TYPE)
        upload_file_to_gcs(bucket, local_manifest_file, meta_key)
        print(f"[INFO] Manifest uploaded: gs://{GCS_BUCKET}/{meta_key}")
    else:
        print(f"[INFO] Manifest saved local: {local_manifest_file}")

    print("\n=== Summary ===")
    print(
        manifest[
            ["round_number", "event_name", "rows_written", "status", "path"]
        ].to_string(index=False)
    )

    failed = (manifest["status"].astype(str).str.startswith("failed")).sum()
    if failed > 0:
        raise RuntimeError(f"Ingest selesai tapi ada failure: {failed}")


if __name__ == "__main__":
    main()
