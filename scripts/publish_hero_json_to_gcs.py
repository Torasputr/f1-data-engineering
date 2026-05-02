"""
Build hero.json for the Website from gold Parquet on GCS, then upload next to your medallion layout.

Reads:
  gs://{GCS_BUCKET}/gold/season={SEASON}/standings.parquet
If missing, rebuilds standings from *report.parquet under the same gold prefix (same logic as standings.py).

Writes:
  gs://{GCS_BUCKET}/{HERO_JSON_BLOB}  (default: website/hero.json)

Requires: google-cloud-storage, pandas, pyarrow (same as your other gold scripts).
Set GCS_BUCKET and SEASON in scripts/.env or the environment.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

GCS_BUCKET = os.getenv("GCS_BUCKET", "").strip()
SEASON = int(os.getenv("SEASON", "0") or 0)
HERO_JSON_BLOB = os.getenv("HERO_JSON_BLOB", "website/hero.json").strip().lstrip("/")


def _gold_path() -> str:
    return f"gold/season={SEASON}"


def _load_standings_from_gcs(client: storage.Client, bucket: storage.Bucket) -> pd.DataFrame:
    gold = _gold_path()
    stan_key = f"{gold}/standings.parquet"
    stan_blob = bucket.blob(stan_key)

    if stan_blob.exists():
        print(f"[INFO] Using existing {stan_key}")
        raw = stan_blob.download_as_bytes()
        return pd.read_parquet(BytesIO(raw))

    print(f"[INFO] {stan_key} not found; aggregating from *report.parquet")
    blobs = [
        b
        for b in client.list_blobs(GCS_BUCKET, prefix=gold)
        if b.name.endswith("report.parquet")
    ]
    if not blobs:
        raise FileNotFoundError(
            f"No standings.parquet and no report.parquet under prefix {gold!r}"
        )

    cols = ["Driver", "DriverNumber", "Team", "season", "Points"]
    parts = []
    for blob in blobs:
        df = pd.read_parquet(BytesIO(blob.download_as_bytes()))
        parts.append(df[cols].copy())

    all_rounds = pd.concat(parts, ignore_index=True)
    standings = (
        all_rounds.groupby("DriverNumber", as_index=False)
        .agg(
            Points=("Points", "sum"),
            Driver=("Driver", "first"),
            Team=("Team", "first"),
            season=("season", "first"),
        )
        .sort_values("Points", ascending=False)
        .reset_index(drop=True)
    )
    return standings


def _hero_payload(standings: pd.DataFrame) -> dict:
    if standings.empty:
        raise ValueError("Standings dataframe is empty")

    top = standings.iloc[0]
    season_val = int(top["season"]) if pd.notna(top["season"]) else SEASON
    points = float(top["Points"])
    if points == int(points):
        points = int(points)

    return {
        "season": season_val,
        "title": "Formula 1",
        "tagline": "Driver standings from your gold layer (published as JSON for the web app).",
        "leader": {
            "position": 1,
            "driver": str(top["Driver"]),
            "team": str(top["Team"]),
            "points": points,
        },
        "lastUpdated": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def main() -> None:
    if not GCS_BUCKET:
        raise SystemExit("GCS_BUCKET is required")
    if SEASON <= 0:
        raise SystemExit("SEASON must be a positive integer")

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    standings = _load_standings_from_gcs(client, bucket)
    payload = _hero_payload(standings)

    dest = bucket.blob(HERO_JSON_BLOB)
    dest.upload_from_string(
        json.dumps(payload, indent=2),
        content_type="application/json; charset=utf-8",
    )
    print(f"[INFO] Uploaded gs://{GCS_BUCKET}/{HERO_JSON_BLOB}")
    print("[INFO] Point the site at:")
    print(f"       https://storage.googleapis.com/{GCS_BUCKET}/{HERO_JSON_BLOB}")


if __name__ == "__main__":
    main()
