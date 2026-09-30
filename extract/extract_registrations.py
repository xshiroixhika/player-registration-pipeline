"""
Batch extract: accounts DB read replica -> S3 raw landing zone.

Pulls registrations created since the last watermark (with a small overlap to
catch late-committing rows), writes them to S3 as Parquet, and only then
advances the watermark. Also writes the source's own daily counts for the
last N days, which dbt uses to reconcile the warehouse against the source.

Duplicates caused by the overlap are expected and removed in silver.
"""
import io
import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd
from sqlalchemy import create_engine, text

log = logging.getLogger(__name__)

BUCKET = os.environ["RAW_BUCKET"]                 # e.g. "studio-data-raw"
REPLICA_URL = os.environ["ACCOUNTS_REPLICA_URL"]  # read replica, never the primary
REG_PREFIX = "raw/player_registrations"
COUNTS_PREFIX = "raw/source_daily_counts"
WATERMARK_KEY = "state/player_registrations_watermark.json"
OVERLAP = timedelta(hours=1)
RECONCILE_DAYS = 7
DEFAULT_START = datetime(2024, 1, 1, tzinfo=timezone.utc)

REGISTRATIONS_SQL = text("""
    select
        player_id,
        created_at,          -- stored in UTC in the accounts DB
        platform,
        country_code,
        country_source,      -- 'billing' or 'ip_geo'
        birth_year,
        campaign_id,
        app_version,
        is_internal,         -- QA / employee / test accounts
        consent_analytics
    from accounts
    where created_at >= :since and created_at < :until
""")

SOURCE_COUNTS_SQL = text("""
    select cast(created_at as date) as registration_date,
           count(*)                 as registration_count
    from accounts
    where created_at >= :since and created_at < :until
      and not is_internal
    group by 1
""")

s3 = boto3.client("s3")


def read_watermark() -> datetime:
    try:
        body = s3.get_object(Bucket=BUCKET, Key=WATERMARK_KEY)["Body"].read()
        return datetime.fromisoformat(json.loads(body)["watermark"])
    except s3.exceptions.NoSuchKey:
        return DEFAULT_START


def write_watermark(value: datetime) -> None:
    s3.put_object(Bucket=BUCKET, Key=WATERMARK_KEY,
                  Body=json.dumps({"watermark": value.isoformat()}))


def write_parquet(df: pd.DataFrame, prefix: str, run_ts: datetime) -> str:
    key = (f"{prefix}/ingest_date={run_ts:%Y-%m-%d}/"
           f"batch_{run_ts:%Y%m%dT%H%M%S}.parquet")
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    return key


def run() -> None:
    run_ts = datetime.now(timezone.utc)
    watermark = read_watermark()
    since, until = watermark - OVERLAP, run_ts

    engine = create_engine(REPLICA_URL)
    with engine.connect() as conn:
        regs = pd.read_sql(REGISTRATIONS_SQL, conn,
                           params={"since": since, "until": until})
        counts = pd.read_sql(SOURCE_COUNTS_SQL, conn, params={
            "since": (until - timedelta(days=RECONCILE_DAYS)).date(),
            "until": until.date(),   # complete days only
        })

    if regs.empty:
        log.info("No new registrations since %s", since)
    else:
        key = write_parquet(regs, REG_PREFIX, run_ts)
        log.info("Wrote %d registrations to s3://%s/%s", len(regs), BUCKET, key)

    write_parquet(counts, COUNTS_PREFIX, run_ts)

    # Advance the watermark only after the data is safely in S3.
    write_watermark(until)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
