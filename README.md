# Player Registration Pipeline

A daily batch pipeline that turns account creations into a trusted,
privacy-safe dashboard of new player demographics.

**Stack:** accounts DB read replica → pandas extract → S3 → Databricks Auto Loader
(bronze) → dbt on Databricks (silver, gold) → Tableau. Orchestrated by Airflow,
version-controlled in Git, tested in CI.

```
Accounts DB replica
      │  extract/extract_registrations.py   (Airflow task, watermark + overlap)
      ▼
S3  raw/player_registrations/ingest_date=YYYY-MM-DD/*.parquet
    raw/source_daily_counts/...             (for reconciliation)
      │  databricks/bronze_autoloader.py    (Auto Loader, availableNow)
      ▼
BRONZE  registrations.bronze.raw_player_registrations     what we received
      │  dbt: stg_player_registrations
      ▼
SILVER  registrations.silver.stg_player_registrations     what's true
      │  dbt: dims, fact, aggregate
      ▼
GOLD    registrations.gold.agg_daily_registrations        what the business sees
      │  Airflow refreshes the Tableau extract
      ▼
Tableau dashboard (reads gold only)
```

## Repository layout

```
extract/                 Replica → S3 batch extract (pandas, boto3)
databricks/              Auto Loader job: S3 → bronze Delta tables
dbt/
  models/staging/        Silver: stg_player_registrations, quarantine, tests
  models/marts/          Gold: dim_*, fct_player_registrations,
                         agg_daily_registrations, rpt_pipeline_status
  seeds/                 Reference data: countries, platforms, age bands, campaigns
  tests/                 Reconciliation against source; quarantine warning
  macros/                Schema naming for dev / ci / prod isolation
airflow/dags/            The daily DAG
.github/workflows/       CI: lint + dbt build on every pull request
```

## What each layer guarantees

**Bronze** – Exact copy of what arrived, append-only, with `_ingested_at` and
`_source_file` for lineage. Duplicates are expected here. Restricted access.

**Silver** – One row per real player. Parsed and typed, deduplicated on
`player_id`, platforms and countries standardized (unrecognized → `unknown` /
`XX`), impossible birth years nulled, internal/test accounts removed. Rows missing
`player_id` or `created_at` go to `quarantine_player_registrations` with a reason.
Incremental merge with a 3-day lookback for late data. Restricted access.

**Gold** – Kimball star schema. `fct_player_registrations` at a grain of one row
per registered player, with conformed dimensions for date, country, platform, age
band, and acquisition channel. Player IDs are salted and hashed, birth year becomes
an age band, and every dimension has an explicit Unknown/Organic member so no
facts are lost in joins. `agg_daily_registrations` is what Tableau reads, with
cells under 10 players suppressed. `rpt_pipeline_status` powers the "data as of"
indicator.

## How trust is enforced

| Check | Where | On failure |
|---|---|---|
| Source freshness | `dbt source freshness` | Warn at 26h, stop at 48h |
| Uniqueness, not-null, relationships | model YAML | Downstream models skipped |
| Reconciliation vs. source DB (±0.5%, last 7 days) | `tests/assert_registrations_reconcile_with_source.sql` | Gold not published |
| Quarantined records | `tests/warn_on_quarantined_records.sql` | Warning only |
| Code changes | CI on every pull request | Can't merge |

Because `dbt build` skips everything downstream of a failed test, and Tableau only
refreshes after `dbt build` succeeds, **bad data never reaches the dashboard** —
it keeps showing the last verified numbers, and the team gets alerted.

## Setup

1. **Databricks:** create catalogs `registrations` (prod), `registrations_dev`,
   `registrations_ci`, each with `bronze`, `silver`, `gold` schemas. Grant analysts
   and the Tableau service principal read access on `gold` only.
2. **Bronze job:** create a Databricks job from `databricks/bronze_autoloader.py`
   with a `raw_bucket` parameter. Put its ID in the Airflow Variable
   `bronze_autoloader_job_id`.
3. **dbt:** `cp dbt/profiles.yml.example ~/.dbt/profiles.yml`, set
   `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`,
   `PLAYER_HASH_SALT`, then `cd dbt && dbt deps && dbt seed && dbt build`.
4. **Airflow:** set `RAW_BUCKET`, `ACCOUNTS_REPLICA_URL`, `DBT_PROJECT_DIR`, the
   `databricks_default` connection, and the `tableau_*` Variables.
5. **Tableau:** connect to the Databricks SQL warehouse, build the datasource on
   `gold.agg_daily_registrations` and `gold.rpt_pipeline_status`, and publish it
   as an extract.

Secrets live in environment variables and secret stores, never in Git.

## Working on the pipeline

1. Branch from `main`: `git checkout -b feature/add-age-band-65-plus`
2. Develop against your own dev schema (`--target dev`, the default).
3. Open a pull request. CI lints Python and runs `dbt build` in an isolated CI schema.
4. After review and a green CI run, merge. Production picks it up on the next run.

Common changes: edit `seeds/age_bands.csv` to change age bands; add a row to
`seeds/campaigns.csv` for a new campaign; add platform aliases in
`stg_player_registrations.sql`.

## Known tradeoffs

- **Daily batch, not streaming.** Simpler and reads the source of truth directly.
  If near-real-time counts are needed, add Kafka and stream into bronze.
- **Only what the accounts DB stores.** Fields that exist only in client events
  would need an event stream joined in silver.
- **Current state, not history.** The extract captures accounts as they are. For
  the dashboard that's fine (country at registration is recorded once), but tracking
  deletions or changes over time would need CDC or daily snapshots.
- **Consent.** `consent_analytics` is carried through to the fact table. Whether
  non-consenting players are excluded from aggregate counts is a policy decision
  to confirm with legal.
