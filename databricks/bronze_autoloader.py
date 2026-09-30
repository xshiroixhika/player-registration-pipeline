# Databricks notebook source
# Bronze load: S3 raw files -> Delta tables, untransformed.
# Auto Loader tracks which files it has already processed (via the checkpoint),
# so reruns never load the same file twice. trigger(availableNow=True) makes
# this a batch job: process everything new, then stop.

from pyspark.sql.functions import col, current_timestamp

BUCKET = dbutils.widgets.get("raw_bucket")          # noqa: F821 (Databricks global)
CATALOG = "registrations"
CHECKPOINTS = f"s3://{BUCKET}/_checkpoints"


def load_to_bronze(prefix: str, table: str) -> None:
    (
        spark.readStream.format("cloudFiles")        # noqa: F821
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/{table}/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"s3://{BUCKET}/{prefix}")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .writeStream
        .option("checkpointLocation", f"{CHECKPOINTS}/{table}/checkpoint")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.bronze.{table}")
        .awaitTermination()
    )


load_to_bronze("raw/player_registrations", "raw_player_registrations")
load_to_bronze("raw/source_daily_counts", "source_daily_counts")
