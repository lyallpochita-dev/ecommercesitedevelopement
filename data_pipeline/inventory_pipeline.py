#!/usr/bin/env python3
"""Inventory ingestion and MongoDB loading pipeline using PySpark.

Milestone 1 deliverables:
1) Connect to remote storage and load inventory dataset.
2) Perform data cleansing and schema validation.
3) Transform to MongoDB-ready structured schema.
4) Load processed inventory records to MongoDB.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urlparse
from urllib.request import urlretrieve

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


EXPECTED_SCHEMA = StructType(
    [
        StructField("inventory_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("warehouse_id", StringType(), False),
        StructField("available_stock", IntegerType(), True),
        StructField("reserved_stock", IntegerType(), True),
        StructField("damaged_stock", IntegerType(), True),
        StructField("reorder_level", IntegerType(), True),
        StructField("last_updated", StringType(), True),
        StructField("batch_number", StringType(), True),
    ]
)


@dataclass
class PipelineConfig:
    source_uri: Optional[str]
    source_format: str
    output_json: Path
    mongo_uri: Optional[str]
    mongo_db: str
    mongo_collection: str
    synth_records: int


def configure_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )


def create_spark(app_name: str = "InventoryDataEngineering") -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def generate_synthetic_inventory(spark: SparkSession, records: int) -> DataFrame:
    logging.info("Generating %s synthetic inventory rows.", records)
    base = spark.range(1, records + 1).withColumnRenamed("id", "seq")

    return (
        base.select(
            F.format_string("INV-%06d", F.col("seq")).alias("inventory_id"),
            F.format_string("PROD-%05d", (F.col("seq") % 250) + 1).alias("product_id"),
            F.format_string("WH-%03d", (F.col("seq") % 8) + 1).alias("warehouse_id"),
            (F.col("seq") * 3 % 500).cast("int").alias("available_stock"),
            (F.col("seq") % 40).cast("int").alias("reserved_stock"),
            (F.col("seq") % 10).cast("int").alias("damaged_stock"),
            ((F.col("seq") * 2 % 120) + 10).cast("int").alias("reorder_level"),
            F.date_format(
                F.to_timestamp(F.lit("2026-01-01 00:00:00"))
                + F.make_interval(days=(F.col("seq") % 28), hours=(F.col("seq") % 24)),
                "yyyy-MM-dd'T'HH:mm:ss'Z'",
            ).alias("last_updated"),
            F.format_string("BATCH-%04d", (F.col("seq") % 50) + 1).alias("batch_number"),
        )
        .withColumn(
            "available_stock",
            F.when(F.col("inventory_id").endswith("005"), None).otherwise(F.col("available_stock")),
        )
        .withColumn(
            "last_updated",
            F.when(F.col("inventory_id").endswith("007"), F.lit("bad-ts")).otherwise(F.col("last_updated")),
        )
    )


def download_remote_source(source_uri: str) -> Path:
    parsed = urlparse(source_uri)
    suffix = Path(parsed.path).suffix or ".csv"
    target = Path(tempfile.mkstemp(prefix="inventory_source_", suffix=suffix)[1])

    if parsed.scheme in {"http", "https"}:
        logging.info("Downloading HTTP source from %s", source_uri)
        urlretrieve(source_uri, target)
        return target

    if parsed.scheme == "az":
        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("azure-storage-blob is required for az:// URIs") from exc

        conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not conn:
            raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING is required for az:// URIs")

        container = parsed.netloc
        blob_name = parsed.path.lstrip("/")
        logging.info("Downloading Azure Blob source from container=%s blob=%s", container, blob_name)
        blob_client = BlobServiceClient.from_connection_string(conn).get_blob_client(
            container=container, blob=blob_name
        )
        with open(target, "wb") as fh:
            fh.write(blob_client.download_blob().readall())
        return target

    if parsed.scheme in {"", "file"}:
        file_path = Path(parsed.path if parsed.scheme == "file" else source_uri).expanduser().resolve()
        if not file_path.exists():
            raise FileNotFoundError(f"Source file not found: {file_path}")
        return file_path

    raise ValueError(f"Unsupported source URI scheme: {parsed.scheme}")


def load_inventory_source(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    if not cfg.source_uri:
        return generate_synthetic_inventory(spark, cfg.synth_records)

    source_file = download_remote_source(cfg.source_uri)
    fmt = cfg.source_format.lower()
    logging.info("Loading source file %s as format=%s", source_file, fmt)
    reader = spark.read

    if fmt == "csv":
        return reader.option("header", True).csv(str(source_file))
    if fmt == "json":
        return reader.json(str(source_file))
    if fmt == "parquet":
        return reader.parquet(str(source_file))

    raise ValueError(f"Unsupported source format: {fmt}")


def validate_schema(df: DataFrame) -> Tuple[bool, list[str]]:
    incoming = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    expected = {field.name: field.dataType.simpleString() for field in EXPECTED_SCHEMA.fields}

    errors: list[str] = []
    for name, dtype in expected.items():
        if name not in incoming:
            errors.append(f"missing_column:{name}")
        elif name in {"available_stock", "reserved_stock", "damaged_stock", "reorder_level"}:
            pass
        elif incoming[name] != dtype:
            errors.append(f"type_mismatch:{name}:expected={dtype}:actual={incoming[name]}")

    return (len(errors) == 0, errors)


def clean_and_validate(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    cleansed = (
        df.select([F.col(c) for c in EXPECTED_SCHEMA.fieldNames()])
        .withColumn("inventory_id", F.trim(F.col("inventory_id")))
        .withColumn("product_id", F.trim(F.col("product_id")))
        .withColumn("warehouse_id", F.trim(F.col("warehouse_id")))
        .withColumn("batch_number", F.trim(F.col("batch_number")))
        .withColumn("available_stock", F.col("available_stock").cast("int"))
        .withColumn("reserved_stock", F.col("reserved_stock").cast("int"))
        .withColumn("damaged_stock", F.col("damaged_stock").cast("int"))
        .withColumn("reorder_level", F.col("reorder_level").cast("int"))
        .withColumn("last_updated_ts", F.to_timestamp("last_updated"))
        .drop("last_updated")
    )

    cleansed = (
        cleansed.fillna({"available_stock": 0, "reserved_stock": 0, "damaged_stock": 0, "reorder_level": 25})
        .withColumn("available_stock", F.greatest(F.col("available_stock"), F.lit(0)))
        .withColumn("reserved_stock", F.greatest(F.col("reserved_stock"), F.lit(0)))
        .withColumn("damaged_stock", F.greatest(F.col("damaged_stock"), F.lit(0)))
        .withColumn("reorder_level", F.greatest(F.col("reorder_level"), F.lit(0)))
    )

    invalid = cleansed.filter(
        F.col("inventory_id").isNull()
        | (F.col("inventory_id") == "")
        | F.col("product_id").isNull()
        | (F.col("product_id") == "")
        | F.col("warehouse_id").isNull()
        | (F.col("warehouse_id") == "")
        | F.col("last_updated_ts").isNull()
    )

    valid = cleansed.subtract(invalid)

    deduped = (
        valid.withColumn(
            "rn",
            F.row_number().over(Window.partitionBy("inventory_id").orderBy(F.col("last_updated_ts").desc())),
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    return deduped, invalid


def to_mongo_schema(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("inventory_id").alias("_id"),
        F.col("inventory_id"),
        F.col("product_id"),
        F.col("warehouse_id"),
        F.struct(
            F.col("available_stock").alias("available"),
            F.col("reserved_stock").alias("reserved"),
            F.col("damaged_stock").alias("damaged"),
            F.col("reorder_level").alias("reorder_level"),
        ).alias("stock"),
        F.date_format(F.col("last_updated_ts"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("last_updated"),
        F.col("batch_number"),
        F.current_timestamp().alias("pipeline_loaded_at"),
    )


def write_to_mongodb(df: DataFrame, mongo_uri: str, database: str, collection: str) -> int:
    from pymongo import MongoClient, ReplaceOne

    docs = [json.loads(row) for row in df.toJSON().collect()]
    if not docs:
        return 0

    logging.info("Writing %s records to MongoDB %s.%s", len(docs), database, collection)
    client = MongoClient(mongo_uri)
    coll = client[database][collection]
    operations = [ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs]
    result = coll.bulk_write(operations, ordered=False)
    return result.upserted_count + result.modified_count


def run_pipeline(cfg: PipelineConfig) -> None:
    spark = create_spark()
    try:
        raw_df = load_inventory_source(spark, cfg)
        schema_ok, schema_errors = validate_schema(raw_df)
        if not schema_ok:
            logging.warning("Schema validation mismatches detected: %s", schema_errors)
        else:
            logging.info("Schema validation passed.")

        clean_df, invalid_df = clean_and_validate(raw_df)
        transformed_df = to_mongo_schema(clean_df)

        output_path = cfg.output_json
        output_path.parent.mkdir(parents=True, exist_ok=True)
        transformed_df.coalesce(1).write.mode("overwrite").json(str(output_path))

        invalid_count = invalid_df.count()
        valid_count = transformed_df.count()
        logging.info("Valid rows: %s | Invalid rows quarantined: %s", valid_count, invalid_count)
        logging.info("Transformed dataset written to %s", output_path)

        if cfg.mongo_uri:
            written = write_to_mongodb(transformed_df, cfg.mongo_uri, cfg.mongo_db, cfg.mongo_collection)
            logging.info("MongoDB upsert completed. Records written/updated: %s", written)
        else:
            logging.info("MongoDB URI not provided; skipping load step.")

        summary = {
            "run_at": datetime.now(timezone.utc).isoformat(),
            "source_uri": cfg.source_uri or "synthetic-generator",
            "source_format": cfg.source_format,
            "schema_valid": schema_ok,
            "schema_errors": schema_errors,
            "valid_records": valid_count,
            "invalid_records": invalid_count,
            "mongo_loaded": bool(cfg.mongo_uri),
        }
        summary_file = output_path.parent / "pipeline_summary.json"
        summary_file.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        logging.info("Pipeline summary written to %s", summary_file)
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the PySpark inventory ingestion pipeline.")
    parser.add_argument("--source-uri", default=None, help="Remote/local inventory source URI. If omitted, synthetic data is generated.")
    parser.add_argument("--source-format", default="csv", choices=["csv", "json", "parquet"], help="Input source format.")
    parser.add_argument("--output-json", default="data_pipeline/data/processed_inventory", help="Output path for transformed JSON data.")
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI"), help="MongoDB connection URI.")
    parser.add_argument("--mongo-db", default="enterprise_ai_commerce", help="MongoDB database.")
    parser.add_argument("--mongo-collection", default="inventory", help="MongoDB collection.")
    parser.add_argument("--synth-records", type=int, default=1000, help="Number of generated records when source-uri is omitted.")
    parser.add_argument("--log-file", default="data_pipeline/logs/inventory_pipeline.log", help="Pipeline execution log path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging(Path(args.log_file))

    cfg = PipelineConfig(
        source_uri=args.source_uri,
        source_format=args.source_format,
        output_json=Path(args.output_json),
        mongo_uri=args.mongo_uri,
        mongo_db=args.mongo_db,
        mongo_collection=args.mongo_collection,
        synth_records=args.synth_records,
    )
    run_pipeline(cfg)


if __name__ == "__main__":
    main()
