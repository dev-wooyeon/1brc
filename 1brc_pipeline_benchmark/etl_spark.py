#!/usr/bin/env python3
"""Spark-based ETL stage: CSV -> Parquet (zstd).

Example:
    python etl_spark.py --csv-dir data/raw --parquet-dir data/parquet --threads 8

Adjust schema/columns to match your 1BRC variant before running in production.
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any, Dict

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


def _build_session(app_name: str, threads: int) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(f"local[{threads}]")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def run_etl(
    csv_dir: str,
    parquet_dir: str,
    threads: int = 4,
    overwrite: bool = True,
) -> Dict[str, Any]:
    spark = _build_session("1BRC-Spark-ETL", max(threads, 1))
    csv_path = str(csv_dir)
    parquet_path = str(parquet_dir)
    mode = "overwrite" if overwrite else "errorifexists"

    start = time.perf_counter()
    schema = StructType(
        [
            StructField("city", StringType(), nullable=False),
            StructField("temperature", DoubleType(), nullable=True),
        ]
    )

    df = (
        spark.read.option("header", "false")
        .option("delimiter", ";")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(csv_path)
    )

    # NOTE: Customize schema casting here if your CSV has typed columns.
    df.write.mode(mode).option("compression", "zstd").parquet(parquet_path)
    write_elapsed = time.perf_counter() - start

    record_count = df.count()  # Triggers an action for logging in metrics.

    spark.stop()
    return {
        "etl_time_sec": write_elapsed,
        "records": record_count,
        "parquet_dir": parquet_path,
        "engine": "spark",
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark CSV->Parquet ETL")
    parser.add_argument("--csv-dir", required=True)
    parser.add_argument("--parquet-dir", required=True)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--no-overwrite", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    result = run_etl(
        csv_dir=args.csv_dir,
        parquet_dir=args.parquet_dir,
        threads=args.threads,
        overwrite=not args.no_overwrite,
    )
    print(result)


if __name__ == "__main__":
    main()
