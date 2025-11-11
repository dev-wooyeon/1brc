#!/usr/bin/env python3
"""Flink Table API ETL stage: CSV -> Parquet (batch mode).

Example:
    python etl_flink.py --csv-dir data/raw --parquet-dir data/parquet --threads 8

Adjust the CREATE TABLE schemas below to match your 1BRC dataset (city, ts, temp).
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any, Dict

from pyflink.table import EnvironmentSettings, TableEnvironment


def _create_table_env(threads: int) -> TableEnvironment:
    settings = EnvironmentSettings.in_batch_mode()
    table_env = TableEnvironment.create(settings)
    table_env.get_config().set("parallelism.default", str(max(threads, 1)))
    return table_env


def run_etl(
    csv_dir: str,
    parquet_dir: str,
    threads: int = 4,
    overwrite: bool = True,
) -> Dict[str, Any]:
    table_env = _create_table_env(threads)
    csv_path = str(Path(csv_dir).absolute())
    parquet_dir_path = Path(parquet_dir).absolute()
    parquet_dir_path.mkdir(parents=True, exist_ok=True)
    parquet_path = str(parquet_dir_path)

    source_stmt = f"""
    CREATE TABLE measurements_src (
        city STRING,
        measurement_time TIMESTAMP(3),
        temperature DOUBLE
    ) WITH (
        'connector' = 'filesystem',
        'path' = '{csv_path}',
        'format' = 'csv',
        'csv.field-delimiter' = ';',
        'csv.ignore-parse-errors' = 'true'
    )
    """

    sink_mode = 'OVERWRITE' if overwrite else 'INTO'
    sink_stmt = f"""
    CREATE TABLE measurements_sink (
        city STRING,
        measurement_time TIMESTAMP(3),
        temperature DOUBLE
    ) WITH (
        'connector' = 'filesystem',
        'path' = '{parquet_path}',
        'format' = 'parquet'
    )
    """

    table_env.execute_sql("DROP TABLE IF EXISTS measurements_src")
    table_env.execute_sql("DROP TABLE IF EXISTS measurements_sink")
    table_env.execute_sql(source_stmt)
    table_env.execute_sql(sink_stmt)

    start = time.perf_counter()
    insert_sql = f"INSERT {sink_mode} measurements_sink SELECT city, measurement_time, temperature FROM measurements_src"
    result = table_env.execute_sql(insert_sql)
    try:
        result.wait()
    except AttributeError:
        job_client = result.get_job_client()
        if job_client is not None:
            job_client.get_job_execution_result().result()
    elapsed = time.perf_counter() - start

    return {
        "etl_time_sec": elapsed,
        "records": None,  # Flink Table API does not expose row count cheaply; fill later if needed.
        "parquet_dir": parquet_path,
        "engine": "flink",
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Flink CSV->Parquet ETL")
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
