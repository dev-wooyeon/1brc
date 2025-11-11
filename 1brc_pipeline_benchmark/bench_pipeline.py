#!/usr/bin/env python3
"""Unified CLI runner for 1BRC ETL/OLAP benchmarks.

Example:
python bench_pipeline.py --pipeline spark_clickhouse \
    --csv-dir ../data/measurements.txt \
    --parquet-dir ../data/parquet \
    --results-dir results \
    --threads 8

This script dynamically imports the requested ETL/OLAP modules via
``importlib`` to avoid Python path conflicts and keeps each engine isolated.
"""
from __future__ import annotations

import argparse
import importlib
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

PIPELINE_REGISTRY = {
    "spark_duckdb": {
        "etl_module": "etl_spark",
        "olap_module": "olap_duckdb",
    },
    "spark_clickhouse": {
        "etl_module": "etl_spark",
        "olap_module": "olap_clickhouse",
    },
    "flink_duckdb": {
        "etl_module": "etl_flink",
        "olap_module": "olap_duckdb",
    },
    "flink_clickhouse": {
        "etl_module": "etl_flink",
        "olap_module": "olap_clickhouse",
    },
}


def _load_module(module_name: str):
    return importlib.import_module(module_name)


def run_benchmark(args: argparse.Namespace) -> Dict[str, Any]:
    config = PIPELINE_REGISTRY[args.pipeline]

    etl_module = _load_module(config["etl_module"])
    olap_module = _load_module(config["olap_module"])

    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    Path(args.parquet_dir).mkdir(parents=True, exist_ok=True)

    # ETL stage
    etl_start = time.perf_counter()
    etl_payload = etl_module.run_etl(
        csv_dir=args.csv_dir,
        parquet_dir=args.parquet_dir,
        threads=args.threads,
        overwrite=not args.no_overwrite,
    )
    etl_time_sec = etl_payload.get("etl_time_sec", time.perf_counter() - etl_start)

    # OLAP stage
    olap_output = results_dir / f"{args.pipeline}_agg.parquet"
    olap_result = olap_module.run_olap(
        parquet_dir=args.parquet_dir,
        threads=args.threads,
        output_path=str(olap_output),
    )

    load_time_sec = olap_result.get("load_time_sec", 0.0)
    read_time_sec = olap_result.get("read_time_sec", 0.0)
    total_time_sec = etl_time_sec + load_time_sec + read_time_sec

    benchmark_record = {
        "pipeline": args.pipeline,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "csv_dir": str(args.csv_dir),
        "parquet_dir": str(args.parquet_dir),
        "results_dir": str(results_dir),
        "etl_time_sec": etl_time_sec,
        "load_time_sec": load_time_sec,
        "read_time_sec": read_time_sec,
        "total_time_sec": total_time_sec,
        "cpu_placeholder": None,  # Reserved for future CPU stats
        "memory_placeholder": None,  # Reserved for future memory stats
        "etl_details": etl_payload,
        "olap_details": olap_result,
    }

    out_path = results_dir / f"{args.pipeline}_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.json"
    out_path.write_text(json.dumps(benchmark_record, indent=2))
    return benchmark_record


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="1BRC pipeline benchmark runner")
    parser.add_argument("--pipeline", choices=PIPELINE_REGISTRY.keys(), required=True)
    parser.add_argument("--csv-dir", default="data/raw", help="Source CSV directory")
    parser.add_argument("--parquet-dir", default="data/parquet", help="ETL output directory")
    parser.add_argument("--results-dir", default="results", help="JSON/Parquet output directory")
    parser.add_argument("--threads", type=int, default=4, help="Engine thread/parallelism hint")
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Do not overwrite existing Parquet output (default overwrites)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    record = run_benchmark(args)
    print(json.dumps(record, indent=2))


if __name__ == "__main__":
    main()
