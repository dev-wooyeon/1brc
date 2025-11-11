#!/usr/bin/env python3
"""ClickHouse OLAP stage using clickhouse-local over Parquet files.

Example:
    python olap_clickhouse.py --parquet-dir data/parquet --output results/clickhouse_agg.parquet

This module shells out to `clickhouse-local`, so ensure it is installed and discoverable
on PATH. The Python dependency on `clickhouse-connect` is kept for future extensions.
"""
from __future__ import annotations

import argparse
import subprocess
import time
from pathlib import Path
from typing import Any, Dict

QUERY_TEMPLATE = """
SELECT
    city,
    min(temperature) AS min_temp,
    max(temperature) AS max_temp,
    avg(temperature) AS avg_temp,
    count() AS measurements
FROM file('{parquet_glob}', 'Parquet')
GROUP BY city
ORDER BY city
FORMAT Parquet
"""


def run_olap(
    parquet_dir: str,
    threads: int = 4,
    output_path: str | None = None,
) -> Dict[str, Any]:
    parquet_glob = str(Path(parquet_dir).absolute() / "*.parquet")
    query = QUERY_TEMPLATE.format(parquet_glob=parquet_glob.replace("'", "\\'"))

    load_time_sec = 0.0  # Placeholder: clickhouse-local streams directly without preload.

    read_start = time.perf_counter()
    proc = subprocess.run(
        [
            "clickhouse",
            "--max_threads",
            str(max(threads, 1)),
            "--query",
            query,
        ],
        check=True,
        capture_output=True,
    )
    read_time_sec = time.perf_counter() - read_start

    if output_path:
        out_path = Path(output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(proc.stdout)

    return {
        "load_time_sec": load_time_sec,
        "read_time_sec": read_time_sec,
        "output_path": str(output_path) if output_path else None,
        "engine": "clickhouse-local",
        "stderr": proc.stderr.decode("utf-8", errors="ignore"),
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ClickHouse Parquet groupby")
    parser.add_argument("--parquet-dir", required=True)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--output", required=False)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    result = run_olap(
        parquet_dir=args.parquet_dir,
        threads=args.threads,
        output_path=args.output,
    )
    print(result)


if __name__ == "__main__":
    main()
