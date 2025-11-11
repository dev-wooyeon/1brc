#!/usr/bin/env python3
"""DuckDB OLAP stage reading Parquet output from the ETL phase.

Example:
    python olap_duckdb.py --parquet-dir data/parquet --output results/duckdb_agg.parquet
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any, Dict

import duckdb

AGG_QUERY = """
SELECT
    city,
    MIN(temperature) AS min_temp,
    MAX(temperature) AS max_temp,
    AVG(temperature) AS avg_temp,
    COUNT(*) AS measurements
FROM measurements
GROUP BY city
ORDER BY city
"""


def run_olap(
    parquet_dir: str,
    threads: int = 4,
    output_path: str | None = None,
) -> Dict[str, Any]:
    parquet_glob = str(Path(parquet_dir).absolute() / "*.parquet")

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={max(threads, 1)}")

    load_start = time.perf_counter()
    con.execute("CREATE OR REPLACE TEMP TABLE measurements AS SELECT * FROM read_parquet(?)", [parquet_glob])
    load_time = time.perf_counter() - load_start

    read_start = time.perf_counter()
    agg_df = con.execute(AGG_QUERY).df()
    read_time = time.perf_counter() - read_start

    if output_path:
        out_path = Path(output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        sanitized_path = out_path.as_posix().replace("'", "''")
        copy_sql = (
            f"COPY ({AGG_QUERY}) TO '{sanitized_path}' "
            "(FORMAT 'parquet', COMPRESSION 'zstd')"
        )
        con.execute(copy_sql)

    con.close()

    return {
        "load_time_sec": load_time,
        "read_time_sec": read_time,
        "rows": len(agg_df),
        "output_path": str(output_path) if output_path else None,
        "engine": "duckdb",
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DuckDB OLAP groupby")
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
