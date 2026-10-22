"""Index, inspect and browse complete CBR-Tests GAN experiment results."""
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from runner.results_catalogue import (
    compare_metric, export_parquet, index_roots, open_catalogue, summary,
)


def arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DuckDB/Parquet CBR experiment results archive")
    parser.add_argument("--db", type=Path, default=Path("outcomes/results-catalogue.duckdb"))
    sub = parser.add_subparsers(dest="action", required=True)
    index = sub.add_parser("index", help="Index existing recursive and single GAN runs")
    index.add_argument("--search-root", type=Path, action="append", required=True,
                       help="Root folder containing experiment.json/campaign.json (repeatable)")
    index.add_argument("--parquet-dir", type=Path, default=Path("outcomes/results-parquet"))
    index.add_argument("--force", action="store_true")
    sub.add_parser("stats", help="Show totals for all catalogue tables")
    sub.add_parser("browse", help="Open the interactive read-only results explorer")
    metric = sub.add_parser("metric", help="Query named metric across runs and dates")
    metric.add_argument("--name", required=True)
    metric.add_argument("--held-out", default="")
    metric.add_argument("--date-from", default="", help="Inclusive UTC ISO date")
    metric.add_argument("--date-to", default="", help="Exclusive UTC ISO date")
    metric.add_argument("--limit", type=int, default=1000)
    metric.add_argument("--offset", type=int, default=0)
    metric.add_argument("--csv", type=Path, help="Export filtered results as CSV")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = arguments(argv)
    if args.action == "browse":
        from runner.results_explorer_tui import launch_results_explorer
        launch_results_explorer(args.db)
        return 0
    conn = open_catalogue(args.db, read_only=args.action != "index")
    try:
        if args.action == "index":
            result = index_roots(conn, args.search_root, force=args.force)
            print(f"Indexed {result['indexed']}; unchanged {result['unchanged']}; failed {result['failed']}")
            for entry in result["reports"]:
                print(f"  {entry['status']:>9}  {entry['path']}" + (
                    f" — {entry['error']}" if entry.get("error") else ""
                ))
            parquet = export_parquet(conn, args.parquet_dir)
            print(f"DuckDB: {args.db.expanduser().resolve()}")
            print(f"Parquet: {parquet['folder']}")
            for table, count in parquet["rows"].items():
                print(f"  {table}: {count:,} rows")
            return 1 if result["failed"] else 0
        if args.action == "stats":
            for key, value in summary(conn).items():
                print(f"{key}: {value:,}")
            return 0
        if args.action == "metric":
            records = compare_metric(
                conn, args.name, held_out=args.held_out,
                date_from=args.date_from, date_to=args.date_to,
                limit=args.limit, offset=args.offset,
            )
            if args.csv:
                dest = args.csv.expanduser().resolve()
                dest.parent.mkdir(parents=True, exist_ok=True)
                if records:
                    with dest.open("w", encoding="utf-8", newline="") as handle:
                        writer = csv.DictWriter(handle, fieldnames=list(records[0]))
                        writer.writeheader()
                        writer.writerows(records)
                else:
                    dest.write_text("", encoding="utf-8")
                print(f"Exported {len(records)} rows: {dest}")
            else:
                for row in records:
                    print(
                        f"{row['started_at']}  {row['held_out_path']}  "
                        f"seed={row['seed']} pct={row['perturbation_pct']} "
                        f"value={row['primary_value']} status={row['domain_status']}"
                    )
                print(f"Rows: {len(records)}")
            return 0
    finally:
        conn.close()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
