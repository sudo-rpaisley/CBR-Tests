from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


DEFAULT_OUTCOMES_DIR = Path("outcomes")
DEFAULT_OUTPUT_DIR = Path("graph_data")


def load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def export_outcomes(outcomes_dir: Path, output_dir: Path) -> dict[str, Path]:
    """Flatten saved outcome JSON files into analysis-friendly CSV tables."""

    outcomes_dir = outcomes_dir.expanduser().resolve()
    output_dir = output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    summary_rows = []
    metric_rows = []
    column_quality_rows = []
    pearson_summary_rows = []
    pearson_pair_rows = []
    timestamp_rows = []
    protocol_rows = []

    for json_file in sorted(outcomes_dir.glob("*.json")):
        data = load_json(json_file)

        case_id = data.get("case_id")
        plan_id = data.get("plan_id")
        dataset_path = data.get("dataset_path")
        status = data.get("status")
        metric_ids = data.get("metric_ids", [])

        summary_rows.append(
            {
                "file_name": json_file.name,
                "case_id": case_id,
                "plan_id": plan_id,
                "dataset_path": dataset_path,
                "status": status,
                "metric_ids": "|".join(metric_ids) if isinstance(metric_ids, list) else metric_ids,
            }
        )

        for metric_result in data.get("metric_results", []):
            metric_rows.append(
                {
                    "file_name": json_file.name,
                    "case_id": case_id,
                    "plan_id": plan_id,
                    "dataset_path": dataset_path,
                    "metric_id": metric_result.get("metric_id"),
                    "metric_status": metric_result.get("status"),
                    "error": metric_result.get("error"),
                }
            )

        test_results = data.get("test_results", {})

        if "column_quality_profile" in test_results:
            summary = test_results["column_quality_profile"].get("summary", {})
            column_quality_rows.append(
                {
                    "file_name": json_file.name,
                    "case_id": case_id,
                    "plan_id": plan_id,
                    "dataset_path": dataset_path,
                    "field_count": summary.get("field_count"),
                    "usable_field_count": summary.get("usable_field_count"),
                    "constant_field_count": summary.get("constant_field_count"),
                    "missing_field_count": summary.get("missing_field_count"),
                    "mean_non_null_ratio": summary.get("mean_non_null_ratio"),
                    "mean_numeric_non_null_ratio": summary.get("mean_numeric_non_null_ratio"),
                    "mean_unique_ratio": summary.get("mean_unique_ratio"),
                    "quality_score": summary.get("quality_score"),
                }
            )

        if "pearson_correlation_profile" in test_results:
            summary = test_results["pearson_correlation_profile"].get("summary", {})
            pearson_summary_rows.append(
                {
                    "file_name": json_file.name,
                    "case_id": case_id,
                    "plan_id": plan_id,
                    "dataset_path": dataset_path,
                    "pair_count": summary.get("pair_count"),
                    "mean_absolute_correlation": summary.get("mean_absolute_correlation"),
                }
            )
            for pair in summary.get("pairs", []):
                fields = pair.get("fields", [])
                pearson_pair_rows.append(
                    {
                        "file_name": json_file.name,
                        "case_id": case_id,
                        "plan_id": plan_id,
                        "dataset_path": dataset_path,
                        "field_a": fields[0] if len(fields) > 0 else None,
                        "field_b": fields[1] if len(fields) > 1 else None,
                        "value": pair.get("value"),
                        "overlap_non_null_count": pair.get("overlap_non_null_count"),
                    }
                )

        if "timestamp_coherence_profile" in test_results:
            timestamp = test_results["timestamp_coherence_profile"]
            timestamp_rows.append(
                {
                    "file_name": json_file.name,
                    "case_id": case_id,
                    "plan_id": plan_id,
                    "dataset_path": dataset_path,
                    "packet_count": timestamp.get("packet_count"),
                    "capture_duration_seconds": timestamp.get("capture_duration_seconds"),
                    "backwards_jump_count": timestamp.get("backwards_jump_count"),
                    "zero_delta_count": timestamp.get("zero_delta_count"),
                    "large_gap_count": timestamp.get("large_gap_count"),
                    "large_gap_threshold_seconds": timestamp.get("large_gap_threshold_seconds"),
                    "gap_count": timestamp.get("gap_count"),
                    "mean_gap_seconds": timestamp.get("mean_gap_seconds"),
                    "max_gap_seconds": timestamp.get("max_gap_seconds"),
                    "status": timestamp.get("status"),
                }
            )

        if "protocol_validity_profile" in test_results:
            protocol = test_results["protocol_validity_profile"]
            protocol_rows.append(
                {
                    "file_name": json_file.name,
                    "case_id": case_id,
                    "plan_id": plan_id,
                    "dataset_path": dataset_path,
                    "packet_count": protocol.get("packet_count"),
                    "valid_packet_count": protocol.get("valid_packet_count"),
                    "protocol_validity_ratio": protocol.get("protocol_validity_ratio"),
                    "invalid_ip_count": protocol.get("invalid_ip_count"),
                    "invalid_port_count": protocol.get("invalid_port_count"),
                    "protocol_mismatch_count": protocol.get("protocol_mismatch_count"),
                    "zero_length_packet_count": protocol.get("zero_length_packet_count"),
                    "suspicious_tcp_flag_count": protocol.get("suspicious_tcp_flag_count"),
                    "status": protocol.get("status"),
                }
            )

    tables = {
        "outcome_summary.csv": summary_rows,
        "metric_results.csv": metric_rows,
        "column_quality_summary.csv": column_quality_rows,
        "pearson_summary.csv": pearson_summary_rows,
        "pearson_pairs.csv": pearson_pair_rows,
        "timestamp_summary.csv": timestamp_rows,
        "protocol_summary.csv": protocol_rows,
    }
    written: dict[str, Path] = {}
    for filename, rows in tables.items():
        destination = output_dir / filename
        pd.DataFrame(rows).to_csv(destination, index=False)
        written[filename] = destination
    return written


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export CBR-Tests outcome JSON files into CSV tables for analysis and graphing.")
    parser.add_argument(
        "--outcomes-dir",
        type=Path,
        default=DEFAULT_OUTCOMES_DIR,
        help="Directory containing outcome JSON files (default: outcomes)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Destination directory for generated CSV tables (default: graph_data)",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    written = export_outcomes(args.outcomes_dir, args.output_dir)
    print(f"Wrote {len(written)} graph/analysis tables to: {args.output_dir.expanduser().resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
