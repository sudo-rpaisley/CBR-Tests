import json
from pathlib import Path
import pandas as pd


OUTCOMES_DIR = Path("/home/rpaisley/CBR_Tests/outcomes")
OUTPUT_DIR = Path("/home/rpaisley/CBR_Tests/graph_data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    summary_rows = []
    metric_rows = []
    column_quality_rows = []
    pearson_summary_rows = []
    pearson_pair_rows = []
    timestamp_rows = []
    protocol_rows = []

    for json_file in sorted(OUTCOMES_DIR.glob("*.json")):
        data = load_json(json_file)

        case_id = data.get("case_id")
        plan_id = data.get("plan_id")
        dataset_path = data.get("dataset_path")
        status = data.get("status")
        metric_ids = data.get("metric_ids", [])

        summary_rows.append({
            "file_name": json_file.name,
            "case_id": case_id,
            "plan_id": plan_id,
            "dataset_path": dataset_path,
            "status": status,
            "metric_ids": "|".join(metric_ids) if isinstance(metric_ids, list) else metric_ids
        })

        for mr in data.get("metric_results", []):
            metric_rows.append({
                "file_name": json_file.name,
                "case_id": case_id,
                "plan_id": plan_id,
                "dataset_path": dataset_path,
                "metric_id": mr.get("metric_id"),
                "metric_status": mr.get("status"),
                "error": mr.get("error")
            })

        test_results = data.get("test_results", {})

        if "column_quality_profile" in test_results:
            cq = test_results["column_quality_profile"]
            summary = cq.get("summary", {})

            column_quality_rows.append({
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
                "quality_score": summary.get("quality_score")
            })

        if "pearson_correlation_profile" in test_results:
            pp = test_results["pearson_correlation_profile"]
            summary = pp.get("summary", {})

            pearson_summary_rows.append({
                "file_name": json_file.name,
                "case_id": case_id,
                "plan_id": plan_id,
                "dataset_path": dataset_path,
                "pair_count": summary.get("pair_count"),
                "mean_absolute_correlation": summary.get("mean_absolute_correlation")
            })

            for pair in summary.get("pairs", []):
                fields = pair.get("fields", [])
                pearson_pair_rows.append({
                    "file_name": json_file.name,
                    "case_id": case_id,
                    "plan_id": plan_id,
                    "dataset_path": dataset_path,
                    "field_a": fields[0] if len(fields) > 0 else None,
                    "field_b": fields[1] if len(fields) > 1 else None,
                    "value": pair.get("value"),
                    "overlap_non_null_count": pair.get("overlap_non_null_count")
                })

        if "timestamp_coherence_profile" in test_results:
            ts = test_results["timestamp_coherence_profile"]
            timestamp_rows.append({
                "file_name": json_file.name,
                "case_id": case_id,
                "plan_id": plan_id,
                "dataset_path": dataset_path,
                "packet_count": ts.get("packet_count"),
                "capture_duration_seconds": ts.get("capture_duration_seconds"),
                "backwards_jump_count": ts.get("backwards_jump_count"),
                "zero_delta_count": ts.get("zero_delta_count"),
                "large_gap_count": ts.get("large_gap_count"),
                "large_gap_threshold_seconds": ts.get("large_gap_threshold_seconds"),
                "gap_count": ts.get("gap_count"),
                "mean_gap_seconds": ts.get("mean_gap_seconds"),
                "max_gap_seconds": ts.get("max_gap_seconds"),
                "status": ts.get("status")
            })

        if "protocol_validity_profile" in test_results:
            pv = test_results["protocol_validity_profile"]
            protocol_rows.append({
                "file_name": json_file.name,
                "case_id": case_id,
                "plan_id": plan_id,
                "dataset_path": dataset_path,
                "packet_count": pv.get("packet_count"),
                "valid_packet_count": pv.get("valid_packet_count"),
                "protocol_validity_ratio": pv.get("protocol_validity_ratio"),
                "invalid_ip_count": pv.get("invalid_ip_count"),
                "invalid_port_count": pv.get("invalid_port_count"),
                "protocol_mismatch_count": pv.get("protocol_mismatch_count"),
                "zero_length_packet_count": pv.get("zero_length_packet_count"),
                "suspicious_tcp_flag_count": pv.get("suspicious_tcp_flag_count"),
                "status": pv.get("status")
            })

    pd.DataFrame(summary_rows).to_csv(OUTPUT_DIR / "outcome_summary.csv", index=False)
    pd.DataFrame(metric_rows).to_csv(OUTPUT_DIR / "metric_results.csv", index=False)
    pd.DataFrame(column_quality_rows).to_csv(OUTPUT_DIR / "column_quality_summary.csv", index=False)
    pd.DataFrame(pearson_summary_rows).to_csv(OUTPUT_DIR / "pearson_summary.csv", index=False)
    pd.DataFrame(pearson_pair_rows).to_csv(OUTPUT_DIR / "pearson_pairs.csv", index=False)
    pd.DataFrame(timestamp_rows).to_csv(OUTPUT_DIR / "timestamp_summary.csv", index=False)
    pd.DataFrame(protocol_rows).to_csv(OUTPUT_DIR / "protocol_summary.csv", index=False)

    print(f"Wrote graph data to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()