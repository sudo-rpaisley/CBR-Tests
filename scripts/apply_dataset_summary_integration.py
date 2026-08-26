from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    file_path = Path(path)
    text = file_path.read_text(encoding="utf-8")
    if old not in text:
        raise SystemExit(f"Expected text not found in {path}: {old[:120]!r}")
    file_path.write_text(text.replace(old, new, 1), encoding="utf-8")


# Fix empty-tabular formatting while integrating the new module.
replace_once(
    "runner/dataset_summary.py",
    '''    if data["missing_cell_count"] is not None:\n        lines.extend(\n            [\n                f"- **Missing cells:** {data['missing_cell_count']:,} ({data['missing_cell_ratio']:.4%})",\n                f"- **Exact duplicate rows:** {data['duplicate_row_count']:,} ({data['duplicate_row_ratio']:.4%})",\n            ]\n        )\n''',
    '''    if data["missing_cell_count"] is not None:\n        missing_ratio = (\n            f"{data['missing_cell_ratio']:.4%}"\n            if data["missing_cell_ratio"] is not None\n            else "n/a"\n        )\n        duplicate_ratio = (\n            f"{data['duplicate_row_ratio']:.4%}"\n            if data["duplicate_row_ratio"] is not None\n            else "n/a"\n        )\n        lines.extend(\n            [\n                f"- **Missing cells:** {data['missing_cell_count']:,} ({missing_ratio})",\n                f"- **Exact duplicate rows:** {data['duplicate_row_count']:,} ({duplicate_ratio})",\n            ]\n        )\n''',
)

# CLI controls.
replace_once(
    "runner/run_plan_helpers.py",
    '''    parser.add_argument("--force-output", action="store_true", help="Allow replacement of an existing output file")\n    parser.add_argument("--field-translation", help="Optional JSON file mapping dataset column names to test field names")\n''',
    '''    parser.add_argument("--force-output", action="store_true", help="Allow replacement of an existing output file")\n    parser.add_argument(\n        "--dataset-summary",\n        action=argparse.BooleanOptionalAction,\n        default=True,\n        help="Create/reuse a hash-validated Markdown dataset summary beside the dataset (default: enabled)",\n    )\n    parser.add_argument(\n        "--refresh-dataset-summary",\n        action="store_true",\n        help="Regenerate the dataset summary even when its stored dataset hash still matches",\n    )\n    parser.add_argument("--field-translation", help="Optional JSON file mapping dataset column names to test field names")\n''',
)

# TUI controls.
replace_once(
    "runner/tui.py",
    '''        TuiField("case_id", "Ad-hoc case ID", "text", args.case_id or "ad_hoc_case", (), "Label written to the outcome when you run a plan directly instead of a case.", "Required inputs"),\n        TuiField("display", "Live display mode", "choice", args.display or "interactive", DISPLAY_MODES, "Choose how much progress detail to show after the run starts.", "Execution"),\n''',
    '''        TuiField("case_id", "Ad-hoc case ID", "text", args.case_id or "ad_hoc_case", (), "Label written to the outcome when you run a plan directly instead of a case.", "Required inputs"),\n        TuiField("dataset_summary", "Dataset summary sidecar", "bool", bool(getattr(args, "dataset_summary", True)), (), "Create or reuse a hash-validated Markdown summary beside the dataset. Enabled by default.", "Dataset summary"),\n        TuiField("refresh_dataset_summary", "Force summary refresh", "bool", bool(getattr(args, "refresh_dataset_summary", False)), (), "Regenerate the dataset summary even when the existing sidecar hash and schema are still current.", "Dataset summary"),\n        TuiField("display", "Live display mode", "choice", args.display or "interactive", DISPLAY_MODES, "Choose how much progress detail to show after the run starts.", "Execution"),\n''',
)

# Runner integration and provenance recording.
replace_once(
    "run_plan.py",
    '''from runner.dataset_loading import is_tabular_dataset, load_shared_tabular_dataset\nfrom runner.dispatch import build_metric_handlers\n''',
    '''from runner.dataset_loading import is_tabular_dataset, load_shared_tabular_dataset\nfrom runner.dataset_summary import ensure_dataset_summary\nfrom runner.dispatch import build_metric_handlers\n''',
)

replace_once(
    "run_plan.py",
    '''    elif is_packet_capture(dataset_path) and any(\n        metric["metric_id"] in PCAP_PACKET_BACKED_METRICS for metric in metrics\n    ):\n        print_phase_status("PCAP", "Building canonical packet view")\n        shared_tabular_df = build_pcap_packet_dataframe(dataset_path)\n\n    def _load_dataset_for_metric(path: Path):\n''',
    '''    elif is_packet_capture(dataset_path) and any(\n        metric["metric_id"] in PCAP_PACKET_BACKED_METRICS for metric in metrics\n    ):\n        print_phase_status("PCAP", "Building canonical packet view")\n        shared_tabular_df = build_pcap_packet_dataframe(dataset_path)\n\n    if getattr(args, "dataset_summary", True):\n        dataset_sha256 = provenance.get("dataset", {}).get("sha256")\n        if dataset_sha256:\n            try:\n                print_phase_status("Dataset summary", "Checking dataset-side summary cache")\n                dataset_summary_info = ensure_dataset_summary(\n                    dataset_path,\n                    dataset_sha256=dataset_sha256,\n                    dataframe=shared_tabular_df,\n                    force=bool(getattr(args, "refresh_dataset_summary", False)),\n                )\n                provenance["dataset_summary"] = dataset_summary_info\n                print_phase_status(\n                    "Dataset summary",\n                    f"{dataset_summary_info['status'].capitalize()}: {dataset_summary_info['path']}",\n                )\n            except (OSError, ValueError) as exc:\n                provenance["dataset_summary"] = {\n                    "status": "error",\n                    "error": str(exc),\n                }\n                print(f"WARNING: Dataset summary could not be created or refreshed: {exc}")\n        else:\n            provenance["dataset_summary"] = {\n                "status": "error",\n                "error": "dataset_sha256_unavailable",\n            }\n    else:\n        provenance["dataset_summary"] = {"status": "suppressed"}\n\n    def _load_dataset_for_metric(path: Path):\n''',
)

# README discoverability.
readme = Path("README.md")
text = readme.read_text(encoding="utf-8")
section = '''\n## Dataset summary sidecars\n\nNormal runs maintain a human-readable `<dataset filename>.summary.md` beside the dataset. The sidecar includes file/hash identity, record and field counts, safe time coverage, and relevant descriptive/network characteristics. Its embedded SHA-256 and summary-schema version are checked on every run, so unchanged datasets reuse the existing summary without an extra dataset scan. Use `--no-dataset-summary` to suppress it or `--refresh-dataset-summary` to rebuild it deliberately. See [Dataset summary sidecars](docs/dataset_summary.md).\n'''
if "## Dataset summary sidecars" not in text:
    readme.write_text(text.rstrip() + "\n" + section, encoding="utf-8")
