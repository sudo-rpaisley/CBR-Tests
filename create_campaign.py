from __future__ import annotations

import argparse
from pathlib import Path

from runner.campaign import build_campaign, write_campaign


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create an ordered queue of CBR-Tests batch/comparison matrices."
    )
    parser.add_argument("--name", required=True, help="Readable campaign name")
    parser.add_argument(
        "--batch",
        action="append",
        required=True,
        help="Batch manifest to add to the campaign queue; repeat in the order to run",
    )
    parser.add_argument("--description", default="", help="Optional campaign description")
    parser.add_argument("--output", type=Path, help="Campaign manifest path")
    parser.add_argument("--force", action="store_true", help="Replace an existing campaign manifest")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parent
    batch_paths = [
        path.expanduser().resolve() if path.expanduser().is_absolute() else (repo_root / path).resolve()
        for path in map(Path, args.batch)
    ]
    payload = build_campaign(
        name=args.name,
        batch_paths=batch_paths,
        repo_root=repo_root,
        description=args.description,
    )
    campaign_id = payload["campaign_meta"]["campaign_id"]
    output = args.output or Path("campaigns") / f"{campaign_id}_campaign.json"
    if not output.is_absolute():
        output = repo_root / output
    written = write_campaign(output, payload, overwrite=args.force)

    print(f"Campaign written: {written}")
    print(f"Matrices queued: {payload['campaign_meta']['matrix_count']}")
    print(f"Comparison jobs queued: {payload['campaign_meta']['job_count']}")
    print(f"Run with: python run_campaign.py --campaign {written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
