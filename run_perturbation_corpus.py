"""Run a recursive leave-one-out CTGAN research campaign from a single directory."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from runner.corpus_perturbation import run_corpus
from runner.perturbation_experiment import parse_integer_set


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Find PCAPs recursively; for each per-directory group of >=2 PCAPs, "
            "train on all-but-one then compare the held-out PCAP to its GAN "
            "0/10/.../100%% variants. Retain full outcomes and measurements."
        )
    )
    parser.add_argument("--dataset-root", type=Path, required=True,
                        help="Root of recursive PCAP folder tree")
    parser.add_argument("--output-dir", type=Path,
                        help="Campaign output directory; defaults to timestamped outcomes/gan-corpus")
    parser.add_argument("--generator-script", type=Path,
                        default=Path(__file__).resolve().parent / "traffic_gan.py")
    parser.add_argument("--percentages", default="0,10,20,30,40,50,60,70,80,90,100")
    parser.add_argument("--seeds", default="42")
    parser.add_argument("--epochs", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--device", choices=("cpu", "cuda"), default="cpu")
    parser.add_argument("--workers", type=int, default=None)
    parser.add_argument("--size-tolerance-pct", type=float, default=5.0)
    parser.add_argument("--max-consecutive-failures", type=int, default=3,
                        help="Halt after this many consecutive failed held-outs (default: 3)")
    parser.add_argument("--preview", action="store_true", help="List groups/jobs without training or writing outputs")
    parser.add_argument("--resume", action="store_true",
                        help="Resume previously started campaign in --output-dir")
    parser.add_argument("--prepare-only", action="store_true",
                        help="Generate perturbations and matrices, skip metrics")
    args = parser.parse_args(argv)
    try:
        args.percentages = parse_integer_set(args.percentages, low=0, high=100)
        args.seeds = parse_integer_set(args.seeds, low=0, high=2**31 - 1)
        if 0 not in args.percentages:
            parser.error("--percentages must include a 0% self-baseline.")
        if args.epochs < 1 or args.batch_size < 10 or args.batch_size % 10:
            parser.error("--epochs must be positive; --batch-size must be a multiple of 10 (>=10).")
        if args.workers is not None and args.workers < 1:
            parser.error("--workers must be positive.")
        if args.size_tolerance_pct < 0:
            parser.error("--size-tolerance-pct must be nonnegative.")
        if args.max_consecutive_failures < 1:
            parser.error("--max-consecutive-failures must be positive.")
        if args.resume and args.output_dir is None:
            parser.error("--resume requires the existing --output-dir.")
    except ValueError as exc:
        parser.error(str(exc))
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    default_output = (
        Path(__file__).resolve().parent / "outcomes" / "gan-corpus"
        / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    )
    output = args.output_dir or default_output
    result = run_corpus(
        dataset_root=args.dataset_root,
        output_dir=output,
        generator_script=args.generator_script,
        percentages=args.percentages,
        seeds=args.seeds,
        epochs=args.epochs,
        batch_size=args.batch_size,
        device=args.device,
        workers=args.workers,
        size_tolerance_pct=args.size_tolerance_pct,
        max_consecutive_failures=args.max_consecutive_failures,
        prepare_only=args.prepare_only,
        resume=args.resume,
        preview_only=args.preview,
    )
    if args.preview:
        print(f"Eligible folders: {result['eligible_groups']}; held-out cases: {result['eligible_cases']}")
        print(f"Skipped one-file folders: {len(result['skipped_folders'])}")
        for group in result["groups"]:
            print(f"  {group['folder']}: {len(group['files'])} PCAPs; leave-one-out {len(group['files'])} times")
        print(f"Planned comparisons: {result['eligible_cases'] * len(args.seeds) * len(args.percentages)}")
        print(f"Approximate generated PCAP storage: {result['estimated_capture_bytes'] / (1024**3):,.2f} GiB")
        print("This excludes training checkpoints, extraction data, outcomes and result exports.")
    else:
        print(f"Campaign: {output / 'campaign.json'}")
        print(f"Status: {result['status']}")
        print(f"Completed: {result.get('completed_cases', 0)}/{len(result['cases'])} held-out cases.")
        if result.get("exports"):
            print(f"Complete measurements: {result['exports']['measurements_csv']}")
            print(f"Full metric JSONL: {result['exports']['metric_payloads_jsonl']}")
        if result["status"] != "completed":
            print(f"Some cases need attention. Resume using: --resume --output-dir {output}")
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
