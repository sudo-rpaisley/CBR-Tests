"""CLI entry point for GAN suffix generation plus frozen-plan CBR evaluation."""
from __future__ import annotations

import argparse
from pathlib import Path

from runner.perturbation_experiment import parse_integer_set, run_experiment


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate 0-100% CTGAN trailing-suffix PCAPs, validate them and evaluate with CBR-Tests."
    )
    parser.add_argument("--base", type=Path, required=True, help="Dataset 1: unmodified reference PCAP/PCAPNG")
    parser.add_argument("--comparison", "--input", dest="comparison", type=Path, required=True,
                        help="Dataset 2: original comparison PCAP/PCAPNG; trailing packets are perturbed")
    parser.add_argument("--plan", type=Path, required=True, help="Existing fixed PCAP test plan JSON")
    model_group = parser.add_mutually_exclusive_group(required=True)
    model_group.add_argument("--model", type=Path,
                             help="Existing locally trained, trusted CTGAN checkpoint")
    model_group.add_argument("--train-pcap", type=Path, action="append", dest="training_pcaps",
                             help="Training PCAP/PCAPNG; repeat to train from multiple independent captures")
    parser.add_argument("--training-epochs", type=int, default=100)
    parser.add_argument("--training-batch-size", type=int, default=100)
    parser.add_argument("--training-device", choices=("cpu", "cuda"), default="cpu")
    parser.add_argument("--generator-script", type=Path,
                        default=Path(__file__).resolve().parent / "traffic_gan.py",
                        help="Optional generator script (defaults to bundled traffic_gan.py)")
    parser.add_argument("--output-dir", type=Path, required=True, help="New empty output directory")
    parser.add_argument("--percentages", default="0,10,20,30,40,50,60,70,80,90,100")
    parser.add_argument("--seeds", default="42", help="Comma-separated random seeds")
    parser.add_argument("--basis", choices=("packets", "packet-bytes"), default="packets")
    parser.add_argument("--size-tolerance-pct", type=float, default=5.0,
                        help="Maximum absolute file-size difference as %% of source (default 5)")
    parser.add_argument("--workers", type=int, default=None)
    parser.add_argument("--display", choices=("quiet", "compact", "full", "interactive"), default="quiet")
    parser.add_argument("--prepare-only", action="store_true", help="Generate captures and matrices without running metrics")
    parser.add_argument("--experiment-mode", action="store_true",
                        help="Enforce CBR-Tests strict final-experiment plan contract")
    args = parser.parse_args(argv)
    try:
        args.percentages = parse_integer_set(args.percentages, low=0, high=100)
        args.seeds = parse_integer_set(args.seeds, low=0, high=2**31 - 1)
        if 0 not in args.percentages:
            parser.error("Include 0 in --percentages for the unmodified baseline.")
        if args.training_epochs < 1:
            parser.error("--training-epochs must be positive.")
        if args.training_batch_size < 10 or args.training_batch_size % 10:
            parser.error("--training-batch-size must be a multiple of 10 and at least 10.")
        if args.workers is not None and args.workers < 1:
            parser.error("--workers must be positive.")
        if args.size_tolerance_pct < 0:
            parser.error("--size-tolerance-pct cannot be negative.")
    except ValueError as exc:
        parser.error(str(exc))
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_experiment(
        base=args.base, comparison=args.comparison, plan=args.plan, model=args.model,
        training_pcaps=tuple(args.training_pcaps or ()),
        training_epochs=args.training_epochs, training_batch_size=args.training_batch_size,
        training_device=args.training_device,
        generator_script=args.generator_script, output_dir=args.output_dir,
        percentages=args.percentages, seeds=args.seeds, basis=args.basis,
        workers=args.workers, display=args.display,
        size_tolerance_pct=args.size_tolerance_pct,
        prepare_only=args.prepare_only, experiment_mode=args.experiment_mode,
    )
    print("Status:", result["status"])
    print("Experiment manifest:", args.output_dir / "experiment.json")
    if not args.prepare_only:
        print("Metric results:", result["metrics_csv"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
