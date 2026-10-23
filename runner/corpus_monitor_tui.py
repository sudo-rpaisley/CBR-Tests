"""Live curses monitor for GAN campaign, group, case, job and partial results."""
from __future__ import annotations

import curses
import json
from pathlib import Path
import subprocess
from typing import Any

from runner.corpus_progress import format_duration, progress_line
from runner.friendly_tui import _safe_addstr


def read_progress(output_dir: Path) -> dict:
    try:
        data = json.loads((Path(output_dir) / "progress.json").read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _log_path(output_dir: Path) -> Path:
    return Path(output_dir).with_suffix(".log")


def _tail(path: Path, limit: int = 18000) -> str:
    if not path.is_file():
        return f"No log file yet at {path}"
    with path.open("rb") as handle:
        handle.seek(0, 2)
        handle.seek(max(0, handle.tell() - limit))
        return handle.read().decode("utf-8", errors="replace")


def monitor_lines(progress: dict, *, process_exit: int | None = None) -> list[str]:
    """Pure renderer, available to tests without an interactive terminal."""
    if not progress:
        return [
            "Preparing campaign inventory / hashing inputs...",
            "Progress details appear when campaign.json has been written.",
        ]
    camp = progress["campaign"]
    group = progress.get("group")
    case = progress.get("case")
    job = progress.get("job")
    groups = camp["total_groups"]
    lines = [
        f"Status: {progress['status']} | Groups: {groups} | "
        f"Elapsed: {format_duration(progress.get('elapsed_seconds'))}",
        "CAMPAIGN: " + progress_line(camp["evaluated_jobs"], camp["total_jobs"]),
        f"  Held-outs: {camp['completed_cases']}/{camp['total_cases']} completed "
        f"| failed: {camp['failed_cases']} | Approx ETA: {format_duration(camp['eta_seconds'])}",
        f"  Finished job outcomes available for exploration: {camp['available_result_jobs']}",
        "",
    ]
    if group:
        lines.extend([
            f"GROUP: {group['name']}",
            "  " + progress_line(group["evaluated_jobs"], group["total_jobs"]),
            f"  Held-outs: {group['completed_cases']}/{group['total_cases']} "
            f"| failed: {group['failed_cases']} | Approx ETA: {format_duration(group['eta_seconds'])}",
            "",
        ])
    if case:
        details = case.get("stage_details", {})
        stage = case["stage"].replace("_", " ").title()
        if case["stage"] == "extracting":
            stage += f" {details.get('capture_index', '?')}/{details.get('capture_total', '?')}"
        if case["stage"] == "training":
            stage += f" ({details.get('epochs', '?')} planned epochs)"
        lines.extend([
            f"HELD-OUT PCAP: {case['name']}",
            "  " + progress_line(case["evaluated_jobs"], case["total_jobs"]),
            f"  Stage: {stage} | Seed: {case.get('seed') if case.get('seed') is not None else '-'}",
            f"  Elapsed: {format_duration(case['elapsed_seconds'])} "
            f"| Approx ETA: {format_duration(case['eta_seconds'])}",
            f"  Available: {case['available_result_jobs']} jobs "
            f"| failed: {case['failed_jobs']}",
            "",
        ])
    if job:
        lines.extend([
            f"CURRENT JOB: {job['name']}",
            f"  Comparison {job.get('index')}/{job.get('total_in_seed')} "
            f"| Seed {job.get('seed')} | GAN {job.get('perturbation_pct')}%",
            f"  Elapsed: {format_duration(job.get('elapsed_seconds'))} "
            f"| Approx ETA: {format_duration(job.get('eta_seconds'))}",
            f"  ETA based on {job.get('observed_job_samples', 0)} observed jobs in this held-out case.",
        ])
    else:
        lines.append("CURRENT JOB: Not evaluating (may be training/generating/validating).")
    if process_exit is not None:
        lines.append(f"Runner exited with code {process_exit}. Existing results remain available.")
    return lines


def _open_results(stdscr, output_dir: Path, repo_root: Path) -> str:
    """Index already-written outcomes without stopping the experiment."""
    from runner.results_catalogue import index_roots, open_catalogue
    from runner.results_explorer_tui import _screen

    if not (output_dir / "campaign.json").is_file():
        return "Campaign is still preparing; no indexed outcomes yet."
    db = repo_root / "outcomes" / "results-catalogue.duckdb"
    try:
        conn = open_catalogue(db)
        try:
            result = index_roots(conn, [output_dir])
            if result["failed"]:
                first = next(x for x in result["reports"] if x["status"] == "failed")
                return f"Catalogue indexing error: {first.get('error', 'unknown')}"
            _screen(stdscr, conn)
        finally:
            conn.close()
    except (OSError, ValueError, RuntimeError) as exc:
        return f"Results explorer unavailable: {exc}"
    return "Returned from available-results explorer. Campaign continues independently."


def _monitor_screen(stdscr, output_dir: Path, repo_root: Path, process) -> None:
    curses.curs_set(0)
    stdscr.timeout(1000)
    message = "q detach  v explore available results now  l view log  r refresh  (auto refresh: 1s)"
    while True:
        progress = read_progress(output_dir)
        exited = process.poll() if process is not None else None
        rows = monitor_lines(progress, process_exit=exited)
        height, _ = stdscr.getmaxyx()
        stdscr.erase()
        _safe_addstr(stdscr, 0, 0, "CBR-Tests | LIVE GAN CAMPAIGN PROGRESS", curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, f"Campaign: {output_dir.name}")
        _safe_addstr(stdscr, 2, 0, "q detach | v browse completed results | l view log | r refresh")
        for i, line in enumerate(rows[:max(0, height-6)], start=4):
            _safe_addstr(stdscr, i, 0, line)
        _safe_addstr(stdscr, height-1, 0, message)
        key = stdscr.getch()
        if key in (ord("q"), ord("Q"), 27):
            return
        if key == ord("r") or key == -1:
            continue
        if key == ord("v"):
            message = _open_results(stdscr, output_dir, repo_root)
            continue
        if key == ord("l"):
            from runner.results_explorer_tui import _detail
            _detail(stdscr, f"Campaign log: {_log_path(output_dir)}",
                    _tail(_log_path(output_dir)))
            message = "Showing latest runner log. Use 'v' for completed measurements."
            continue


def launch_corpus_monitor(
    output_dir: Path, *, repo_root: Path | None = None, process=None,
) -> None:
    """Attach or reattach to a running campaign; 'q' never kills the runner."""
    output = Path(output_dir).expanduser().resolve()
    root = (repo_root or Path(__file__).resolve().parents[1]).expanduser().resolve()
    if not output.parent.is_dir():
        raise FileNotFoundError(f"Experiment parent folder missing: {output.parent}")
    curses.wrapper(_monitor_screen, output, root, process)


def run_monitor_tool_action(repo_root: Path) -> None:
    """Select an existing run's folder from the Toolbox, then attach to it."""
    from runner.corpus_perturbation_tui import _select_directory

    root = repo_root.expanduser().resolve()
    target = root / "outcomes" / "gan-corpus"
    if not target.is_dir():
        print(f"No GAN campaigns found in {target}")
        return
    chosen = curses.wrapper(
        _select_directory, root, start_dir=target
    )
    if chosen is None:
        return
    if not (chosen / "campaign.json").is_file():
        print(f"Select a directory containing campaign.json: {chosen}")
        return
    launch_corpus_monitor(chosen, repo_root=root)
