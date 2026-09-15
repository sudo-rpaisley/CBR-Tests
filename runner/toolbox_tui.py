from __future__ import annotations

import curses
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cbr_tests.metric_catalog import available_metric_ids
from runner.friendly_tui import (
    _choice_dialog,
    _edit_text,
    _initialise_colours,
    _safe_addstr,
    _status_attr,
)
from runner.plan_builder import write_plan
from runner.tui import _display_path, list_file_browser_entries
from runner.tui_batch import (
    SUPPORTED_DATASET_SUFFIXES,
    _format_list,
    _multi_file_browser,
    comparison_job_count,
)


@dataclass(frozen=True)
class ToolboxItem:
    key: str
    title: str
    description: str
    group: str


TOOLBOX_ITEMS = (
    ToolboxItem("single", "Run one dataset", "Run a prepared case or plan against one dataset.", "Run experiments"),
    ToolboxItem("batch", "Run batch / comparison", "Build and execute a candidate/reference experiment matrix.", "Run experiments"),
    ToolboxItem("build_plan", "Build plan / batch definition", "Create a dataset-aware plan containing the tests that can actually run.", "Prepare experiments"),
    ToolboxItem("validate_plan", "Validate plan", "Check a saved plan against the current schema and metric registry.", "Prepare experiments"),
    ToolboxItem("migrate_plan", "Migrate legacy plan IDs", "Rewrite safe legacy metric IDs to their canonical experiment IDs.", "Prepare experiments"),
    ToolboxItem("compare_outcomes", "Compare outcomes", "Compare two outcome JSON files and highlight meaningful changes.", "Review results"),
    ToolboxItem("rerun_compare", "Rerun and compare", "Archive a baseline, rerun a plan and build a reproducibility comparison record.", "Review results"),
    ToolboxItem("export_graphs", "Export graph / analysis tables", "Flatten outcome JSON files into CSV tables for analysis and plotting.", "Review results"),
    ToolboxItem("list_metrics", "Browse metric catalogue", "List every metric currently discoverable by the plan builder.", "Reference"),
    ToolboxItem("docs_check", "Check generated documentation", "Verify that generated function/test references are current.", "Maintenance"),
    ToolboxItem("docs_inventory", "Build documentation inventory", "Regenerate the repository documentation inventory.", "Maintenance"),
)


_PLAN_ESSENTIAL_FIELDS = ("name", "datasets", "references", "metric_policy")
_PLAN_ADVANCED_FIELDS = (
    "description",
    "field_translation",
    "include_metrics",
    "exclude_metrics",
    "single_service",
    "expected_ports",
    "output",
    "force",
)


def toolbox_items() -> tuple[ToolboxItem, ...]:
    return TOOLBOX_ITEMS


def _json_browser(stdscr, root: Path, *, title: str, initial_dir: str) -> str | None:
    root = root.expanduser().resolve()
    current = root / initial_dir
    if not current.is_dir():
        current = root
    current = current.resolve()
    selected = 0
    message = "↑/↓ move   Enter open/select   Backspace parent   e path   R repo   H home   q cancel"
    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        entries = [
            entry
            for entry in list_file_browser_entries(current, root)
            if entry.is_dir or entry.path.suffix.lower() == ".json"
        ]
        selected = min(selected, max(0, len(entries) - 1))
        _safe_addstr(stdscr, 0, 0, title, curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, message)
        _safe_addstr(stdscr, 2, 0, f"Directory: {_display_path(current, root)}")
        visible = max(1, height - 5)
        start = min(max(0, selected - visible + 1), max(0, len(entries) - visible))
        for row, entry in enumerate(entries[start : start + visible], start=4):
            index = start + row - 4
            marker = ">" if index == selected else " "
            attr = curses.A_REVERSE if index == selected else curses.A_NORMAL
            _safe_addstr(stdscr, row, 0, f"{marker} {entry.label}", attr)
        if not entries:
            _safe_addstr(stdscr, 4, 0, "No JSON files in this directory.")

        key = stdscr.getch()
        if key in (ord("q"), ord("Q"), 27):
            return None
        if key == ord("R"):
            current = root
            selected = 0
            continue
        if key == ord("H"):
            current = Path.home().resolve()
            selected = 0
            continue
        if key == ord("e"):
            typed = _edit_text(stdscr, min(height - 2, 4), 0, str(current), max(8, width - 1)).strip()
            if not typed:
                continue
            path = Path(typed).expanduser()
            if not path.is_absolute():
                path = current / path
            path = path.resolve()
            if path.is_dir():
                current = path
                selected = 0
            elif path.is_file() and path.suffix.lower() == ".json":
                return _display_path(path, root)
            else:
                message = "Choose a JSON file or directory."
            continue
        if key in (curses.KEY_BACKSPACE, 127, 8):
            if current.parent != current:
                current = current.parent.resolve()
                selected = 0
            continue
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
            continue
        if key in (curses.KEY_DOWN, ord("j")):
            selected = min(max(0, len(entries) - 1), selected + 1)
            continue
        if key in (10, 13) and entries:
            entry = entries[selected]
            if entry.is_dir:
                current = entry.path.resolve()
                selected = 0
            else:
                return _display_path(entry.path, root)


def _metric_multiselect(stdscr, title: str, initial: list[str]) -> list[str] | None:
    metrics = list(available_metric_ids())
    selected_values = set(initial)
    selected = 0
    while True:
        stdscr.erase()
        height, _ = stdscr.getmaxyx()
        _safe_addstr(stdscr, 0, 0, title, curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, "Space toggle   Enter toggle   d done   c clear   a all   q cancel")
        _safe_addstr(stdscr, 2, 0, f"Selected: {len(selected_values)} / {len(metrics)}")
        visible = max(1, height - 5)
        start = min(max(0, selected - visible + 1), max(0, len(metrics) - visible))
        for row, metric_id in enumerate(metrics[start : start + visible], start=4):
            index = start + row - 4
            marker = ">" if index == selected else " "
            check = "[x]" if metric_id in selected_values else "[ ]"
            attr = curses.A_REVERSE if index == selected else curses.A_NORMAL
            _safe_addstr(stdscr, row, 0, f"{marker} {check} {metric_id}", attr)
        key = stdscr.getch()
        if key in (ord("q"), ord("Q"), 27):
            return None
        if key in (ord("d"), ord("D")):
            return [metric_id for metric_id in metrics if metric_id in selected_values]
        if key in (ord("c"), ord("C")):
            selected_values.clear()
            continue
        if key in (ord("a"), ord("A")):
            selected_values = set(metrics)
            continue
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
            continue
        if key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(metrics) - 1, selected + 1)
            continue
        if key in (ord(" "), 10, 13) and metrics:
            metric_id = metrics[selected]
            if metric_id in selected_values:
                selected_values.remove(metric_id)
            else:
                selected_values.add(metric_id)


def initial_plan_builder_state() -> dict[str, Any]:
    return {
        "name": "",
        "description": "Automatically generated CBR-Tests plan.",
        "datasets": [],
        "references": [],
        "per_dataset_metrics": False,
        "field_translation": "",
        "include_metrics": [],
        "exclude_metrics": [],
        "single_service": "",
        "expected_ports": "",
        "output": "",
        "output_auto": True,
        "force": False,
    }


def plan_builder_visible_fields(show_advanced: bool) -> tuple[str, ...]:
    return _PLAN_ESSENTIAL_FIELDS + (_PLAN_ADVANCED_FIELDS if show_advanced else ())


def _plan_id(name: str) -> str:
    from create_plan import _slug

    return _slug(name)


def _automatic_plan_output(state: dict[str, Any]) -> str:
    name = str(state.get("name") or "").strip()
    if not name:
        return ""
    plan_id = _plan_id(name)
    datasets = list(state.get("datasets") or [])
    references = list(state.get("references") or [])
    is_batch = len(datasets) > 1 or len(references) > 1
    suffix = "_batch.json" if is_batch else "_plan.json"
    return str(Path("plans") / f"{plan_id}{suffix}")


def _refresh_plan_output(state: dict[str, Any]) -> None:
    if state.get("output_auto", True):
        state["output"] = _automatic_plan_output(state)


def _resolve(root: Path, value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = root / path
    return path.resolve()


def plan_builder_issues(state: dict[str, Any], root: Path) -> list[str]:
    issues: list[str] = []
    name = str(state.get("name") or "").strip()
    datasets = list(state.get("datasets") or [])
    references = list(state.get("references") or [])
    if not name:
        issues.append("Enter a plan name.")
    if not datasets:
        issues.append("Select at least one candidate dataset.")
    for value in datasets:
        path = _resolve(root, value)
        if not path.is_file():
            issues.append(f"Candidate dataset does not exist: {value}")
            break
        if path.suffix.lower() not in SUPPORTED_DATASET_SUFFIXES:
            issues.append(f"Unsupported candidate dataset type: {path.suffix or '(none)'}")
            break
    for value in references:
        path = _resolve(root, value)
        if not path.is_file():
            issues.append(f"Reference dataset does not exist: {value}")
            break
        if path.suffix.lower() not in SUPPORTED_DATASET_SUFFIXES:
            issues.append(f"Unsupported reference dataset type: {path.suffix or '(none)'}")
            break
    if datasets and references:
        candidate_pcap = all(_resolve(root, value).suffix.lower() in {".pcap", ".pcapng"} for value in datasets)
        candidate_tabular = all(_resolve(root, value).suffix.lower() not in {".pcap", ".pcapng"} for value in datasets)
        reference_pcap = all(_resolve(root, value).suffix.lower() in {".pcap", ".pcapng"} for value in references)
        if not candidate_pcap and not candidate_tabular:
            issues.append("Do not mix raw PCAP and tabular candidate datasets in one plan batch.")
        elif candidate_pcap != reference_pcap:
            issues.append("Candidate and reference datasets must use compatible raw-PCAP/tabular formats.")
        elif comparison_job_count(datasets, references) == 0:
            issues.append("All selected comparisons are self-comparisons.")
    service = str(state.get("single_service") or "").strip()
    ports = str(state.get("expected_ports") or "").strip()
    if bool(service) != bool(ports):
        issues.append("Single-service name and expected ports must be supplied together.")
    if ports:
        from create_plan import _parse_expected_ports

        try:
            _parse_expected_ports(ports)
        except ValueError as exc:
            issues.append(str(exc))
    include = set(state.get("include_metrics") or [])
    exclude = set(state.get("exclude_metrics") or [])
    overlap = sorted(include & exclude)
    if overlap:
        issues.append(f"A metric cannot be both included and excluded: {overlap[0]}")
    output = str(state.get("output") or "").strip()
    if name and not output:
        issues.append("Choose an output path.")
    return issues


def plan_builder_review_lines(state: dict[str, Any]) -> list[str]:
    datasets = list(state.get("datasets") or [])
    references = list(state.get("references") or [])
    jobs = comparison_job_count(datasets, references)
    if not references:
        jobs = len(datasets)
    return [
        f"Name: {state.get('name') or 'Not named'}",
        f"Candidates: {len(datasets)} — {_format_list(datasets)}",
        f"References: {len(references)} — {_format_list(references)}",
        f"Generated jobs: {jobs}",
        f"Metric policy: {'Per-job runnable metrics' if state.get('per_dataset_metrics') else 'Common metrics across every job'}",
        f"Include filter: {len(state.get('include_metrics') or []) or 'All runnable metrics'}",
        f"Excluded metrics: {len(state.get('exclude_metrics') or [])}",
        f"Single-service profile: {state.get('single_service') or 'Not asserted'}",
        f"Output: {state.get('output') or 'Not selected'}",
        f"Replace existing output: {'Yes' if state.get('force') else 'No'}",
    ]


def _plan_field_label(name: str) -> str:
    return {
        "name": "Plan name",
        "datasets": "Candidate datasets",
        "references": "Reference datasets",
        "metric_policy": "Metric policy",
        "description": "Description",
        "field_translation": "Field translation JSON",
        "include_metrics": "Include only metrics",
        "exclude_metrics": "Exclude metrics",
        "single_service": "Single-service name",
        "expected_ports": "Expected service ports",
        "output": "Output JSON",
        "force": "Replace existing output",
    }[name]


def _plan_field_help(name: str) -> str:
    return {
        "name": "Readable experiment-plan name. The stable plan ID is derived from it.",
        "datasets": "One or more candidate datasets the plan builder should inspect.",
        "references": "Optional independent references used to enable reference-comparison metrics.",
        "metric_policy": "For matrices, use a common metric set for comparability or keep each job's runnable set.",
        "description": "Saved into the plan metadata for experiment documentation.",
        "field_translation": "Optional explicit translation file. Blank uses dataset sidecars/detection.",
        "include_metrics": "Blank means all runnable metrics. Select values to restrict the builder.",
        "exclude_metrics": "Metrics the builder must leave out even when they are runnable.",
        "single_service": "Only assert this when the whole raw capture is independently known to be one service.",
        "expected_ports": "Comma-separated expected ports paired with the single-service assertion.",
        "output": "Automatically derived from the plan name unless you override it.",
        "force": "Permit replacement of an existing plan or generated batch definition.",
    }[name]


def _plan_field_value(state: dict[str, Any], name: str) -> str:
    if name == "datasets":
        return _format_list(list(state.get("datasets") or []))
    if name == "references":
        return _format_list(list(state.get("references") or []))
    if name == "metric_policy":
        return "Per-job runnable metrics" if state.get("per_dataset_metrics") else "Common metrics across every job"
    if name in {"include_metrics", "exclude_metrics"}:
        values = list(state.get(name) or [])
        return "All runnable" if name == "include_metrics" and not values else (f"{len(values)} selected" if values else "None")
    if name == "force":
        return "Yes" if state.get("force") else "No"
    return str(state.get(name) or "(blank)")


def _review_plan_builder(stdscr, state: dict[str, Any]) -> bool:
    while True:
        stdscr.erase()
        _safe_addstr(stdscr, 0, 0, "Review plan creation", curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, "The builder will inspect the selected datasets and keep only runnable metrics.")
        lines = plan_builder_review_lines(state)
        for row, line in enumerate(lines, start=3):
            _safe_addstr(stdscr, row, 2, line)
        _safe_addstr(stdscr, 3 + len(lines) + 1, 0, "Enter/b build plan   Esc return", curses.A_BOLD)
        key = stdscr.getch()
        if key in (10, 13, ord("b"), ord("B")):
            return True
        if key in (27, ord("q"), ord("Q")):
            return False


def _plan_builder_curses(stdscr, initial: dict[str, Any], root: Path) -> dict[str, Any] | None:
    curses.curs_set(0)
    _initialise_colours()
    state = dict(initial)
    show_advanced = False
    selected = 0
    message = ""
    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        _refresh_plan_output(state)
        fields = plan_builder_visible_fields(show_advanced)
        selected = min(selected, len(fields) - 1)
        issues = plan_builder_issues(state, root)
        _safe_addstr(stdscr, 0, 0, "CBR Tests — Plan builder", curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, "Create a dataset-aware plan or batch definition without hand-editing JSON.")
        status = "READY TO REVIEW" if not issues else f"NEEDS ATTENTION — {issues[0]}"
        _safe_addstr(stdscr, 2, 0, status, _status_attr(not issues))
        _safe_addstr(
            stdscr,
            3,
            0,
            f"↑/↓ move   Enter edit   Space toggle   a {'essentials' if show_advanced else 'advanced'}   r review/build   q back",
        )
        if message:
            _safe_addstr(stdscr, 4, 0, message, curses.A_BOLD)
        content_top = 6
        visible_height = max(1, height - content_top - 5)
        start = min(max(0, selected - visible_height + 1), max(0, len(fields) - visible_height))
        for row, name in enumerate(fields[start : start + visible_height], start=content_top):
            index = start + row - content_top
            marker = ">" if index == selected else " "
            attr = curses.A_REVERSE if index == selected else curses.A_NORMAL
            _safe_addstr(stdscr, row, 0, f"{marker} {_plan_field_label(name):26} {_plan_field_value(state, name)}", attr)
        footer = max(content_top, height - 4)
        try:
            stdscr.hline(footer, 0, curses.ACS_HLINE, max(1, width - 1))
        except curses.error:
            pass
        current = fields[selected]
        _safe_addstr(stdscr, footer + 1, 0, _plan_field_help(current))
        _safe_addstr(stdscr, footer + 2, 0, "Advanced options do not change unless you explicitly edit them.")

        key = stdscr.getch()
        message = ""
        if key in (ord("q"), ord("Q"), 27):
            return None
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
            continue
        if key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(fields) - 1, selected + 1)
            continue
        if key in (ord("a"), ord("A")):
            current_name = fields[selected]
            show_advanced = not show_advanced
            new_fields = plan_builder_visible_fields(show_advanced)
            selected = new_fields.index(current_name) if current_name in new_fields else 0
            continue
        if key in (ord("r"), ord("R")):
            issues = plan_builder_issues(state, root)
            if issues:
                message = issues[0]
                continue
            if _review_plan_builder(stdscr, state):
                return state
            continue
        name = fields[selected]
        if key == ord(" ") and name in {"force"}:
            state[name] = not bool(state.get(name))
            continue
        if key not in (10, 13):
            continue

        if name == "name":
            state["name"] = _edit_text(stdscr, min(height - 2, 5), 0, str(state.get("name") or ""), max(8, width - 1))
            _refresh_plan_output(state)
        elif name == "datasets":
            chosen = _multi_file_browser(stdscr, root, list(state.get("datasets") or []), title="Select candidate datasets", allow_empty=False)
            if chosen is not None:
                state["datasets"] = chosen
                _refresh_plan_output(state)
        elif name == "references":
            chosen = _multi_file_browser(stdscr, root, list(state.get("references") or []), title="Select reference datasets", allow_empty=True)
            if chosen is not None:
                state["references"] = chosen
                _refresh_plan_output(state)
        elif name == "metric_policy":
            state["per_dataset_metrics"] = not bool(state.get("per_dataset_metrics"))
        elif name in {"description", "field_translation", "single_service", "expected_ports", "output"}:
            state[name] = _edit_text(stdscr, min(height - 2, 5), 0, str(state.get(name) or ""), max(8, width - 1))
            if name == "output":
                state["output_auto"] = False
        elif name == "include_metrics":
            chosen = _metric_multiselect(stdscr, "Include only these metrics", list(state.get(name) or []))
            if chosen is not None:
                state[name] = chosen
        elif name == "exclude_metrics":
            chosen = _metric_multiselect(stdscr, "Exclude these metrics", list(state.get(name) or []))
            if chosen is not None:
                state[name] = chosen
        elif name == "force":
            state["force"] = not bool(state.get("force"))


def create_plan_from_state(state: dict[str, Any], repo_root: Path | None = None) -> dict[str, Any]:
    from create_plan import (
        _build_single_plan,
        _create_batch,
        _deduplicate_dataset_values,
        _parse_expected_ports,
        _print_report,
        _slug,
    )

    root = (repo_root or Path.cwd()).expanduser().resolve()
    issues = plan_builder_issues(state, root)
    if issues:
        raise ValueError(issues[0])
    name = str(state["name"]).strip()
    plan_id = _slug(name)
    description = str(state.get("description") or "Automatically generated CBR-Tests plan.")
    datasets = _deduplicate_dataset_values([str(_resolve(root, value)) for value in state.get("datasets") or []])
    references = _deduplicate_dataset_values([str(_resolve(root, value)) for value in state.get("references") or []])
    field_translation = str(state.get("field_translation") or "").strip()
    field_translation_path = _resolve(root, field_translation) if field_translation else None
    include = list(state.get("include_metrics") or []) or None
    exclude = list(state.get("exclude_metrics") or []) or None
    service = str(state.get("single_service") or "").strip()
    ports_text = str(state.get("expected_ports") or "").strip()
    service_config = None
    if service and ports_text:
        service_config = {
            "service_name": service,
            "expected_ports": _parse_expected_ports(ports_text),
            "population_mode": "all_rows",
        }
    output = _resolve(root, str(state["output"]))
    force = bool(state.get("force"))
    per_dataset = bool(state.get("per_dataset_metrics"))

    if len(datasets) > 1 or len(references) > 1:
        written = _create_batch(
            plan_id=plan_id,
            name=name,
            description=description,
            dataset_paths=datasets,
            field_translation_path=field_translation_path,
            include_metric_ids=include,
            exclude_metric_ids=exclude,
            reference_dataset_paths=references,
            service_port_configuration=service_config,
            output_path=output,
            force=force,
            per_dataset_metrics=per_dataset,
            interactive=False,
        )
        return {"kind": "batch", "output_path": str(written), "plan_id": plan_id}

    reference = references[0] if references else None
    plan, report = _build_single_plan(
        plan_id=plan_id,
        name=name,
        description=description,
        dataset_path=datasets[0],
        field_translation_path=field_translation_path,
        include_metric_ids=include,
        exclude_metric_ids=exclude,
        reference_dataset_path=reference,
        service_port_configuration=service_config,
    )
    _print_report(report)
    written = write_plan(output, plan, overwrite=force)
    print(f"\nPlan written: {written}")
    return {
        "kind": "plan",
        "output_path": str(written),
        "plan_id": plan_id,
        "runnable_metric_count": len(plan.get("metrics", [])),
    }


def launch_plan_builder(repo_root: Path | None = None) -> dict[str, Any] | None:
    root = (repo_root or Path.cwd()).expanduser().resolve()
    state = curses.wrapper(_plan_builder_curses, initial_plan_builder_state(), root)
    if state is None:
        return None
    return create_plan_from_state(state, root)


def _collect_rerun_compare(stdscr, root: Path) -> list[str] | None:
    baseline = _json_browser(stdscr, root, title="Choose baseline outcome", initial_dir="outcomes")
    if baseline is None:
        return None
    plan = _json_browser(stdscr, root, title="Choose case or plan", initial_dir="plans")
    if plan is None:
        return None
    datasets = _multi_file_browser(stdscr, root, [], title="Choose dataset to rerun", allow_empty=False)
    if not datasets:
        return None
    height, width = stdscr.getmaxyx()
    default_record = str(Path("outcomes") / "rerun_records" / Path(baseline).stem)
    record = _edit_text(stdscr, min(height - 2, 5), 0, default_record, max(8, width - 1)).strip()
    if not record:
        return None
    return [baseline, plan, datasets[0], record]


def run_tool_action(action: str, repo_root: Path | None = None) -> int:
    root = (repo_root or Path.cwd()).expanduser().resolve()
    if action == "build_plan":
        result = launch_plan_builder(root)
        if result:
            print(f"Created {result['kind']}: {result['output_path']}")
        return 0
    if action == "validate_plan":
        selected = curses.wrapper(_json_browser, root, title="Choose plan to validate", initial_dir="plans")
        if selected is None:
            return 0
        return subprocess.run([sys.executable, str(root / "create_plan.py"), "--check", selected], cwd=root, check=False).returncode
    if action == "migrate_plan":
        selected = curses.wrapper(_json_browser, root, title="Choose plan to migrate", initial_dir="plans")
        if selected is None:
            return 0
        return subprocess.run([sys.executable, str(root / "scripts" / "migrate_plan_to_canonical_ids.py"), selected], cwd=root, check=False).returncode
    if action == "compare_outcomes":
        before = curses.wrapper(_json_browser, root, title="Choose baseline / earlier outcome", initial_dir="outcomes")
        if before is None:
            return 0
        after = curses.wrapper(_json_browser, root, title="Choose later / comparison outcome", initial_dir="outcomes")
        if after is None:
            return 0
        return subprocess.run([sys.executable, str(root / "scripts" / "compare_outcomes.py"), before, after], cwd=root, check=False).returncode
    if action == "rerun_compare":
        values = curses.wrapper(_collect_rerun_compare, root)
        if not values:
            return 0
        baseline, plan, dataset, record = values
        command = [
            sys.executable,
            str(root / "scripts" / "rerun_and_compare.py"),
            "--baseline", baseline,
            "--plan", plan,
            "--dataset", dataset,
            "--record-dir", record,
            "--no-update-field-translation",
        ]
        return subprocess.run(command, cwd=root, check=False).returncode
    if action == "export_graphs":
        return subprocess.run([sys.executable, str(root / "export_outcomes_for_graphs.py")], cwd=root, check=False).returncode
    if action == "list_metrics":
        return subprocess.run([sys.executable, str(root / "create_plan.py"), "--list-tests"], cwd=root, check=False).returncode
    if action == "docs_check":
        return subprocess.run([sys.executable, str(root / "scripts" / "build_reference_documentation.py"), "--check"], cwd=root, check=False).returncode
    if action == "docs_inventory":
        return subprocess.run([sys.executable, str(root / "scripts" / "build_documentation_inventory.py")], cwd=root, check=False).returncode
    raise ValueError(f"Unknown toolbox action: {action}")
