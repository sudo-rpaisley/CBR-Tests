from pathlib import Path

path = Path("runner/campaign_tui.py")
text = path.read_text(encoding="utf-8")

text = text.replace(
    "from runner.tui import _display_path, list_file_browser_entries\n",
    "from runner.tui import _display_path\n",
)

start = text.index("def _batch_manifest_for_plan_directory")
end = text.index("def _campaign_review_lines", start)
helpers = '''def _discover_batch_manifests(root: Path, directory: Path | None = None) -> list[tuple[Path, dict[str, Any]]]:
    """Return valid saved batch manifests without descending into generated per-job plan folders."""

    root = root.expanduser().resolve()
    base = (directory or (root / "plans")).expanduser().resolve()
    if not base.is_dir():
        return []

    manifests: list[tuple[Path, dict[str, Any]]] = []
    for candidate in sorted(base.rglob("*.json")):
        try:
            relative = candidate.relative_to(base)
        except ValueError:
            continue
        if any(part.endswith("_batch_plans") for part in relative.parts[:-1]):
            continue
        try:
            batch = validate_batch_manifest(candidate)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        manifests.append((candidate.resolve(), batch))
    return manifests


def _batch_picker(stdscr, root: Path, *, initial_dir: str = "plans") -> list[str] | None:
    """Select one or more already-built batch matrices to append to a campaign queue."""

    root = root.expanduser().resolve()
    search_root = (root / initial_dir).resolve()
    selected = 0
    checked: set[Path] = set()
    manifests = _discover_batch_manifests(root, search_root)
    message = ""

    while True:
        stdscr.erase()
        height, _ = stdscr.getmaxyx()
        selected = min(selected, max(0, len(manifests) - 1))
        _safe_addstr(stdscr, 0, 0, "Choose saved batch matrices", curses.A_BOLD)
        _safe_addstr(
            stdscr,
            1,
            0,
            "Space toggle   Enter queue selected   a select all   c clear   r refresh   q cancel",
        )
        _safe_addstr(
            stdscr,
            2,
            0,
            f"Found {len(manifests)} batch(es) under {_display_path(search_root, root)}; selected {len(checked)}",
        )
        if message:
            _safe_addstr(stdscr, 3, 0, message, curses.A_BOLD)

        start_row = 5
        visible = max(1, height - start_row - 1)
        scroll = min(max(0, selected - visible + 1), max(0, len(manifests) - visible))
        for row, (manifest_path, batch) in enumerate(manifests[scroll : scroll + visible], start=start_row):
            index = scroll + row - start_row
            meta = batch["batch_meta"]
            name = str(meta.get("name") or meta["batch_id"])
            marker = ">" if index == selected else " "
            tick = "x" if manifest_path in checked else " "
            attr = curses.A_REVERSE if index == selected else curses.A_NORMAL
            label = (
                f"{marker} [{tick}] {name} — {len(batch['jobs'])} jobs — "
                f"{_display_path(manifest_path, root)}"
            )
            _safe_addstr(stdscr, row, 0, label, attr)

        if not manifests:
            _safe_addstr(
                stdscr,
                start_row,
                0,
                "No valid saved batch manifests were found under plans/. Build the batches first, then return here.",
            )

        key = stdscr.getch()
        message = ""
        if key in (ord("q"), ord("Q"), 27):
            return None
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
            continue
        if key in (curses.KEY_DOWN, ord("j")):
            selected = min(max(0, len(manifests) - 1), selected + 1)
            continue
        if key == ord(" ") and manifests:
            manifest_path = manifests[selected][0]
            if manifest_path in checked:
                checked.remove(manifest_path)
            else:
                checked.add(manifest_path)
            continue
        if key in (ord("a"), ord("A")):
            checked = {manifest_path for manifest_path, _ in manifests}
            continue
        if key in (ord("c"), ord("C")):
            checked.clear()
            continue
        if key in (ord("r"), ord("R")):
            previous = set(checked)
            manifests = _discover_batch_manifests(root, search_root)
            available = {manifest_path for manifest_path, _ in manifests}
            checked = previous & available
            selected = min(selected, max(0, len(manifests) - 1))
            message = f"Refreshed: {len(manifests)} saved batch(es) found."
            continue
        if key in (10, 13) and manifests:
            if not checked:
                checked.add(manifests[selected][0])
            return [
                _display_path(manifest_path, root)
                for manifest_path, _ in manifests
                if manifest_path in checked
            ]


'''
text = text[:start] + helpers + text[end:]

text = text.replace(
    'issues.append("Add at least one batch/comparison matrix.")',
    'issues.append("Add at least one saved batch matrix.")',
)
text = text.replace(
    '_safe_addstr(stdscr, 1, 0, "Queue independent matrices; each matrix finishes before the next starts.")',
    '_safe_addstr(stdscr, 1, 0, "Queue prepared batch matrices; each batch finishes before the next starts.")',
)
text = text.replace(
    '_safe_addstr(stdscr, 3, 0, "n name   a add matrix   x remove   U/D reorder   d description   o output   f overwrite   b build   q quit")',
    '_safe_addstr(stdscr, 3, 0, "n name   a add batch(es)   x remove   U/D reorder   d description   o output   f overwrite   b build   q quit")',
)
text = text.replace(
    '_safe_addstr(stdscr, 6, 0, f"Matrices queued: {len(batches)}")',
    '_safe_addstr(stdscr, 6, 0, f"Batch matrices queued: {len(batches)}")',
)
old_empty = '''            _safe_addstr(
                stdscr,
                start_row,
                2,
                "No matrices queued. Press a to add a *_batch.json or select its *_batch_plans folder.",
            )'''
new_empty = '''            _safe_addstr(
                stdscr,
                start_row,
                2,
                "No batches queued. Press a to choose one or more saved batch matrices.",
            )'''
if old_empty not in text:
    raise SystemExit("Could not find empty-queue message block")
text = text.replace(old_empty, new_empty)

add_start = text.index('        if key in (ord("a"), ord("A")):')
add_end = text.index('        if key in (ord("x"), ord("X")) and batches:', add_start)
new_add = '''        if key in (ord("a"), ord("A")):
            chosen = _batch_picker(stdscr, root, initial_dir="plans")
            if chosen is None:
                continue
            state["batches"].extend(chosen)
            selected = len(state["batches"]) - 1
            message = f"Queued {len(chosen)} batch matrix{'es' if len(chosen) != 1 else ''}."
            continue
'''
text = text[:add_start] + new_add + text[add_end:]

text = text.replace(
    'print(f"Matrices queued: {payload[\'campaign_meta\'][\'matrix_count\']}")',
    'print(f"Batch matrices queued: {payload[\'campaign_meta\'][\'matrix_count\']}")',
)

path.write_text(text, encoding="utf-8")
