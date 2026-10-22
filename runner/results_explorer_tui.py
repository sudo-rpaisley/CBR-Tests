"""Paged, read-only curses browser for the DuckDB research results catalogue.

Campaign -> held-out capture -> metric -> run/seed/percentage -> every leaf
measurement. No LIMIT-less full-table fetches; remains responsive for large
experiments with millions of nested measurement values.
"""
from __future__ import annotations

import curses
from pathlib import Path
import textwrap

from runner.friendly_tui import _safe_addstr
from runner.results_catalogue import (
    export_parquet, index_roots, open_catalogue, summary,
)
from runner.tui import _edit_text


def _records(conn, query: str, args: list, limit: int, offset: int) -> list[dict]:
    cursor = conn.execute(query + " LIMIT ? OFFSET ?", args + [limit, offset])
    fields = [entry[0] for entry in cursor.description]
    return [dict(zip(fields, row)) for row in cursor.fetchall()]


def _rows(conn, frame: dict, limit: int):
    """Return a single page for one part of the drill-down hierarchy."""
    view = frame["view"]
    search = frame.get("search", "").lower()
    where = f"%{search}%"
    args = []
    if view == "campaigns":
        sql = """
        SELECT campaign_key AS id, campaign_id AS title,
               started_at AS extra_date, status, total_cases AS count,
               source_kind, source_path
        FROM campaigns
        WHERE lower(coalesce(campaign_id, '') || ' ' || coalesce(source_path, '')) LIKE ?
        ORDER BY started_at DESC, source_path
        """
        args = [where]
    elif view == "cases":
        sql = """
        SELECT cs.case_key AS id,
            coalesce(nullif(cs.group_path, ''), '(root)') || ' / ' ||
                cs.held_out_path AS title,
            cs.status, cs.group_path, cs.held_out_path AS file,
            (SELECT count(*) FROM jobs j WHERE j.case_key = cs.case_key) AS count
        FROM cases cs WHERE cs.campaign_key = ?
            AND lower(coalesce(cs.held_out_path, '') || ' ' || coalesce(cs.group_path, '')) LIKE ?
        ORDER BY group_path, held_out_path
        """
        args = [frame["campaign_key"], where]
    elif view == "metrics":
        sql = """
        SELECT m.metric_id AS id, m.metric_id AS title,
            COUNT(*) AS count,
            COUNT(DISTINCT j.perturbation_pct) AS levels,
            MIN(j.perturbation_pct) AS min_pct,
            MAX(j.perturbation_pct) AS max_pct
        FROM metrics m JOIN jobs j ON m.job_key = j.job_key
        WHERE m.case_key = ? AND lower(m.metric_id) LIKE ?
        GROUP BY m.metric_id ORDER BY m.metric_id
        """
        args = [frame["case_key"], where]
    elif view == "observations":
        sql = """
        SELECT m.metric_key AS id, j.perturbation_pct AS pct, j.seed,
               j.attempt, j.attempt_status AS status,
               m.domain_status, m.execution_status,
               m.primary_value, j.outcome_path
        FROM metrics m JOIN jobs j ON m.job_key = j.job_key
        WHERE m.case_key = ? AND m.metric_id = ?
        ORDER BY j.perturbation_pct, j.seed, j.attempt
        """
        args = [frame["case_key"], frame["metric_id"]]
    elif view == "timeline":
        sql = """
        SELECT m.metric_key AS id, ca.started_at AS extra_date,
               cs.held_out_path AS file,
               j.perturbation_pct AS pct, j.seed, j.attempt,
               m.primary_value, m.domain_status, j.outcome_path
        FROM metrics m
        JOIN jobs j ON m.job_key = j.job_key
        JOIN cases cs ON j.case_key = cs.case_key
        JOIN campaigns ca ON j.campaign_key = ca.campaign_key
        WHERE m.metric_id = ?
        ORDER BY ca.started_at DESC, cs.held_out_path, j.perturbation_pct, j.seed
        """
        args = [frame["metric_id"]]
    elif view == "measurements":
        sql = """
        SELECT me.json_pointer AS id, me.json_pointer AS title,
               me.value_type, me.numeric_value, me.value_json
        FROM measurements me WHERE me.metric_key = ?
        ORDER BY me.json_pointer
        """
        args = [frame["metric_key"]]
    else:
        raise ValueError(f"Unknown view: {view}")
    return _records(conn, sql, args, limit, frame["offset"])


def _preview(frame: dict, row: dict) -> str:
    view = frame["view"]
    if view == "campaigns":
        return f"{row['title']}  [{row['status']}]  {row['extra_date']}  ({row['count']} held-outs)"
    if view == "cases":
        return f"{row['title']}  [{row['status']}]  ({row['count']} evaluation jobs)"
    if view == "metrics":
        return f"{row['title']}  ({row['count']} measurements; {row['levels']} GAN levels)"
    if view in {"observations", "timeline"}:
        when = str(row.get("extra_date") or "")
        source = (Path(row["file"]).name + "  ") if row.get("file") else ""
        return (
            f"{when[:10]} {source} GAN {row['pct']}%  seed={row['seed']} "
            f"attempt={row['attempt']}  value={row['primary_value']} "
            f"status={row.get('domain_status', row.get('status', ''))}"
        )
    if view == "measurements":
        displayed = str(row["value_json"])
        return f"{row['title']} = {displayed[:100]}"
    return str(row)


def _detail(stdscr, title: str, content: str) -> None:
    """Scrollable detail window for long JSON values/measurement provenance."""
    height, width = stdscr.getmaxyx()
    width = max(12, width - 3)
    paragraphs = str(content).splitlines() or [""]
    lines = []
    for paragraph in paragraphs:
        lines.extend(textwrap.wrap(paragraph, width=width, replace_whitespace=False,
                                   drop_whitespace=False) or [""])
    offset = 0
    while True:
        stdscr.erase()
        _safe_addstr(stdscr, 0, 0, title, curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, "Up/Down or PgUp/PgDn scroll; q/Esc/Enter returns.")
        visible = max(1, height - 3)
        for i, text in enumerate(lines[offset:offset+visible], start=2):
            _safe_addstr(stdscr, i, 0, text)
        _safe_addstr(stdscr, height-1, 0, f"Line {offset+1}/{max(1,len(lines))}")
        k = stdscr.getch()
        if k in (ord("q"), 27, 10, 13, curses.KEY_LEFT):
            return
        if k in (curses.KEY_DOWN, ord("j")):
            offset = min(max(0, len(lines)-visible), offset+1)
        elif k in (curses.KEY_UP, ord("k")):
            offset = max(0, offset-1)
        elif k == curses.KEY_NPAGE:
            offset = min(max(0, len(lines)-visible), offset+visible)
        elif k == curses.KEY_PPAGE:
            offset = max(0, offset-visible)


def _screen(stdscr, conn) -> str | None:
    """Read-only UI, returns 'refresh' when re-indexing is requested."""
    curses.curs_set(0)
    stack = [{"view": "campaigns", "label": "All campaigns", "offset": 0, "selected": 0, "search": ""}]
    message = "Enter drill down  / filter  t cross-date metric  o raw outcome path  r reindex  b back  q quit"
    while True:
        frame = stack[-1]
        height, width = stdscr.getmaxyx()
        page = max(1, height - 6)
        rows = _rows(conn, frame, page)
        frame["selected"] = min(frame["selected"], max(0, len(rows)-1))
        stdscr.erase()
        _safe_addstr(stdscr, 0, 0, "CBR-Tests Results Explorer  |  " +
                     "  >  ".join(x["label"] for x in stack), curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, message)
        if frame.get("search"):
            _safe_addstr(stdscr, 2, 0, f"Filter: {frame['search']}")
        else:
            _safe_addstr(stdscr, 2, 0, f"View: {frame['view']}")
        for index, row in enumerate(rows):
            attr = curses.A_REVERSE if frame["selected"] == index else curses.A_NORMAL
            _safe_addstr(stdscr, 4+index, 0,
                         ("> " if index == frame["selected"] else "  ") + _preview(frame, row), attr)
        _safe_addstr(stdscr, height-1, 0,
                     f"Showing {frame['offset']+1 if rows else 0}–{frame['offset']+len(rows)}"
                     + ("  PgDn next" if len(rows) == page else "  End of results"))
        key = stdscr.getch()
        if key in (ord("q"), ord("Q"), 27):
            return None
        if key in (ord("b"), curses.KEY_LEFT, curses.KEY_BACKSPACE, 127, 8):
            if len(stack) > 1:
                stack.pop()
                message = "Back to previous results"
            else:
                return None
            continue
        if key == ord("r") and frame["view"] == "campaigns":
            return "refresh"
        if key == ord("/") and frame["view"] in {"campaigns", "cases", "metrics"}:
            curses.curs_set(1)
            typed = _edit_text(stdscr, max(0, height-2), 0,
                               str(frame.get("search", "")), max(8, width-1))
            curses.curs_set(0)
            frame.update(search=typed.strip(), offset=0, selected=0)
            continue
        if key in (curses.KEY_DOWN, ord("j")):
            if frame["selected"] < len(rows)-1:
                frame["selected"] += 1
            elif len(rows) == page:
                frame.update(offset=frame["offset"]+1)
            continue
        if key in (curses.KEY_UP, ord("k")):
            if frame["selected"] > 0:
                frame["selected"] -= 1
            else:
                frame.update(offset=max(0, frame["offset"]-1))
            continue
        if key == curses.KEY_NPAGE and len(rows) == page:
            frame.update(offset=frame["offset"]+page, selected=0)
            continue
        if key == curses.KEY_PPAGE:
            frame.update(offset=max(0, frame["offset"]-page), selected=0)
            continue
        if not rows:
            continue
        row = rows[frame["selected"]]
        if key == ord("t") and frame["view"] in {"metrics", "observations"}:
            metric_id = row["id"] if frame["view"] == "metrics" else frame["metric_id"]
            stack.append({"view": "timeline", "label": "Across dates: " + metric_id,
                          "metric_id": metric_id, "offset": 0, "selected": 0})
            continue
        if key == ord("o") and row.get("outcome_path"):
            _detail(stdscr, "Raw CBR-Tests outcome JSON", row["outcome_path"])
            continue
        if key not in (10, 13):
            continue
        if frame["view"] == "campaigns":
            stack.append({"view": "cases", "campaign_key": row["id"],
                          "label": str(row["title"]), "offset": 0, "selected": 0, "search": ""})
        elif frame["view"] == "cases":
            stack.append({"view": "metrics", "case_key": row["id"],
                          "label": str(Path(row["file"]).name), "offset": 0,
                          "selected": 0, "search": ""})
        elif frame["view"] == "metrics":
            stack.append({"view": "observations", "case_key": frame["case_key"],
                          "metric_id": row["id"], "label": str(row["title"]),
                          "offset": 0, "selected": 0})
        elif frame["view"] in {"observations", "timeline"}:
            stack.append({"view": "measurements", "metric_key": row["id"],
                          "label": f"{row['pct']}%, seed {row['seed']}",
                          "offset": 0, "selected": 0})
        elif frame["view"] == "measurements":
            _detail(stdscr, str(row["id"]), str(row["value_json"]))


def launch_results_explorer(db_path: Path, *, repo_root: Path | None = None) -> None:
    """Index historical outcomes and browse without loading full archives at once."""
    root = (repo_root or Path(__file__).resolve().parents[1]).expanduser().resolve()
    db = Path(db_path).expanduser()
    if not db.is_absolute():
        db = root / db
    search = root / "outcomes"
    if not search.is_dir():
        raise FileNotFoundError(f"Outcome directory not found: {search}")
    while True:
        conn = open_catalogue(db)
        try:
            report = index_roots(conn, [search])
            counts = summary(conn)
            print(f"Results catalogue: {db}")
            print(f"Campaigns {counts['campaigns']}; jobs {counts['jobs']}; "
                  f"metrics {counts['metrics']}; measurements {counts['measurements']}")
            print(f"New/updated archives {report['indexed']}; indexing errors {report['failed']}")
            for item in report["reports"]:
                if item["status"] == "failed":
                    print(f"  {item['path']}: {item['error']}")
            parquet = root / "outcomes" / "results-parquet"
            if report["indexed"] or not (parquet / "campaigns.parquet").is_file():
                export_parquet(conn, parquet)
        finally:
            conn.close()
        reader = open_catalogue(db, read_only=True)
        try:
            action = curses.wrapper(_screen, reader)
        finally:
            reader.close()
        if action != "refresh":
            break
