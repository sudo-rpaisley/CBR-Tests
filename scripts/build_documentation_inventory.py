from __future__ import annotations

import argparse
import ast
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


EXCLUDED_PARTS = {".git", ".pytest_cache", ".venv", "venv", "__pycache__"}


@dataclass(frozen=True)
class Symbol:
    file: str
    qualified_name: str
    kind: str
    signature: str
    line: int
    docstring: str | None
    is_public: bool
    is_test: bool


def _annotation(node: ast.expr | None) -> str | None:
    return ast.unparse(node) if node is not None else None


def _signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    parts: list[str] = []
    positional = [*node.args.posonlyargs, *node.args.args]
    defaults = [None] * (len(positional) - len(node.args.defaults)) + list(node.args.defaults)
    for index, (argument, default) in enumerate(zip(positional, defaults)):
        prefix = ""
        if index == len(node.args.posonlyargs) and node.args.posonlyargs:
            parts.append("/")
        annotation = _annotation(argument.annotation)
        text = f"{argument.arg}: {annotation}" if annotation else argument.arg
        if default is not None:
            text += f" = {ast.unparse(default)}"
        parts.append(prefix + text)
    if node.args.vararg:
        annotation = _annotation(node.args.vararg.annotation)
        parts.append(f"*{node.args.vararg.arg}: {annotation}" if annotation else f"*{node.args.vararg.arg}")
    elif node.args.kwonlyargs:
        parts.append("*")
    for argument, default in zip(node.args.kwonlyargs, node.args.kw_defaults):
        annotation = _annotation(argument.annotation)
        text = f"{argument.arg}: {annotation}" if annotation else argument.arg
        if default is not None:
            text += f" = {ast.unparse(default)}"
        parts.append(text)
    if node.args.kwarg:
        annotation = _annotation(node.args.kwarg.annotation)
        parts.append(f"**{node.args.kwarg.arg}: {annotation}" if annotation else f"**{node.args.kwarg.arg}")
    returns = _annotation(node.returns)
    return f"({', '.join(parts)})" + (f" -> {returns}" if returns else "")


def _iter_python_files(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*.py")
        if not any(part in EXCLUDED_PARTS for part in path.parts)
    )


def _symbol(file: Path, root: Path, node: ast.AST, qualified_name: str, kind: str) -> Symbol:
    function_node = node if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) else None
    name = qualified_name.rsplit(".", 1)[-1]
    return Symbol(
        file=file.relative_to(root).as_posix(),
        qualified_name=qualified_name,
        kind=kind,
        signature=_signature(function_node) if function_node else "",
        line=getattr(node, "lineno", 1),
        docstring=ast.get_docstring(node, clean=True),
        is_public=not name.startswith("_"),
        is_test=name.startswith("test_") or file.name.startswith("test_"),
    )


def collect_symbols(root: Path) -> list[Symbol]:
    symbols: list[Symbol] = []
    for path in _iter_python_files(root):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError, UnicodeDecodeError):
            continue
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                symbols.append(_symbol(path, root, node, node.name, "function"))
            elif isinstance(node, ast.ClassDef):
                symbols.append(_symbol(path, root, node, node.name, "class"))
                for child in node.body:
                    if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        symbols.append(_symbol(path, root, child, f"{node.name}.{child.name}", "method"))
    return symbols


def _constant_string(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def collect_cli_options(root: Path) -> list[dict[str, Any]]:
    options: list[dict[str, Any]] = []
    for path in _iter_python_files(root):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError, UnicodeDecodeError):
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr != "add_argument":
                continue
            flags = [value for arg in node.args if (value := _constant_string(arg)) is not None]
            details: dict[str, Any] = {
                "file": path.relative_to(root).as_posix(),
                "line": node.lineno,
                "flags": flags,
            }
            for keyword in node.keywords:
                if keyword.arg in {"help", "default", "required", "action", "choices", "type", "dest"}:
                    try:
                        details[keyword.arg] = ast.literal_eval(keyword.value)
                    except (ValueError, TypeError):
                        details[keyword.arg] = ast.unparse(keyword.value)
            options.append(details)
    return sorted(options, key=lambda item: (item["file"], item["line"]))


def collect_metric_ids(root: Path) -> list[dict[str, Any]]:
    found: dict[str, set[str]] = {}
    for path in _iter_python_files(root):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError, UnicodeDecodeError):
            continue
        relative = path.relative_to(root).as_posix()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "register_metric" and node.args:
                metric_id = _constant_string(node.args[0])
                if metric_id:
                    found.setdefault(metric_id, set()).add(relative)
            if isinstance(node, ast.Dict):
                for key in node.keys:
                    metric_id = _constant_string(key) if key is not None else None
                    if metric_id and any(token in metric_id for token in ("ratio", "profile", "deviation", "distance", "divergence", "score", "validity", "consistency", "correlation")):
                        found.setdefault(metric_id, set()).add(relative)
    return [
        {"metric_id": metric_id, "files": sorted(files)}
        for metric_id, files in sorted(found.items())
    ]


def collect_json_files(root: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*.json")):
        if any(part in EXCLUDED_PARTS for part in path.parts):
            continue
        relative = path.relative_to(root).as_posix()
        kind = "json"
        if relative.startswith("plans/"):
            kind = "plan"
        elif relative.startswith("cases/"):
            kind = "case"
        elif "field_translation" in relative:
            kind = "field_translation"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            top_level_keys = sorted(payload) if isinstance(payload, dict) else []
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            top_level_keys = []
        records.append({"file": relative, "kind": kind, "top_level_keys": top_level_keys})
    return records


def build_inventory(root: Path) -> dict[str, Any]:
    symbols = collect_symbols(root)
    files = sorted(
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and not any(part in EXCLUDED_PARTS for part in path.parts)
    )
    return {
        "files": files,
        "python_files": [path.relative_to(root).as_posix() for path in _iter_python_files(root)],
        "symbols": [asdict(symbol) for symbol in symbols],
        "public_symbols": [asdict(symbol) for symbol in symbols if symbol.is_public and not symbol.is_test],
        "tests": [asdict(symbol) for symbol in symbols if symbol.is_test and symbol.kind == "function"],
        "cli_options": collect_cli_options(root),
        "metric_ids": collect_metric_ids(root),
        "json_files": collect_json_files(root),
    }


def render_markdown(inventory: dict[str, Any]) -> str:
    lines = [
        "# Repository documentation inventory",
        "",
        "Generated by `scripts/build_documentation_inventory.py`.",
        "",
        "## Summary",
        "",
        f"- Files: {len(inventory['files'])}",
        f"- Python files: {len(inventory['python_files'])}",
        f"- Public functions/classes/methods: {len(inventory['public_symbols'])}",
        f"- Pytest test functions: {len(inventory['tests'])}",
        f"- CLI options discovered: {len(inventory['cli_options'])}",
        f"- Metric-like identifiers discovered: {len(inventory['metric_ids'])}",
        "",
        "## Public symbols",
        "",
    ]
    current_file = None
    for item in inventory["public_symbols"]:
        if item["file"] != current_file:
            current_file = item["file"]
            lines.extend([f"### `{current_file}`", ""])
        signature = item["signature"]
        lines.append(f"- `{item['qualified_name']}{signature}` (line {item['line']})")
        if item["docstring"]:
            lines.append(f"  - {item['docstring'].splitlines()[0]}")
    lines.extend(["", "## Tests", ""])
    current_file = None
    for item in inventory["tests"]:
        if item["file"] != current_file:
            current_file = item["file"]
            lines.extend([f"### `{current_file}`", ""])
        lines.append(f"- `{item['qualified_name']}{item['signature']}` (line {item['line']})")
        if item["docstring"]:
            lines.append(f"  - {item['docstring'].splitlines()[0]}")
    lines.extend(["", "## CLI options", ""])
    for option in inventory["cli_options"]:
        flags = ", ".join(f"`{flag}`" for flag in option["flags"]) or "positional argument"
        lines.append(f"- {flags} — `{option['file']}:{option['line']}`")
        if option.get("help"):
            lines.append(f"  - {option['help']}")
    lines.extend(["", "## JSON files", ""])
    for item in inventory["json_files"]:
        lines.append(f"- `{item['file']}` ({item['kind']})")
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inventory repository symbols for documentation work.")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--json", type=Path, default=Path("documentation-inventory.json"))
    parser.add_argument("--markdown", type=Path, default=Path("documentation-inventory.md"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.root.resolve()
    inventory = build_inventory(root)
    args.json.write_text(json.dumps(inventory, indent=2) + "\n", encoding="utf-8")
    args.markdown.write_text(render_markdown(inventory), encoding="utf-8")
    print(
        f"Inventory: {len(inventory['files'])} files, "
        f"{len(inventory['public_symbols'])} public symbols, "
        f"{len(inventory['tests'])} tests, "
        f"{len(inventory['metric_ids'])} metric-like identifiers."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
