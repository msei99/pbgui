"""Policy guardrails for PBGui-controlled local pbgui.ini writers."""

from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ALLOWED_WRITERS = {
    Path("pbgui_purefunc.py"),
    Path("setup/installer/core.py"),
}
EXCLUDED_ROOTS = {"tests", "setup", "scripts", ".git", ".venv", "venv"}


def _is_config_write(call: ast.Call) -> bool:
    """Return whether a call serializes a ConfigParser-style object."""
    return isinstance(call.func, ast.Attribute) and call.func.attr == "write"


def _is_literal_ini(node: ast.AST) -> bool:
    """Return whether a node is the cwd-relative local INI literal."""
    return isinstance(node, ast.Constant) and node.value == "pbgui.ini"


def _is_relative_ini_constructor(call: ast.Call) -> bool:
    """Return whether Path/open constructs a cwd-relative local INI path."""
    if isinstance(call.func, ast.Name):
        name = call.func.id
    elif isinstance(call.func, ast.Attribute):
        name = call.func.attr
    else:
        name = ""
    return (name in {"Path", "PurePath", "open"} or name.endswith("Path")) and bool(call.args) and _is_literal_ini(call.args[0])


def test_local_pbgui_ini_writers_use_shared_transaction() -> None:
    """Production local INI writers must not serialize ConfigParser directly."""
    violations = []
    for path in ROOT.rglob("*.py"):
        relative = path.relative_to(ROOT)
        if relative in ALLOWED_WRITERS or relative.parts[0] in EXCLUDED_ROOTS:
            continue
        source = path.read_text(encoding="utf-8")
        if "pbgui.ini" not in source:
            continue
        tree = ast.parse(source, filename=str(relative))
        scopes = [node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
        for scope in scopes:
            scope_source = ast.get_source_segment(source, scope) or ""
            if "pbgui.ini" not in scope_source:
                continue
            parser_names = {
                node.targets[0].id
                for node in ast.walk(scope)
                if isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and isinstance(node.value, ast.Call)
                and isinstance(node.value.func, ast.Attribute)
                and node.value.func.attr in {"ConfigParser", "RawConfigParser"}
            }
            for node in ast.walk(scope):
                if not isinstance(node, ast.Call) or not _is_config_write(node):
                    continue
                if isinstance(node.func.value, ast.Name) and node.func.value.id in parser_names:
                    violations.append(f"{relative}:{node.lineno}")

    assert violations == [], "Direct local pbgui.ini ConfigParser writers: " + ", ".join(violations)


def test_local_pbgui_ini_readers_are_not_cwd_relative() -> None:
    """Production local readers must use the canonical INI path or snapshot helper."""
    violations = []
    for path in ROOT.rglob("*.py"):
        relative = path.relative_to(ROOT)
        if relative.parts[0] in EXCLUDED_ROOTS:
            continue
        source = path.read_text(encoding="utf-8")
        if "pbgui.ini" not in source:
            continue
        tree = ast.parse(source, filename=str(relative))
        relative_names = {
            target.id
            for node in ast.walk(tree)
            if isinstance(node, (ast.Assign, ast.AnnAssign))
            for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
            if isinstance(target, ast.Name) and _is_literal_ini(node.value)
        }
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if _is_relative_ini_constructor(node):
                violations.append(f"{relative}:{node.lineno}")
                continue
            if (
                isinstance(node.func, ast.Attribute)
                and node.func.attr in {"read", "read_text", "open"}
                and node.args
                and (
                    _is_literal_ini(node.args[0])
                    or (isinstance(node.args[0], ast.Name) and node.args[0].id in relative_names)
                )
            ):
                violations.append(f"{relative}:{node.lineno}")

    assert violations == [], "Cwd-relative local pbgui.ini readers: " + ", ".join(violations)
