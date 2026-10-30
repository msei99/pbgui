"""Conservative Python AST call/import inventory; never imports inspected modules.

This replaces the repository's mandatory JavaScript-based indexing tool. Dynamic
dispatch and JavaScript callbacks still require source review and browser tests;
an empty static result is explicitly UNKNOWN, never proof of unused code.
"""
from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
import subprocess


def inventory(root: Path, paths: list[str]) -> dict:
    """Collect definitions, call names and imports without executing project code."""
    definitions, calls, imports, errors = [], [], [], []
    for filename in sorted(set(paths)):
        path = root / filename
        if path.suffix != ".py" or not path.is_file() or path.is_symlink():
            continue
        try:
            tree = ast.parse(path.read_text(), filename=filename)
        except (OSError, SyntaxError, UnicodeError) as exc:
            errors.append({"file": filename, "error": type(exc).__name__})
            continue

        class Visitor(ast.NodeVisitor):
            """Retain lexical owners and conservative unresolved call names."""

            def __init__(self):
                """Start at module scope."""
                self.owners = []

            def visit_FunctionDef(self, node):
                """Index a function and its nested declarations/calls."""
                name = ".".join([*self.owners, node.name])
                definitions.append({"file": filename, "line": node.lineno, "name": name})
                self.owners.append(node.name)
                self.generic_visit(node)
                self.owners.pop()

            visit_AsyncFunctionDef = visit_FunctionDef
            visit_ClassDef = visit_FunctionDef

            def visit_Call(self, node):
                """Record names conservatively; attribute receiver types remain unresolved."""
                name = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else None
                if name:
                    calls.append({"file": filename, "line": node.lineno, "owner": ".".join(self.owners),
                                  "target": name, "dynamic_receiver": isinstance(node.func, ast.Attribute)})
                self.generic_visit(node)

            def visit_Import(self, node):
                """Record explicit module imports."""
                imports.extend({"file": filename, "module": item.name} for item in node.names)

            def visit_ImportFrom(self, node):
                """Record imports without following or executing them."""
                imports.append({"file": filename, "module": "." * node.level + (node.module or "")})

        Visitor().visit(tree)
    return {"definitions": definitions, "calls": calls, "imports": imports, "parse_errors": errors}


def impact(index: dict, target: str) -> dict:
    """Report possible callers and the limits of name-based static resolution."""
    name = target.rsplit(".", 1)[-1]
    definitions = [row for row in index["definitions"] if row["name"] == target or row["name"].rsplit(".", 1)[-1] == name]
    callers = [row for row in index["calls"] if row["target"] == name]
    return {"target": target, "risk": "UNKNOWN" if not callers else "HIGH" if len(callers) > 15 else "MEDIUM",
            "definitions": definitions, "possible_callers": callers,
            "limitations": "Conservative name matches; dynamic dispatch, callback registration and browser JavaScript require source review. Zero callers does not prove unused code.",
            "parse_errors": index["parse_errors"]}


def main():
    """Provide a Python-only targeted impact or changed-file inventory command."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", nargs="?")
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--changed", action="store_true")
    options = parser.parse_args()
    root = options.repo.resolve()
    files = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard"], cwd=root, text=True).splitlines()
    index = inventory(root, files)
    if options.changed:
        changed = subprocess.check_output(["git", "diff", "--name-only", "HEAD"], cwd=root, text=True).splitlines()
        changed += subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard"], cwd=root, text=True).splitlines()
        value = {"changed_files": sorted(set(changed)), "definitions": [row for row in index["definitions"] if row["file"] in changed],
                 "parse_errors": index["parse_errors"], "limitations": "Browser JavaScript changes require browser checks and source review."}
    else:
        if not options.target:
            parser.error("Supply a target symbol or --changed")
        value = impact(index, options.target)
    print(json.dumps(value, indent=2))


if __name__ == "__main__":
    main()
