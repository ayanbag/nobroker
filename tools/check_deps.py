#!/usr/bin/env python3
"""Prove that nobroker imports nothing outside the standard library.

An empty ``dependencies = []`` in ``pyproject.toml`` proves nothing on its own --
it is a promise about what ``pip`` will fetch, not about what the code imports.
This checks the actual claim: parse every source file, collect every module
anyone imports, and verify each one is either part of nobroker or listed in
``sys.stdlib_module_names``.

Normally this job goes to ``deptry`` or ``pipdeptree``. Both are third-party, so
using them to prove a zero-dependency claim would be self-refuting. The standard
library ships everything needed: :mod:`ast` parses the source without importing
it -- which matters, since importing a module to inspect it would run its
top-level code -- and :data:`sys.stdlib_module_names` is the authoritative list
of what is built in, maintained by the people who decide.

Run ``python tools/check_deps.py > deps-proof.txt`` to regenerate the evidence.
Exits non-zero if anything third-party has crept in, so ``make check-deps`` can
fail a build.
"""

from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
#: Everything under these roots is checked. Tests are included deliberately:
#: the hackathon's dev-only dependency exception does not apply to a project
#: whose whole thesis is that the standard library is enough, and Python ships
#: ``unittest``.
SOURCE_ROOTS = ("src", "tests", "tools", "examples")

#: Modules that are part of this project, not dependencies.
FIRST_PARTY = {"nobroker", "tests"}


def imported_modules(path: Path) -> set[str]:
    """Top-level module names imported by one file, found without importing it."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                found.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            # level > 0 is a relative import: `from .codec import ...`. Those are
            # by definition first-party and have no module name to resolve.
            if node.level == 0 and node.module:
                found.add(node.module.split(".")[0])
    return found


def scan() -> dict[str, set[str]]:
    """Map each imported module to the files that import it."""
    usage: dict[str, set[str]] = {}
    for root in SOURCE_ROOTS:
        base = ROOT / root
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.py")):
            rel = path.relative_to(ROOT).as_posix()
            for module in imported_modules(path):
                usage.setdefault(module, set()).add(rel)
    return usage


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--quiet", action="store_true", help="print only the verdict"
    )
    args = parser.parse_args(argv)

    usage = scan()
    stdlib = sorted(m for m in usage if m in sys.stdlib_module_names)
    first_party = sorted(m for m in usage if m in FIRST_PARTY)
    third_party = sorted(
        m
        for m in usage
        if m not in sys.stdlib_module_names and m not in FIRST_PARTY
    )

    if not args.quiet:
        print("nobroker -- third-party dependency proof")
        print("=" * 60)
        print(f"python:   {sys.version.split()[0]} ({sys.platform})")
        print(f"scanned:  {', '.join(root + '/**/*.py' for root in SOURCE_ROOTS)}")
        print("method:   ast.parse + sys.stdlib_module_names (no imports executed)")
        print()
        print(f"standard library modules imported ({len(stdlib)}):")
        for module in stdlib:
            files = sorted(usage[module])
            shown = ", ".join(files[:3]) + (" ..." if len(files) > 3 else "")
            print(f"  {module:<20} {shown}")
        print()
        print(f"first-party modules ({len(first_party)}):")
        for module in first_party:
            print(f"  {module}")
        print()

    if third_party:
        print(f"THIRD-PARTY DEPENDENCIES FOUND ({len(third_party)}):", file=sys.stderr)
        for module in third_party:
            print(f"  {module}: {', '.join(sorted(usage[module]))}", file=sys.stderr)
        return 1

    print("third-party dependencies: 0")
    print()
    print("VERDICT: nobroker runs on the Python standard library alone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
