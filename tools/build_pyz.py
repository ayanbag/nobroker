"""Build ``dist/nobroker.pyz`` -- the whole tool as one runnable file.

Why this exists
---------------
The hackathon asks for "a build command that produces a runnable artifact in one
step". For a pure-Python project the honest answer is that there is nothing to
compile, but "nothing to build" is a worse answer than an artifact a judge can
double-click. ``zipapp`` (stdlib, PEP 441) gives us a real one: a single file,
runnable by any Python 3.11+, with no install step and nothing unpacked to disk.

Why not ``shiv`` / ``pex`` / ``PyInstaller``
-------------------------------------------
All three exist to solve the hard part of bundling -- vendoring third-party
dependencies and their compiled extensions into one file. We have no
dependencies and no extensions, so the hard part is absent and ``zipapp`` is the
whole job. Reaching for a bundler here would mean installing a package whose
purpose is to package the packages we do not have.

Why not ``python -m zipapp`` directly in the Makefile
-----------------------------------------------------
Two reasons, both small and both real: the CLI form has no way to exclude
``__pycache__`` (which would ship stale bytecode compiled for whichever
interpreter last ran the tests), and it cannot verify the result. This script
does both, and prints the size so the "zero dependencies" claim has a number
attached.
"""

from __future__ import annotations

import subprocess
import sys
import zipapp
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "src"
TARGET = ROOT / "dist" / "nobroker.pyz"

# Interpreter line for POSIX. Ignored on Windows, where the .pyz extension is
# associated with the launcher instead.
SHEBANG = "/usr/bin/env python3"


def _include(path: Path) -> bool:
    """Keep source and the PEP 561 marker; drop bytecode caches.

    ``zipapp`` walks the tree and asks this about every entry. Bytecode is
    excluded because it is both redundant (Python recompiles from source) and
    actively harmful in an archive: a ``__pycache__`` from a different
    interpreter version is dead weight that cannot be used.
    """
    if "__pycache__" in path.parts:
        return False
    return path.suffix != ".pyc"


def main() -> int:
    if not (SOURCE / "__main__.py").exists():
        print(f"error: {SOURCE / '__main__.py'} is missing", file=sys.stderr)
        return 1

    TARGET.parent.mkdir(parents=True, exist_ok=True)
    zipapp.create_archive(
        SOURCE,
        target=TARGET,
        interpreter=SHEBANG,
        filter=_include,
        compressed=True,
    )

    size_kb = TARGET.stat().st_size / 1024

    # Prove the artifact runs, rather than asserting that it does. Invoking
    # sys.executable is not a shell-out to a third-party tool -- it is the
    # interpreter already running this script.
    proof = subprocess.run(
        [sys.executable, str(TARGET), "--version"],
        capture_output=True,
        text=True,
    )
    if proof.returncode != 0:
        print(f"error: {TARGET.name} was built but does not run", file=sys.stderr)
        print(proof.stderr.strip(), file=sys.stderr)
        return 1

    rel = TARGET.relative_to(ROOT).as_posix()
    print(f"built    {rel}  ({size_kb:.0f} KB, no dependencies)")
    print(f"verified {proof.stdout.strip()}")
    print()
    print(f"  run it:  python {rel} --help")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
