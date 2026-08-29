"""Test suite for nobroker. Standard library ``unittest`` only -- no pytest.

Putting ``src/`` on the path here means ``python -m unittest discover`` works
from a fresh clone with no install step and no ``PYTHONPATH`` incantation, which
is the same promise the rest of the project makes: clone it, run it.
"""

import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))
