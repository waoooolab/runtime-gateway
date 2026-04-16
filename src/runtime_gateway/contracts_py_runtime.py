"""Contracts-py import bootstrap helpers for runtime-gateway."""

from __future__ import annotations

import sys
from pathlib import Path


def ensure_contracts_py_importable() -> None:
    """Ensure `openwaoooo_contracts` can be imported.

    Preferred path is normal environment installation.
    For monorepo execution, we also probe sibling `openwaoooo/packages/contracts-py/src`.
    """
    try:
        import openwaoooo_contracts  # noqa: F401
        return
    except ModuleNotFoundError:
        pass

    contracts_py_src = (
        Path(__file__).resolve().parents[3] / "openwaoooo" / "packages" / "contracts-py" / "src"
    )
    if contracts_py_src.exists():
        path_text = contracts_py_src.as_posix()
        if path_text not in sys.path:
            sys.path.insert(0, path_text)
        import openwaoooo_contracts  # noqa: F401
        return

    raise ModuleNotFoundError(
        "openwaoooo_contracts not importable; install openwaoooo-contracts-py "
        "or run from a monorepo checkout with openwaoooo/packages/contracts-py/src present."
    )
