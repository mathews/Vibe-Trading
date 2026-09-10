"""Static metadata exported by the academic factor zoo."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

_METADATA_KEYS = (
    "id",
    "nickname",
    "theme",
    "formula_latex",
    "columns_required",
    "decay_horizon",
    "min_warmup_bars",
)


def _extract_metadata(path: Path) -> dict[str, Any]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for statement in tree.body:
        if not isinstance(statement, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == "__alpha_meta__"
            for target in statement.targets
        ):
            continue
        metadata = ast.literal_eval(statement.value)
        if not isinstance(metadata, dict):
            raise ValueError(f"{path.name}: __alpha_meta__ must be a dictionary")
        return {key: metadata[key] for key in _METADATA_KEYS}
    raise ValueError(f"{path.name}: __alpha_meta__ assignment not found")


def extract_academic_metadata() -> list[dict[str, Any]]:
    """Return the selected metadata for every academic factor module."""
    academic_dir = Path(__file__).parent
    return [
        _extract_metadata(path)
        for path in sorted(academic_dir.glob("*.py"))
        if not path.name.startswith("_") and path.name != Path(__file__).name
    ]


ACADEMIC_ALPHA_METADATA = extract_academic_metadata()


__all__ = ["ACADEMIC_ALPHA_METADATA", "extract_academic_metadata"]