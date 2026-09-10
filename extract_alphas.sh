.venv/bin/python -c '
import json
from src.factors.registry import Registry

keys = (
    "id",
    "nickname",
    "theme",
    "formula_latex",
    "columns_required",
    "decay_horizon",
    "min_warmup_bars",
)

registry = Registry()

result = [
    {key: registry.get(alpha_id).meta[key] for key in keys}
    for alpha_id in registry.list(zoo="qlib158")
]

print(json.dumps(result, ensure_ascii=False, indent=2))
' > qlib158_alphas.json
