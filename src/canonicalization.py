import json
from typing import Any, Union, Dict, List


class CanonicalJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, float):
            return f"{obj:.15f}".rstrip('0').rstrip('.')
        return super().default(obj)

    def encode(self, obj: Any) -> str:
        if isinstance(obj, dict):
            return "{" + ",".join(f"{self.encode(k)}:{self.encode(v)}" for k, v in sorted(obj.items())) + "}"
        elif isinstance(obj, list):
            return "[" + ",".join(self.encode(item) for item in obj) + "]"
        elif isinstance(obj, str):
            return json.dumps(obj, separators=(',', ':'))
        elif isinstance(obj, (int, float, bool)) or obj is None:
            return json.dumps(obj, separators=(',', ':'))
        else:
            return super().encode(obj)

def canonical_json_dumps(obj: Union[Dict, List]) -> str:
    return CanonicalJSONEncoder().encode(obj)