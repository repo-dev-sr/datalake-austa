"""
Extrai nomes de campos de um .avro Debezium (usa primeiro registro + schema).
Uso: python extract_bronze_avro_fields.py <path-to.avro>
"""
import json
import sys
from pathlib import Path

import fastavro


def flatten_fields(schema: dict, prefix: str = "") -> list[str]:
    """Lista nomes de campos top-level do record Debezium (after, before, source, op, ...)."""
    if isinstance(schema, str):
        return []
    t = schema.get("type")
    if t == "record":
        return [f["name"] for f in schema.get("fields", [])]
    if isinstance(t, list) and "null" in t:
        for x in t:
            if x != "null" and isinstance(x, dict):
                return flatten_fields(x, prefix)
    return []


def main():
    path = Path(sys.argv[1])
    with path.open("rb") as f:
        reader = fastavro.reader(f)
        schema = reader.schema
    fields = flatten_fields(schema)
    print(json.dumps({"fields": fields}, indent=2))


if __name__ == "__main__":
    main()
