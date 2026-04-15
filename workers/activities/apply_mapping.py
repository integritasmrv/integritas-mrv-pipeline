import yaml
from pathlib import Path
from typing import Any
from temporalio import activity


def _extract(data: dict, path: str) -> Any:
    parts = path.split(".")
    value = data
    for part in parts:
        if isinstance(value, dict):
            value = value.get(part)
        else:
            return None
    return value


def _transform(value: Any, transform: str) -> Any:
    if value is None:
        return None
    if not transform:
        return value
    if transform == "strip":
        return str(value).strip()
    if transform == "upper":
        return str(value).upper()
    if transform == "lower":
        return str(value).lower()
    if transform == "string":
        return str(value)
    if transform == "int":
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    if transform.startswith("const:"):
        return transform.split(":", 1)[1]
    if transform.startswith("concat_with:"):
        concat_field = transform.split(":", 1)[1]
        return str(value) + " " + str(concat_field) if value else str(concat_field)
    return value


def _set_nested(data: dict, path: str, value: Any) -> None:
    parts = path.split(".")
    current = data
    for part in parts[:-1]:
        if part not in current:
            current[part] = {}
        current = current[part]
    current[parts[-1]] = value


@activity.defn
async def apply_mapping(payload: dict, mapping_name: str) -> dict:
    config = _load_mapping_with_extends(mapping_name)

    if "targets" in config:
        result = {}
        for target_name, target_config in config["targets"].items():
            target_result = {}
            table = target_config.get("table", "")
            if table:
                target_result["table"] = table
            for field in target_config.get("fields", []):
                value = _extract(payload, field["source"])
                value = _transform(value, field.get("transform", ""))
                _set_nested(target_result, field["target"], value)
            if target_result:
                result[target_name] = target_result
        return result

    result = {}
    for field in config.get("fields", []):
        value = _extract(payload, field["source"])
        value = _transform(value, field.get("transform", ""))
        _set_nested(result, field["target"], value)
    return result


def _load_mapping_with_extends(mapping_name: str) -> dict:
    base_path = Path(f"/app/mappings/{mapping_name}.yaml")
    config = yaml.safe_load(base_path.read_text())

    if "extends" in config:
        parent_name = config.pop("extends")
        parent_config = _load_mapping_with_extends(parent_name)
        parent_fields = {(f["source"], f.get("target")): f for f in parent_config.get("fields", [])}
        child_fields = {(f["source"], f.get("target")): f for f in config.get("fields", [])}
        merged = {**parent_fields, **child_fields}
        config["fields"] = list(merged.values())
    return config


@activity.defn
def apply_mapping_batch(file_path: str, mapping_name: str) -> list[dict]:
    import polars as pl
    config = yaml.safe_load(
        Path(f"/app/mappings/{mapping_name}.yaml").read_text()
    )
    df = pl.read_csv(
        file_path,
        separator=config.get("separator", ","),
        encoding=config.get("encoding", "utf-8"),
        infer_schema_length=0,
    )
    rename_map = {f["source"]: f["target"].split(".")[-1] for f in config["fields"]}
    return df.rename(rename_map).to_dicts()
