from typing import Any, Type
from pydantic import BaseModel, field_validator, model_validator

from xetl.models import EnvVariableType
from xetl.models.utils.dicts import conform_key


class TaskInputDetails(BaseModel):
    description: str | None = None
    required: bool = True
    default: Any | None = None
    type: Type[EnvVariableType] | None = None

    @model_validator(mode="before")
    @classmethod
    def set_defaults(cls, data: Any) -> Any:
        if isinstance(data, dict):
            data = {conform_key(key): value for key, value in data.items()}
            if "optional" in data:
                if "required" in data:
                    raise ValueError("Cannot specify both `required` and `optional`")
                data["required"] = not data.pop("optional")
            if data.get("default") is not None:
                data["required"] = data.get("required", False)
        return data

    @field_validator("type", mode="before")
    def valid_type(cls, value: Any) -> Any:
        if isinstance(value, str):
            mapping = {
                "str": str,
                "string": str,
                "int": int,
                "integer": int,
                "float": float,
                "decimal": float,
                "bool": bool,
                "boolean": bool,
            }
            if type_ := mapping.get(value.lower()):
                return type_
        return value
