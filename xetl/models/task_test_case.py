from pydantic import BaseModel, ValidationInfo, field_validator
from traitlets import Any

from xetl.models import EnvVariableType
from xetl.models.utils.run import parse_run_command


class TaskTestCase(BaseModel):
    env: dict[str, EnvVariableType] = {}
    verify: list[str]
    setup: list[str] | None = None
    teardown: list[str] | None = None

    @field_validator("verify", "setup", "teardown", mode="before")
    @classmethod
    def generate_run_command(cls, data: Any, info: ValidationInfo) -> list[str]:
        if run_command := parse_run_command(data):
            return run_command
        raise ValueError(
            f"Task test '{info.field_name}' command must be a string, a list of strings, or a script object, received: {data}"
        )
