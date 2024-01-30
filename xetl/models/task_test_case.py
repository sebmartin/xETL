from pydantic import BaseModel, field_validator
from traitlets import Any

from xetl.models import EnvVariableType
from xetl.models.utils.run import parse_run_command


class TaskTestCase(BaseModel):
    env: dict[str, EnvVariableType] = {}
    verify: list[str]

    @field_validator("verify", mode="before")
    @classmethod
    def generate_run_command(cls, data: Any) -> list[str]:
        if run_command := parse_run_command(data):
            return run_command
        raise ValueError(
            f"Task test verify command must be a string, a list of strings, or a script object, received: {data}"
        )
