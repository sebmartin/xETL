from pydantic import BaseModel, field_validator
from traitlets import Any

from xetl.models import EnvVariableType
from xetl.models.utils.run import parse_run_command


class TaskTestCase(BaseModel):
    """
    Defines test cases used to verify that a custom task is functioning as expected. The test case
    consists of a set of environment variables and a command to execute. The command is expected to
    return a zero exit code if the task is functioning as expected. If the command returns a non-zero
    exit code, the task is considered to have failed the test case.
    """

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
