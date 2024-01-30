import shlex
import sys
from typing import Any


def parse_run_command(data: Any) -> list[str] | None:
    """
    Pre-validation method for converting a task's `run` command into a list of strings
    that can be passed to `subprocess.Popen()`.
    """

    if isinstance(data, str):
        # Parse strings as a raw executable command
        return shlex.split(data)
    if isinstance(data, list):
        # Parse lists as raw executable commands and coalesce items to strings
        return [str(value) for value in data]
    if isinstance(data, dict) and "script" in data.keys():
        # Parse scripts as inline scripts
        script = data["script"]
        interpreter = data.get("interpreter", f"{sys.executable} -c")
        return shlex.split(interpreter) + [script]
    return None
