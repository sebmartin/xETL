from contextlib import contextmanager
from datetime import datetime
import logging
from enum import Enum

from dataclasses import dataclass
import sys


class LogContext(Enum):
    NONE = 0
    APP = 1
    JOB = 2
    STEP = 3


@dataclass
class Decorators:
    record_prefix: str
    header_prefix: str
    header_suffix: str


log_decorations = {
    LogContext.NONE: Decorators(record_prefix="", header_prefix="", header_suffix=""),
    LogContext.APP: Decorators(record_prefix="│", header_prefix="╭" + "─" * 2 + "╴", header_suffix=" ╶╴╴╶ ╶"),
    LogContext.JOB: Decorators(record_prefix="║", header_prefix="╔" + "═" * 2 + "╸", header_suffix=" ═╴╴╶ ╶"),
    LogContext.STEP: Decorators(record_prefix="┃", header_prefix="┏" + "━" * 2 + "╸", header_suffix=" ━╴╴╶ ╶"),
}


def esc(code):
    return f"\033[{code}m"


class Color(Enum):
    END = esc("0")
    BRIGHT_WHITE = esc("1;37")
    RED = esc("91")
    YELLOW = esc("93")
    BLUE = esc("2;34")
    GRAY = esc("90")


def colored(text, color: Color):
    return f"{color.value}{text}{Color.END.value}" if sys.stdout.isatty() else text


class NestedFormatter(logging.Formatter):
    def __init__(self, context: LogContext, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.context = context
        self.stack: list[tuple] = []
        self.header = False

    def push_context(self, context: LogContext, header: bool = False):
        self.stack.append((self.context, self.header))
        self.set_context(context, header)

    def pop_context(self):
        if self.stack:
            self.set_context(*self.stack.pop())
        else:
            self.set_context(LogContext.NONE, False)

    def set_context(self, context: LogContext, header: bool = False):
        self.context = context
        self.header = header

    def _formatted_date(self, record: logging.LogRecord):
        return f"{datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')}.{record.msecs:03.0f}"

    def format(self, record: logging.LogRecord):
        match record.levelname:
            case "ERROR":
                message = colored(f"ERROR {record.msg}", Color.RED)
            case "WARNING":
                message = colored(f"WARNING {record.msg}", Color.YELLOW)
            case _:
                message = record.msg

        if self.header:
            prefix = colored(log_decorations[self.context].header_prefix, Color.BLUE)
            suffix = colored(log_decorations[self.context].header_suffix, Color.BLUE)
            log_format = f"{prefix}{colored(message, Color.BRIGHT_WHITE)}{suffix}"
        else:
            prefix = colored(log_decorations[self.context].record_prefix, Color.BLUE)
            if self.context in (LogContext.NONE, LogContext.APP):
                prefix = f"{prefix} " if prefix else ""
                log_format = f"{prefix}{message}"
            else:
                log_format = (
                    f"{prefix}{colored(self._formatted_date(record), Color.GRAY)}{colored('┊', Color.BLUE)} {message}"
                )

        return log_format


@contextmanager
def log_context(context: LogContext, header: str):
    root_logger = logging.getLogger()

    def push_context(context: LogContext, header: bool = False):
        for handler in root_logger.handlers:
            if isinstance(handler.formatter, NestedFormatter):
                handler.formatter.push_context(context, header)

    def set_context(context: LogContext, header: bool = False):
        for handler in root_logger.handlers:
            if isinstance(handler.formatter, NestedFormatter):
                handler.formatter.set_context(context, header)

    def pop_context():
        for handler in root_logger.handlers:
            if isinstance(handler.formatter, NestedFormatter):
                handler.formatter.pop_context()

    push_context(context, header=True)
    root_logger.info(header)
    set_context(context, header=False)
    yield
    pop_context()


def configure_logging(root_logger):
    logging.basicConfig(level=logging.DEBUG)
    formatter = NestedFormatter(LogContext.NONE)
    for handler in root_logger.handlers:
        handler.setFormatter(formatter)


configure_logging(logging.getLogger())
