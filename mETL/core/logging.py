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
    footer_prefix: str
    header_suffix: str


log_decorations = {
    LogContext.NONE: Decorators(record_prefix="", header_prefix="", footer_prefix="", header_suffix=""),
    LogContext.APP: Decorators(
        header_prefix="╭" + "─" * 2 + "╴",
        record_prefix="│",
        footer_prefix="╰" + "─" * 2 + "╴",
        header_suffix=" ╶╴╴╶ ╶",
    ),
    LogContext.JOB: Decorators(
        header_prefix="╔" + "═" * 2 + "╸",
        record_prefix="║",
        footer_prefix="╚" + "═" * 2 + "╸",
        header_suffix=" ═╴╴╶ ╶",
    ),
    LogContext.STEP: Decorators(
        header_prefix="║┏" + "━" * 2 + "╸",
        record_prefix="║┃",
        footer_prefix="║┗" + "━" * 2 + "╸",
        header_suffix=" ━╴╴╶ ╶",
    ),
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


class LogLineType(Enum):
    HEADER = "header"
    NORMAL = "normal"
    FOOTER = "footer"


def colored(text, color: Color):
    return f"{color.value}{text}{Color.END.value}" if sys.stdout.isatty() else text


class NestedFormatter(logging.Formatter):
    def __init__(self, context: LogContext, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.context = context
        self.stack: list[tuple] = []
        self.line_type = LogLineType.NORMAL

    def push_context(self, context: LogContext, line_type: LogLineType = LogLineType.NORMAL):
        self.stack.append((self.context, self.line_type))
        self.set_context(context, line_type)

    def pop_context(self):
        if self.stack:
            self.set_context(*self.stack.pop())
        else:
            self.set_context(LogContext.NONE, LogLineType.NORMAL)

    def set_context(self, context: LogContext, line_type: LogLineType = LogLineType.NORMAL):
        self.context = context
        self.line_type = line_type

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

        match self.line_type:
            case LogLineType.HEADER:
                prefix = colored(log_decorations[self.context].header_prefix, Color.BLUE)
                suffix = colored(log_decorations[self.context].header_suffix, Color.BLUE)
                log_format = f"{prefix}{colored(message, Color.BRIGHT_WHITE)}{suffix}"
            case LogLineType.FOOTER:
                prefix = colored(log_decorations[self.context].footer_prefix, Color.BLUE)
                suffix = colored(log_decorations[self.context].header_suffix, Color.BLUE)
                log_format = f"{prefix}{colored(message, Color.BRIGHT_WHITE)}{suffix}"
            case LogLineType.NORMAL:
                prefix = colored(log_decorations[self.context].record_prefix, Color.BLUE)
                if self.context in (LogContext.NONE, LogContext.APP, LogContext.JOB):
                    prefix = f"{prefix} " if prefix else ""
                    log_format = f"{prefix}{message}"
                else:
                    log_format = f"{prefix}{colored(self._formatted_date(record), Color.GRAY)}{colored('┊', Color.BLUE)} {message}"

        return log_format


@contextmanager
def log_context(context: LogContext, header: str):
    root_logger = logging.getLogger()

    def push_context(context: LogContext, line_type: LogLineType = LogLineType.NORMAL):
        for handler in root_logger.handlers:
            if isinstance(handler.formatter, NestedFormatter):
                handler.formatter.push_context(context, line_type)

    def set_context(context: LogContext, line_type: LogLineType = LogLineType.NORMAL):
        for handler in root_logger.handlers:
            if isinstance(handler.formatter, NestedFormatter):
                handler.formatter.set_context(context, line_type)

    def pop_context():
        for handler in root_logger.handlers:
            if isinstance(handler.formatter, NestedFormatter):
                handler.formatter.pop_context()

    push_context(context, line_type=LogLineType.HEADER)
    root_logger.info(header)
    set_context(context, line_type=LogLineType.NORMAL)

    tail_message = None

    def set_tail_message(message):
        nonlocal tail_message
        tail_message = message

    try:
        yield set_tail_message
    finally:
        if tail_message:
            set_context(context, line_type=LogLineType.FOOTER)
            root_logger.info(tail_message)
        pop_context()


def configure_logging(root_logger):
    logging.basicConfig(level=logging.DEBUG)
    formatter = NestedFormatter(LogContext.NONE)
    for handler in root_logger.handlers:
        handler.setFormatter(formatter)


configure_logging(logging.getLogger())
