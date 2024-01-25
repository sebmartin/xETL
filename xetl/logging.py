import logging
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class LogContext(Enum):
    NONE = 0
    JOB = 1
    TASK = 2
    COMMAND = 3


@dataclass
class Decorators:
    record_prefix: str
    header_prefix: str
    footer_prefix: str
    header_suffix: str

    @classmethod
    def none(cls) -> "Decorators":
        return cls("", "", "", "")


class LogStyle(Enum):
    MINIMAL = 0
    MODERATE = 1
    GAUDY = 2


def log_decorations(style: LogStyle, context: LogContext) -> Decorators:
    match style:
        case LogStyle.MINIMAL:
            return Decorators.none()

        case LogStyle.MODERATE:
            match context:
                case LogContext.NONE:
                    return Decorators.none()
                case LogContext.JOB:
                    return Decorators(
                        header_prefix="─╴",
                        record_prefix="",
                        footer_prefix="─╴",
                        header_suffix="╶─",
                    )
                case LogContext.TASK:
                    return Decorators(
                        header_prefix="━╸",
                        record_prefix="",
                        footer_prefix="━╸",
                        header_suffix="╺━",
                    )
                case LogContext.COMMAND:
                    return Decorators(
                        header_prefix="═╴",
                        record_prefix="",
                        footer_prefix="═╴",
                        header_suffix="╶═",
                    )

        case LogStyle.GAUDY:
            match context:
                case LogContext.NONE:
                    return Decorators.none()
                case LogContext.JOB:
                    return Decorators(
                        header_prefix="╭──╴",
                        record_prefix="│",
                        footer_prefix="╰──╴",
                        header_suffix=" ╶╴╴╶ ╶",
                    )
                case LogContext.TASK:
                    return Decorators(
                        header_prefix="┏━━╸",
                        record_prefix="┃",
                        footer_prefix="┗━━╸",
                        header_suffix=" ━╴╴╶ ╶",
                    )
                case LogContext.COMMAND:
                    return Decorators(
                        header_prefix="┃╭──╴",
                        record_prefix="┃│",
                        footer_prefix="┃╰──╴",
                        header_suffix=" ─╴╴╶ ╶",
                    )


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
    def __init__(
        self, style: LogStyle = LogStyle.GAUDY, context: LogContext = LogContext.NONE, *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.style = style
        self.context = context
        self.stack: list[tuple] = []
        self.line_type = LogLineType.NORMAL

    def push_context(self, context: LogContext, line_type: LogLineType = LogLineType.NORMAL):
        self.stack.append((self.context, self.line_type))
        self.set_context(context, line_type)

    def pop_context(self):
        if self.stack:
            self.set_context(*self.stack.pop())

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

        decorations = log_decorations(self.style, self.context)
        match self.line_type:
            case LogLineType.HEADER:
                prefix = colored(decorations.header_prefix, Color.BLUE)
                suffix = colored(decorations.header_suffix, Color.BLUE)
                log_format = f"{prefix}{colored(message, Color.BRIGHT_WHITE)}{suffix}"
            case LogLineType.FOOTER:
                prefix = colored(decorations.footer_prefix, Color.BLUE)
                suffix = colored(decorations.header_suffix, Color.BLUE)
                log_format = f"{prefix}{colored(message, Color.BRIGHT_WHITE)}{suffix}"
            case LogLineType.NORMAL:
                prefix = colored(decorations.record_prefix, Color.BLUE)
                if self.context in (LogContext.NONE, LogContext.JOB, LogContext.TASK):
                    prefix = f"{prefix} " if prefix else ""
                    log_format = f"{prefix}{message}"
                else:
                    datesep = "" if self.style == LogStyle.MINIMAL else "┊"
                    log_format = f"{prefix}{colored(self._formatted_date(record), Color.GRAY)}{colored(datesep, Color.BLUE)} {message}"

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


def configure_logging(root_logger, style: LogStyle = LogStyle.GAUDY):
    logging.basicConfig(level=logging.DEBUG)
    formatter = NestedFormatter(style=style)
    for handler in root_logger.handlers:
        handler.setFormatter(formatter)


configure_logging(logging.getLogger())
