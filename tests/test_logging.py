import logging

import mock
import pytest

from xetl.logging import LogContext, LogStyle, configure_logging, log_context


@pytest.fixture
def mock_handler():
    class TestHandler(logging.Handler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.messages = []

        def handle(self, record):
            self.messages.append(self.format(record))

    handler = TestHandler()
    handler.setLevel(logging.DEBUG)

    return handler


@pytest.fixture
def logger(mock_handler):
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(mock_handler)
    return logger


def print_logs(logger, style: LogStyle):
    configure_logging(logger, style=style)

    logger.info("Some info without a context")
    logger.warning("A warning without a context")
    logger.error("An error without a context")
    with log_context(LogContext.JOB, "My cool job"):
        logger.info("Some info at the JOB level")
        logger.warning("A warning at the JOB level")
        logger.error("An error at the JOB level")

        with log_context(LogContext.TASK, "Command 1"):
            logger.info("Some info at the TASK 1 level")
            logger.warning("A warning at the TASK 1 level")
            logger.error("An error at the TASK 1 level")

            with log_context(LogContext.COMMAND, "Task 1.1") as footer:
                logger.info("Some info at the COMMAND 1.1 level")
                logger.warning("A warning at the COMMAND 1.1 level")
                logger.error("An error at the COMMAND 1.1 level")
                footer("Return code: 0")

            with log_context(LogContext.COMMAND, "Task 1.2") as footer:
                logger.info("Some info at the COMMAND 1.2 level")
                logger.warning("A warning at the COMMAND 1.2 level")
                logger.error("An error at the COMMAND 1.2 level")
                footer("Return code: 0")

        with log_context(LogContext.TASK, "Command 2"):
            logger.info("Some info at the TASK 2 level")
            logger.warning("A warning at the TASK 2 level")
            logger.error("An error at the TASK 2 level")

            with log_context(LogContext.COMMAND, "Task 2.1") as footer:
                logger.info("Some info at the COMMAND 2.1 level")
                logger.warning("A warning at the COMMAND 2.1 level")
                logger.error("An error at the COMMAND 2.1 level")
                footer("Return code: 0")
    logger.info("Add one.")


@mock.patch("xetl.logging.NestedFormatter._formatted_date", return_value="2023-11-13 23:23:51.228")
@mock.patch("xetl.logging.sys.stdout.isatty", return_value=False)
def test_logging_all_no_tty_not_colored(_, __, logger, mock_handler):
    print_logs(logger, LogStyle.GAUDY)

    assert mock_handler.messages == [
        "Some info without a context",
        "WARNING A warning without a context",
        "ERROR An error without a context",
        "╭──╴My cool job ╶╴╴╶ ╶",
        "│ Some info at the JOB level",
        "│ WARNING A warning at the JOB level",
        "│ ERROR An error at the JOB level",
        "┏━━╸Command 1 ━╴╴╶ ╶",
        "┃ Some info at the TASK 1 level",
        "┃ WARNING A warning at the TASK 1 level",
        "┃ ERROR An error at the TASK 1 level",
        "┃╭──╴Task 1.1 ─╴╴╶ ╶",
        "┃│2023-11-13 23:23:51.228┊ Some info at the COMMAND 1.1 level",
        "┃│2023-11-13 23:23:51.228┊ WARNING A warning at the COMMAND 1.1 level",
        "┃│2023-11-13 23:23:51.228┊ ERROR An error at the COMMAND 1.1 level",
        "┃╰──╴Return code: 0 ─╴╴╶ ╶",
        "┃╭──╴Task 1.2 ─╴╴╶ ╶",
        "┃│2023-11-13 23:23:51.228┊ Some info at the COMMAND 1.2 level",
        "┃│2023-11-13 23:23:51.228┊ WARNING A warning at the COMMAND 1.2 level",
        "┃│2023-11-13 23:23:51.228┊ ERROR An error at the COMMAND 1.2 level",
        "┃╰──╴Return code: 0 ─╴╴╶ ╶",
        "┏━━╸Command 2 ━╴╴╶ ╶",
        "┃ Some info at the TASK 2 level",
        "┃ WARNING A warning at the TASK 2 level",
        "┃ ERROR An error at the TASK 2 level",
        "┃╭──╴Task 2.1 ─╴╴╶ ╶",
        "┃│2023-11-13 23:23:51.228┊ Some info at the COMMAND 2.1 level",
        "┃│2023-11-13 23:23:51.228┊ WARNING A warning at the COMMAND 2.1 level",
        "┃│2023-11-13 23:23:51.228┊ ERROR An error at the COMMAND 2.1 level",
        "┃╰──╴Return code: 0 ─╴╴╶ ╶",
        "Add one.",
    ]


@mock.patch("xetl.logging.NestedFormatter._formatted_date", return_value="2023-11-13 23:23:51.228")
@mock.patch("xetl.logging.sys.stdout.isatty", return_value=True)
def test_logging_all_tty_is_colored(_, __, logger, mock_handler):
    print_logs(logger, LogStyle.GAUDY)

    assert mock_handler.messages == [
        "\x1b[2;34m\x1b[0m Some info without a context",
        "\x1b[2;34m\x1b[0m \x1b[93mWARNING A warning without a context\x1b[0m",
        "\x1b[2;34m\x1b[0m \x1b[91mERROR An error without a context\x1b[0m",
        "\x1b[2;34m╭──╴\x1b[0m\x1b[1;37mMy cool job\x1b[0m\x1b[2;34m ╶╴╴╶ ╶\x1b[0m",
        "\x1b[2;34m│\x1b[0m Some info at the JOB level",
        "\x1b[2;34m│\x1b[0m \x1b[93mWARNING A warning at the JOB level\x1b[0m",
        "\x1b[2;34m│\x1b[0m \x1b[91mERROR An error at the JOB level\x1b[0m",
        "\x1b[2;34m┏━━╸\x1b[0m\x1b[1;37mCommand 1\x1b[0m\x1b[2;34m ━╴╴╶ ╶\x1b[0m",
        "\x1b[2;34m┃\x1b[0m Some info at the TASK 1 level",
        "\x1b[2;34m┃\x1b[0m \x1b[93mWARNING A warning at the TASK 1 level\x1b[0m",
        "\x1b[2;34m┃\x1b[0m \x1b[91mERROR An error at the TASK 1 level\x1b[0m",
        "\x1b[2;34m┃╭──╴\x1b[0m\x1b[1;37mTask 1.1\x1b[0m\x1b[2;34m ─╴╴╶ ╶\x1b[0m",
        "\x1b[2;34m┃│\x1b[0m\x1b[90m2023-11-13 23:23:51.228\x1b[0m\x1b[2;34m┊\x1b[0m "
        "Some info at the COMMAND 1.1 level",
        "\x1b[2;34m┃│\x1b[0m\x1b[90m2023-11-13 23:23:51.228\x1b[0m\x1b[2;34m┊\x1b[0m "
        "\x1b[93mWARNING A warning at the COMMAND 1.1 level\x1b[0m",
        "\x1b[2;34m┃│\x1b[0m\x1b[90m2023-11-13 23:23:51.228\x1b[0m\x1b[2;34m┊\x1b[0m "
        "\x1b[91mERROR An error at the COMMAND 1.1 level\x1b[0m",
        "\x1b[2;34m┃╰──╴\x1b[0m\x1b[1;37mReturn code: 0\x1b[0m\x1b[2;34m ─╴╴╶ " "╶\x1b[0m",
        "\x1b[2;34m┃╭──╴\x1b[0m\x1b[1;37mTask 1.2\x1b[0m\x1b[2;34m ─╴╴╶ ╶\x1b[0m",
        "\x1b[2;34m┃│\x1b[0m\x1b[90m2023-11-13 23:23:51.228\x1b[0m\x1b[2;34m┊\x1b[0m "
        "Some info at the COMMAND 1.2 level",
        "\x1b[2;34m┃│\x1b[0m\x1b[90m2023-11-13 23:23:51.228\x1b[0m\x1b[2;34m┊\x1b[0m "
        "\x1b[93mWARNING A warning at the COMMAND 1.2 level\x1b[0m",
        "\x1b[2;34m┃│\x1b[0m\x1b[90m2023-11-13 23:23:51.228\x1b[0m\x1b[2;34m┊\x1b[0m "
        "\x1b[91mERROR An error at the COMMAND 1.2 level\x1b[0m",
        "\x1b[2;34m┃╰──╴\x1b[0m\x1b[1;37mReturn code: 0\x1b[0m\x1b[2;34m ─╴╴╶ " "╶\x1b[0m",
        "\x1b[2;34m┏━━╸\x1b[0m\x1b[1;37mCommand 2\x1b[0m\x1b[2;34m ━╴╴╶ ╶\x1b[0m",
        "\x1b[2;34m┃\x1b[0m Some info at the TASK 2 level",
        "\x1b[2;34m┃\x1b[0m \x1b[93mWARNING A warning at the TASK 2 level\x1b[0m",
        "\x1b[2;34m┃\x1b[0m \x1b[91mERROR An error at the TASK 2 level\x1b[0m",
        "\x1b[2;34m┃╭──╴\x1b[0m\x1b[1;37mTask 2.1\x1b[0m\x1b[2;34m ─╴╴╶ ╶\x1b[0m",
        "\x1b[2;34m┃│\x1b[0m\x1b[90m2023-11-13 23:23:51.228\x1b[0m\x1b[2;34m┊\x1b[0m "
        "Some info at the COMMAND 2.1 level",
        "\x1b[2;34m┃│\x1b[0m\x1b[90m2023-11-13 23:23:51.228\x1b[0m\x1b[2;34m┊\x1b[0m "
        "\x1b[93mWARNING A warning at the COMMAND 2.1 level\x1b[0m",
        "\x1b[2;34m┃│\x1b[0m\x1b[90m2023-11-13 23:23:51.228\x1b[0m\x1b[2;34m┊\x1b[0m "
        "\x1b[91mERROR An error at the COMMAND 2.1 level\x1b[0m",
        "\x1b[2;34m┃╰──╴\x1b[0m\x1b[1;37mReturn code: 0\x1b[0m\x1b[2;34m ─╴╴╶ " "╶\x1b[0m",
        "\x1b[2;34m\x1b[0m Add one.",
    ]


@mock.patch("xetl.logging.NestedFormatter._formatted_date", return_value="2023-11-13 23:23:51.228")
@mock.patch("xetl.logging.sys.stdout.isatty", return_value=False)
def test_logging_style_moderate(_, __, logger, mock_handler):
    print_logs(logger, LogStyle.MODERATE)

    assert mock_handler.messages == [
        "Some info without a context",
        "WARNING A warning without a context",
        "ERROR An error without a context",
        "─╴My cool job╶─",
        "Some info at the JOB level",
        "WARNING A warning at the JOB level",
        "ERROR An error at the JOB level",
        "━╸Command 1╺━",
        "Some info at the TASK 1 level",
        "WARNING A warning at the TASK 1 level",
        "ERROR An error at the TASK 1 level",
        "═╴Task 1.1╶═",
        "2023-11-13 23:23:51.228┊ Some info at the COMMAND 1.1 level",
        "2023-11-13 23:23:51.228┊ WARNING A warning at the COMMAND 1.1 level",
        "2023-11-13 23:23:51.228┊ ERROR An error at the COMMAND 1.1 level",
        "═╴Return code: 0╶═",
        "═╴Task 1.2╶═",
        "2023-11-13 23:23:51.228┊ Some info at the COMMAND 1.2 level",
        "2023-11-13 23:23:51.228┊ WARNING A warning at the COMMAND 1.2 level",
        "2023-11-13 23:23:51.228┊ ERROR An error at the COMMAND 1.2 level",
        "═╴Return code: 0╶═",
        "━╸Command 2╺━",
        "Some info at the TASK 2 level",
        "WARNING A warning at the TASK 2 level",
        "ERROR An error at the TASK 2 level",
        "═╴Task 2.1╶═",
        "2023-11-13 23:23:51.228┊ Some info at the COMMAND 2.1 level",
        "2023-11-13 23:23:51.228┊ WARNING A warning at the COMMAND 2.1 level",
        "2023-11-13 23:23:51.228┊ ERROR An error at the COMMAND 2.1 level",
        "═╴Return code: 0╶═",
        "Add one.",
    ]


@mock.patch("xetl.logging.NestedFormatter._formatted_date", return_value="2023-11-13 23:23:51.228")
@mock.patch("xetl.logging.sys.stdout.isatty", return_value=False)
def test_logging_style_minimal(_, __, logger, mock_handler):
    print_logs(logger, LogStyle.MINIMAL)

    assert mock_handler.messages == [
        "Some info without a context",
        "WARNING A warning without a context",
        "ERROR An error without a context",
        "My cool job",
        "Some info at the JOB level",
        "WARNING A warning at the JOB level",
        "ERROR An error at the JOB level",
        "Command 1",
        "Some info at the TASK 1 level",
        "WARNING A warning at the TASK 1 level",
        "ERROR An error at the TASK 1 level",
        "Task 1.1",
        "2023-11-13 23:23:51.228 Some info at the COMMAND 1.1 level",
        "2023-11-13 23:23:51.228 WARNING A warning at the COMMAND 1.1 level",
        "2023-11-13 23:23:51.228 ERROR An error at the COMMAND 1.1 level",
        "Return code: 0",
        "Task 1.2",
        "2023-11-13 23:23:51.228 Some info at the COMMAND 1.2 level",
        "2023-11-13 23:23:51.228 WARNING A warning at the COMMAND 1.2 level",
        "2023-11-13 23:23:51.228 ERROR An error at the COMMAND 1.2 level",
        "Return code: 0",
        "Command 2",
        "Some info at the TASK 2 level",
        "WARNING A warning at the TASK 2 level",
        "ERROR An error at the TASK 2 level",
        "Task 2.1",
        "2023-11-13 23:23:51.228 Some info at the COMMAND 2.1 level",
        "2023-11-13 23:23:51.228 WARNING A warning at the COMMAND 2.1 level",
        "2023-11-13 23:23:51.228 ERROR An error at the COMMAND 2.1 level",
        "Return code: 0",
        "Add one.",
    ]
