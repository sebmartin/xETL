import mock
import pytest
import logging
from metl.core.logging import LogContext, configure_logging, log_context


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
    configure_logging(logger)
    return logger


@mock.patch("metl.core.logging.NestedFormatter._formatted_date", return_value="2023-11-13 23:23:51.228")
@mock.patch("metl.core.logging.sys.stdout.isatty", return_value=False)
def test_logging_all_no_tty_not_colored(_, __, logger, mock_handler):
    logger.info("Some info without a context")
    logger.warning("A warning without a context")
    logger.error("An error without a context")
    with log_context(LogContext.APP, "My cool app"):
        logger.info("Some info at the APP level")
        logger.warning("A warning at the APP level")
        logger.error("An error at the APP level")

        with log_context(LogContext.JOB, "Job 1"):
            logger.info("Some info at the JOB 1 level")
            logger.warning("A warning at the JOB 1 level")
            logger.error("An error at the JOB 1 level")

            with log_context(LogContext.STEP, "Step 1.1") as footer:
                logger.info("Some info at the STEP 1.1 level")
                logger.warning("A warning at the STEP 1.1 level")
                logger.error("An error at the STEP 1.1 level")
                footer("Return code: 0")

            with log_context(LogContext.STEP, "Step 1.2") as footer:
                logger.info("Some info at the STEP 1.2 level")
                logger.warning("A warning at the STEP 1.2 level")
                logger.error("An error at the STEP 1.2 level")
                footer("Return code: 0")

        with log_context(LogContext.JOB, "Job 2"):
            logger.info("Some info at the JOB 2 level")
            logger.warning("A warning at the JOB 2 level")
            logger.error("An error at the JOB 2 level")

            with log_context(LogContext.STEP, "Step 2.1") as footer:
                logger.info("Some info at the STEP 2.1 level")
                logger.warning("A warning at the STEP 2.1 level")
                logger.error("An error at the STEP 2.1 level")
                footer("Return code: 0")
    logger.info("Add one.")

    assert mock_handler.messages == [
        "Some info without a context",
        "WARNING A warning without a context",
        "ERROR An error without a context",
        "╭──╴My cool app ╶╴╴╶ ╶",
        "│ Some info at the APP level",
        "│ WARNING A warning at the APP level",
        "│ ERROR An error at the APP level",
        "╔══╸Job 1 ═╴╴╶ ╶",
        "║ Some info at the JOB 1 level",
        "║ WARNING A warning at the JOB 1 level",
        "║ ERROR An error at the JOB 1 level",
        "║┏━━╸Step 1.1 ━╴╴╶ ╶",
        "║┃2023-11-13 23:23:51.228┊ Some info at the STEP 1.1 level",
        "║┃2023-11-13 23:23:51.228┊ WARNING A warning at the STEP 1.1 level",
        "║┃2023-11-13 23:23:51.228┊ ERROR An error at the STEP 1.1 level",
        "║┗━━╸Return code: 0 ━╴╴╶ ╶",
        "║┏━━╸Step 1.2 ━╴╴╶ ╶",
        "║┃2023-11-13 23:23:51.228┊ Some info at the STEP 1.2 level",
        "║┃2023-11-13 23:23:51.228┊ WARNING A warning at the STEP 1.2 level",
        "║┃2023-11-13 23:23:51.228┊ ERROR An error at the STEP 1.2 level",
        "║┗━━╸Return code: 0 ━╴╴╶ ╶",
        "╔══╸Job 2 ═╴╴╶ ╶",
        "║ Some info at the JOB 2 level",
        "║ WARNING A warning at the JOB 2 level",
        "║ ERROR An error at the JOB 2 level",
        "║┏━━╸Step 2.1 ━╴╴╶ ╶",
        "║┃2023-11-13 23:23:51.228┊ Some info at the STEP 2.1 level",
        "║┃2023-11-13 23:23:51.228┊ WARNING A warning at the STEP 2.1 level",
        "║┃2023-11-13 23:23:51.228┊ ERROR An error at the STEP 2.1 level",
        "║┗━━╸Return code: 0 ━╴╴╶ ╶",
        "Add one.",
    ]


@mock.patch("metl.core.logging.NestedFormatter._formatted_date", return_value="2023-11-13 23:23:51.228")
@mock.patch("metl.core.logging.sys.stdout.isatty", return_value=True)
def test_logging_all_tty_is_colored(_, __, logger, mock_handler):
    logger.info("Some info without a context")
    logger.warning("A warning without a context")
    logger.error("An error without a context")
    with log_context(LogContext.APP, "My cool app"):
        logger.info("Some info at the APP level")
        logger.warning("A warning at the APP level")
        logger.error("An error at the APP level")

        with log_context(LogContext.JOB, "Job 1"):
            logger.info("Some info at the JOB 1 level")
            logger.warning("A warning at the JOB 1 level")
            logger.error("An error at the JOB 1 level")

            with log_context(LogContext.STEP, "Step 1.1") as footer:
                logger.info("Some info at the STEP 1.1 level")
                logger.warning("A warning at the STEP 1.1 level")
                logger.error("An error at the STEP 1.1 level")
                footer("Return code: 0")

            with log_context(LogContext.STEP, "Step 1.2") as footer:
                logger.info("Some info at the STEP 1.2 level")
                logger.warning("A warning at the STEP 1.2 level")
                logger.error("An error at the STEP 1.2 level")
                footer("Return code: 0")

        with log_context(LogContext.JOB, "Job 2"):
            logger.info("Some info at the JOB 2 level")
            logger.warning("A warning at the JOB 2 level")
            logger.error("An error at the JOB 2 level")

            with log_context(LogContext.STEP, "Step 2.1") as footer:
                logger.info("Some info at the STEP 2.1 level")
                logger.warning("A warning at the STEP 2.1 level")
                logger.error("An error at the STEP 2.1 level")
                footer("Return code: 0")
    logger.info("Add one.")

    assert mock_handler.messages == [
        "\x1b[2;34m\x1b[0m Some info without a context",
        "\x1b[2;34m\x1b[0m \x1b[93mWARNING A warning without a context\x1b[0m",
        "\x1b[2;34m\x1b[0m \x1b[91mERROR An error without a context\x1b[0m",
        "\x1b[2;34m╭──╴\x1b[0m\x1b[1;37mMy cool app\x1b[0m\x1b[2;34m ╶╴╴╶ ╶\x1b[0m",
        "\x1b[2;34m│\x1b[0m Some info at the APP level",
        "\x1b[2;34m│\x1b[0m \x1b[93mWARNING A warning at the APP level\x1b[0m",
        "\x1b[2;34m│\x1b[0m \x1b[91mERROR An error at the APP level\x1b[0m",
        "\x1b[2;34m╔══╸\x1b[0m\x1b[1;37mJob 1\x1b[0m\x1b[2;34m ═╴╴╶ ╶\x1b[0m",
        "\x1b[2;34m║\x1b[0m Some info at the JOB 1 level",
        "\x1b[2;34m║\x1b[0m \x1b[93mWARNING A warning at the JOB 1 level\x1b[0m",
        "\x1b[2;34m║\x1b[0m \x1b[91mERROR An error at the JOB 1 level\x1b[0m",
        "\x1b[2;34m║┏━━╸\x1b[0m\x1b[1;37mStep 1.1\x1b[0m\x1b[2;34m ━╴╴╶ ╶\x1b[0m",
        "\x1b[2;34m║┃\x1b[0m\x1b[90m2023-11-13 23:23:51.228\x1b[0m\x1b[2;34m┊\x1b[0m "
        "Some info at the STEP 1.1 level",
        "\x1b[2;34m║┃\x1b[0m\x1b[90m2023-11-13 23:23:51.228\x1b[0m\x1b[2;34m┊\x1b[0m "
        "\x1b[93mWARNING A warning at the STEP 1.1 level\x1b[0m",
        "\x1b[2;34m║┃\x1b[0m\x1b[90m2023-11-13 23:23:51.228\x1b[0m\x1b[2;34m┊\x1b[0m "
        "\x1b[91mERROR An error at the STEP 1.1 level\x1b[0m",
        "\x1b[2;34m║┗━━╸\x1b[0m\x1b[1;37mReturn code: 0\x1b[0m\x1b[2;34m ━╴╴╶ " "╶\x1b[0m",
        "\x1b[2;34m║┏━━╸\x1b[0m\x1b[1;37mStep 1.2\x1b[0m\x1b[2;34m ━╴╴╶ ╶\x1b[0m",
        "\x1b[2;34m║┃\x1b[0m\x1b[90m2023-11-13 23:23:51.228\x1b[0m\x1b[2;34m┊\x1b[0m "
        "Some info at the STEP 1.2 level",
        "\x1b[2;34m║┃\x1b[0m\x1b[90m2023-11-13 23:23:51.228\x1b[0m\x1b[2;34m┊\x1b[0m "
        "\x1b[93mWARNING A warning at the STEP 1.2 level\x1b[0m",
        "\x1b[2;34m║┃\x1b[0m\x1b[90m2023-11-13 23:23:51.228\x1b[0m\x1b[2;34m┊\x1b[0m "
        "\x1b[91mERROR An error at the STEP 1.2 level\x1b[0m",
        "\x1b[2;34m║┗━━╸\x1b[0m\x1b[1;37mReturn code: 0\x1b[0m\x1b[2;34m ━╴╴╶ " "╶\x1b[0m",
        "\x1b[2;34m╔══╸\x1b[0m\x1b[1;37mJob 2\x1b[0m\x1b[2;34m ═╴╴╶ ╶\x1b[0m",
        "\x1b[2;34m║\x1b[0m Some info at the JOB 2 level",
        "\x1b[2;34m║\x1b[0m \x1b[93mWARNING A warning at the JOB 2 level\x1b[0m",
        "\x1b[2;34m║\x1b[0m \x1b[91mERROR An error at the JOB 2 level\x1b[0m",
        "\x1b[2;34m║┏━━╸\x1b[0m\x1b[1;37mStep 2.1\x1b[0m\x1b[2;34m ━╴╴╶ ╶\x1b[0m",
        "\x1b[2;34m║┃\x1b[0m\x1b[90m2023-11-13 23:23:51.228\x1b[0m\x1b[2;34m┊\x1b[0m "
        "Some info at the STEP 2.1 level",
        "\x1b[2;34m║┃\x1b[0m\x1b[90m2023-11-13 23:23:51.228\x1b[0m\x1b[2;34m┊\x1b[0m "
        "\x1b[93mWARNING A warning at the STEP 2.1 level\x1b[0m",
        "\x1b[2;34m║┃\x1b[0m\x1b[90m2023-11-13 23:23:51.228\x1b[0m\x1b[2;34m┊\x1b[0m "
        "\x1b[91mERROR An error at the STEP 2.1 level\x1b[0m",
        "\x1b[2;34m║┗━━╸\x1b[0m\x1b[1;37mReturn code: 0\x1b[0m\x1b[2;34m ━╴╴╶ " "╶\x1b[0m",
        "\x1b[2;34m\x1b[0m Add one.",
    ]
