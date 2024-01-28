import io
import os
import sys
from textwrap import dedent
from typing import Generator

import mock
import pytest

from xetl.argparse import ArgumentParser
from xetl.models.task import Task

COMMAND_PATH = "./tests/fixtures/tasks/download/manifest.yml"


@pytest.fixture
def task() -> Task:
    yaml = dedent(
        """
        name: download
        description: Download files from a remote server
        run: curl http://www.example.com
        """
    )
    return Task.from_yaml(yaml, "./tests/fixtures/tasks/download")


@pytest.fixture
def output_buffer() -> Generator[io.StringIO, None, None]:
    with io.StringIO() as output:
        yield output


@pytest.mark.parametrize(
    "task",
    [
        COMMAND_PATH,
        Task.from_file(COMMAND_PATH),
    ],
    ids=["from_file", "from_task"],
)
def test_argument_parser_from_file_or_task(task):
    assert isinstance(ArgumentParser(task), ArgumentParser)


def test_argument_parser_help(output_buffer: io.StringIO):
    yaml = dedent(
        """
        name: download
        description: Download files from a remote server
        env:
          URL:
            description: URL to download
            type: str
            required: true
          THROTTLE:
            description: Seconds to wait between downloads
            type: float
            optional: true
          FOLLOW_REDIRECTS:
            description: Follow HTTP redirects
            type: bool
            optional: true
        run: python -m download
        """
    )
    task = Task.from_yaml(yaml, "./tests/fixtures/tasks/download")
    ArgumentParser(task, "python -m download").print_help(file=output_buffer)
    assert (
        output_buffer.getvalue().strip()
        == dedent(
            """
            usage: python -m download [-h] --url URL [--throttle THROTTLE]
                                      [--follow-redirects FOLLOW_REDIRECTS]

            Download files from a remote server

            options:
              -h, --help            show this help message and exit
              --url URL             URL to download
              --throttle THROTTLE   Seconds to wait between downloads
              --follow-redirects FOLLOW_REDIRECTS
                                    Follow HTTP redirects
            """
        ).strip()
    )


@pytest.mark.parametrize("type, value", [("int", 1), ("float", 1.1), ("bool", True), ("str", "one")])
def test_argument_parser_types(type, value):
    yaml = dedent(
        f"""
        name: dummy
        description: A dummy job to test argument types
        env:
          VAR:
            description: The best variable ever
            type: {type}
            required: true
        run: python -m dummy
        """
    )
    task = Task.from_yaml(yaml, "./tests/fixtures/tasks/download")
    parser = ArgumentParser(task)
    assert isinstance(parser.parse_args([f"--var={value}"]).var, eval(type))
    assert parser.parse_args([f"--var={value}"]).var == value


@pytest.mark.parametrize("required", [True, False])
def test_argument_parser_required(required, capsys):
    yaml = dedent(
        f"""
        name: dummy
        description: A dummy job to test argument types
        env:
          VAR:
            description: The best variable ever
            required: {required}
        run: python -m dummy
        """
    )
    task = Task.from_yaml(yaml, "./tests/fixtures/tasks/download")
    parser = ArgumentParser(task)

    if required:
        with pytest.raises(SystemExit):
            parser.parse_args([])
        assert "error: the following arguments are required: --var" in capsys.readouterr().err
    else:
        assert parser.parse_args([]).var is None


def test_argument_parser_default():
    yaml = dedent(
        """
        name: dummy
        description: A dummy job to test argument types
        env:
          VAR:
            description: The best variable ever
            optional: true
            type: int
            default: 1
        run: python -m dummy
        """
    )
    task = Task.from_yaml(yaml, "./tests/fixtures/tasks/download")
    parser = ArgumentParser(task)
    assert parser.parse_args([]).var == 1
    assert parser.parse_args(["--var=2"]).var == 2


@mock.patch.object(sys, "argv", ["dummy", "--var=2"])
def test_argument_parser_default_argv():
    yaml = dedent(
        """
        name: dummy
        description: A dummy job to test argument types
        env:
          VAR:
            description: The best variable ever
            optional: true
            type: int
            default: 1
        run: python -m dummy
        """
    )
    task = Task.from_yaml(yaml, "./tests/fixtures/tasks/download")
    parser = ArgumentParser(task)
    assert parser.parse_args().var == 2


@mock.patch.dict(
    os.environ,
    {
        "URL": "http://www.example.com",
        "THROTTLE": "1.1",
        "FOLLOW_REDIRECTS": "true",
    },
)
def test_argument_parser_all_from_env(capsys):
    yaml = dedent(
        """
        name: download
        description: Download files from a remote server
        env:
          URL:
            description: URL to download
            type: str
          THROTTLE:
            description: Seconds to wait between downloads
            type: float
          FOLLOW_REDIRECTS:
            description: Follow HTTP redirects
            type: bool
        run: python -m download
        """
    )
    task = Task.from_yaml(yaml, "./tests/fixtures/tasks/download")
    try:
        args = ArgumentParser(task).parse_args([])
        assert args.url == "http://www.example.com"
        assert args.throttle == 1.1
        assert args.follow_redirects == True
    except SystemExit:
        pytest.fail("All arguments should have been used from the env, output was:\n" + capsys.readouterr().err)


@mock.patch.dict(
    os.environ,
    {
        "THROTTLE": "1.1",
        "FOLLOW_REDIRECTS": "true",
    },
)
def test_argument_parser_some_from_env(capsys):
    yaml = dedent(
        """
        name: download
        description: Download files from a remote server
        env:
          URL:
            description: URL to download
            type: str
          THROTTLE:
            description: Seconds to wait between downloads
            type: float
          FOLLOW_REDIRECTS:
            description: Follow HTTP redirects
            type: bool
        run: python -m download
        """
    )
    task = Task.from_yaml(yaml, "./tests/fixtures/tasks/download")
    try:
        args = ArgumentParser(task).parse_args(["--url=http://www.example.com"])
        assert args.url == "http://www.example.com"
        assert args.throttle == 1.1
        assert args.follow_redirects == True
    except SystemExit:
        pytest.fail("All arguments should have been used from the env, output was:\n" + capsys.readouterr().err)


@mock.patch.dict(
    os.environ,
    {
        "URL": "http://www.example.com",
        "THROTTLE": "1.1",
        "FOLLOW_REDIRECTS": "true",
    },
)
def test_argument_parser_cli_args_override_env(capsys):
    yaml = dedent(
        """
        name: download
        description: Download files from a remote server
        env:
          URL:
            description: URL to download
            type: str
          THROTTLE:
            description: Seconds to wait between downloads
            type: float
          FOLLOW_REDIRECTS:
            description: Follow HTTP redirects
            type: bool
        run: python -m download
        """
    )
    task = Task.from_yaml(yaml, "./tests/fixtures/tasks/download")
    try:
        args = ArgumentParser(task).parse_args(["--url=http://www.cli-url.com", "--throttle=2.2"])
        assert args.url == "http://www.cli-url.com"
        assert args.throttle == 2.2
        assert args.follow_redirects == True
    except SystemExit:
        pytest.fail("All arguments should have been used from the env, output was:\n" + capsys.readouterr().err)
