import io
import os
from textwrap import dedent
from typing import Generator
import mock
import pytest

from xetl.argparse import ArgumentParser
from xetl.models.command import Command

COMMAND_PATH = "./tests/fixtures/commands/download/manifest.yml"


@pytest.fixture
def command() -> Command:
    yaml = dedent(
        """
        name: download
        description: Download files from a remote server
        env-type: bash
        run-command: curl http://www.example.com
        """
    )
    return Command.from_yaml(yaml, "./tests/fixtures/commands/download")


@pytest.fixture
def output_buffer() -> Generator[io.StringIO, None, None]:
    with io.StringIO() as output:
        yield output


@pytest.mark.parametrize(
    "command",
    [
        COMMAND_PATH,
        Command.from_file(COMMAND_PATH),
    ],
    ids=["from_file", "from_command"],
)
def test_argument_parser_from_file_or_command(command):
    assert isinstance(ArgumentParser(command), ArgumentParser)


def test_argument_parser_help(output_buffer: io.StringIO):
    yaml = dedent(
        """
        name: download
        description: Download files from a remote server
        env-type: python
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
        run-command: python -m download
        """
    )
    command = Command.from_yaml(yaml, "./tests/fixtures/commands/download")
    ArgumentParser(command).print_help(file=output_buffer)
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
        env-type: python
        env:
          VAR:
            description: The best variable ever
            type: {type}
            required: true
        run-command: python -m dummy
        """
    )
    command = Command.from_yaml(yaml, "./tests/fixtures/commands/download")
    parser = ArgumentParser(command)
    assert isinstance(parser.parse_args([f"--var={value}"]).var, eval(type))
    assert parser.parse_args([f"--var={value}"]).var == value


@pytest.mark.parametrize("required", [True, False])
def test_argument_parser_required(required, capsys):
    yaml = dedent(
        f"""
        name: dummy
        description: A dummy job to test argument types
        env-type: python
        env:
          VAR:
            description: The best variable ever
            required: {required}
        run-command: python -m dummy
        """
    )
    command = Command.from_yaml(yaml, "./tests/fixtures/commands/download")
    parser = ArgumentParser(command)

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
        env-type: python
        env:
          VAR:
            description: The best variable ever
            optional: true
            type: int
            default: 1
        run-command: python -m dummy
        """
    )
    command = Command.from_yaml(yaml, "./tests/fixtures/commands/download")
    parser = ArgumentParser(command)
    assert parser.parse_args([]).var == 1
    assert parser.parse_args(["--var=2"]).var == 2


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
        env-type: python
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
        run-command: python -m download
        """
    )
    command = Command.from_yaml(yaml, "./tests/fixtures/commands/download")
    try:
        args = ArgumentParser(command).parse_args([])
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
        env-type: python
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
        run-command: python -m download
        """
    )
    command = Command.from_yaml(yaml, "./tests/fixtures/commands/download")
    try:
        args = ArgumentParser(command).parse_args(["--url=http://www.example.com"])
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
        env-type: python
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
        run-command: python -m download
        """
    )
    command = Command.from_yaml(yaml, "./tests/fixtures/commands/download")
    try:
        args = ArgumentParser(command).parse_args(["--url=http://www.cli-url.com", "--throttle=2.2"])
        assert args.url == "http://www.cli-url.com"
        assert args.throttle == 2.2
        assert args.follow_redirects == True
    except SystemExit:
        pytest.fail("All arguments should have been used from the env, output was:\n" + capsys.readouterr().err)
