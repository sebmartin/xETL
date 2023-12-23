import io
from textwrap import dedent
from typing import Generator
import pytest

from xetl.argparse import ArgumentParser
from xetl.models.transform import Transform

TRANSFORM_PATH = "./tests/fixtures/transforms/download/manifest.yml"


@pytest.fixture
def transform() -> Transform:
    yaml = dedent(
        """
        name: download
        description: Download files from a remote server
        env-type: bash
        run-command: curl http://www.example.com
        """
    )
    return Transform.from_yaml(yaml, "./tests/fixtures/transforms/download")


@pytest.fixture
def output_buffer() -> Generator[io.StringIO, None, None]:
    with io.StringIO() as output:
        yield output


@pytest.mark.parametrize(
    "transform",
    [
        TRANSFORM_PATH,
        Transform.from_file(TRANSFORM_PATH),
    ],
    ids=["from_file", "from_transform"],
)
def test_argument_parser_from_file_or_transform(transform):
    assert isinstance(ArgumentParser(transform), ArgumentParser)


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
    transform = Transform.from_yaml(yaml, "./tests/fixtures/transforms/download")
    ArgumentParser(transform).print_help(file=output_buffer)
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
def test_argument_parser_types(type, value, output_buffer: io.StringIO):
    yaml = dedent(
        f"""
        name: dummy
        description: A dummy application to test argument types
        env-type: python
        env:
          VAR:
            description: The best variable ever
            type: {type}
            required: true
        run-command: python -m dummy
        """
    )
    transform = Transform.from_yaml(yaml, "./tests/fixtures/transforms/download")
    parser = ArgumentParser(transform)
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
