import os
import re
from shutil import copytree
from textwrap import dedent

import mock
import pytest
import yaml
from mock import call
from pydantic import ValidationError

from xetl.models.task import Task
from xetl.models.command import EnvType, InputDetails, Command, discover_commands


def copy_tree(src, dst):
    copytree(src, dst, dirs_exist_ok=True)


def command_file(command_yaml: str, tmpdir):
    path = os.path.join(tmpdir, "manifest.yml")
    with open(path, "w") as fd:
        fd.write(command_yaml)
    return path


def command_with_env(env: str) -> str:
    return dedent(
        """
        name: simple-command
        type: command
        env-type: python
        {env}
        run-command: python run.py
        """
    ).format(env=dedent(env))


@pytest.fixture
def simple_command_manifest_yml():
    return dedent(
        """
        name: simple-command
        type: command
        env-type: python
        commands: /something
        env:
          FOO:
            description: something
            type: string
            required: true
          OPTION_WITH_HYPHENS:
            description: something else
          OUTPUT:
            description: the result of the command
            type: string
            required: true
        run-command: python run.py
        test-command: py.test
        """
    )


@pytest.fixture
def simple_command_manifest_path(simple_command_manifest_yml, tmpdir):
    return command_file(simple_command_manifest_yml, tmpdir)


@pytest.fixture
def bash_command_command_manifest_yml():
    return dedent(
        """
        name: bash-command-command
        env-type: bash
        run-command: ls -l ~/
        """
    )


@pytest.fixture
def bash_command_command_manifest_path(bash_command_command_manifest_yml, tmpdir):
    return command_file(bash_command_command_manifest_yml, tmpdir)


@pytest.fixture
def complex_command_command_manifest_yml():
    return dedent(
        """
        name: complex-command-command
        type: command
        env-type: bash
        run-command: echo "hello world" | awk '{print $2}'
        """
    )


@pytest.fixture
def complex_command_command_manifest_path(complex_command_command_manifest_yml, tmpdir):
    return command_file(complex_command_command_manifest_yml, tmpdir)


class TestDiscoverCommands:
    def test_discover_commands(self, commands_fixtures_path):
        commands = discover_commands(commands_fixtures_path)

        names_and_paths = [(name, command.path) for name, command in commands.items()]

        assert sorted(names_and_paths) == sorted(
            [
                ("splitter", "{repo_dir}/commands/splitter".format(repo_dir=commands_fixtures_path)),
                ("download", "{repo_dir}/commands/download".format(repo_dir=commands_fixtures_path)),
                ("parser", "{repo_dir}/commands/parser".format(repo_dir=commands_fixtures_path)),
            ]
        )

    def test_discover_commands_ignore_dirs_without_manifests(self, commands_fixtures_path, tmpdir):
        repo_dir = str(tmpdir.mkdir("commands"))
        copy_tree(commands_fixtures_path, repo_dir)

        os.mkdir(os.path.join(repo_dir, "not-a-command"))
        with open(os.path.join(repo_dir, "not-a-command", "manifest"), "w") as fd:
            fd.write("not really a manifest")

        commands = discover_commands(repo_dir)

        assert sorted(commands.keys()) == sorted(["splitter", "download", "parser"])

    def test_discover_commands_ignore_test_dirs(self, commands_fixtures_path, simple_command_manifest_yml, tmpdir):
        repo_dir = tmpdir.mkdir("manifests")
        tests_dir = repo_dir.mkdir("commands").mkdir("parser").mkdir("tests")
        nested_tests_dir = tests_dir.mkdir("nested").mkdir("deeply")

        copy_tree(commands_fixtures_path, str(repo_dir))

        for path in [tests_dir, nested_tests_dir]:
            with open(os.path.join(str(path), "manifest.yml"), "w") as fd:
                fd.write(simple_command_manifest_yml)

        commands = discover_commands(repo_dir)

        def strip_tmpdir(path):
            return str(path).replace(str(tmpdir), "")

        discovered_paths = [strip_tmpdir(t.path) for t in commands.values()]

        assert strip_tmpdir(tests_dir) not in discovered_paths, 'the "tests" directory was not skipped'
        assert strip_tmpdir(nested_tests_dir) not in discovered_paths, 'the nested "tests" directory was not skipped'
        assert len(discovered_paths) == 3, "there should be 3 discovered commands"

    def test_discover_commands_ignore_invalid_yaml_manifest(self, commands_fixtures_path, tmpdir, caplog):
        repo_dir = str(tmpdir.mkdir("commands"))
        copy_tree(commands_fixtures_path, repo_dir)

        manifest_path = os.path.join(repo_dir, "invalid-yaml-command", "manifest.yml")
        os.mkdir(os.path.dirname(manifest_path))
        with open(manifest_path, "w") as fd:
            fd.write("not really a manifest")

        commands = discover_commands(repo_dir)

        assert (
            f"Skipping command due to error: Could not load YAML file at path: {manifest_path}; Failed to parse YAML, expected a dictionary"
            in caplog.text
        )
        assert sorted(commands.keys()) == sorted(["splitter", "download", "parser"])

    @pytest.mark.parametrize("required_key", ["name", "run-command"])
    def test_discover_commands_ignore_missing_required_manifest_field(
        self, required_key, commands_fixtures_path, tmpdir, caplog
    ):
        repo_dir = str(tmpdir.mkdir("commands"))
        copy_tree(commands_fixtures_path, repo_dir)

        # comment out the parameterized required key
        yaml = re.sub(
            r"^([ \t]*{}\:)".format(required_key),
            r"# \1",
            dedent(
                """
                name: invalid-manifest-command
                type: command
                run-command: python run.py
                """
            ),
            flags=re.MULTILINE,
        )
        os.mkdir(os.path.join(repo_dir, "invalid-command"))
        with open(os.path.join(repo_dir, "invalid-command", "manifest.yml"), "w") as fd:
            fd.write(yaml)

        commands = discover_commands(repo_dir)

        assert (
            f"Skipping command due to error: Could not load YAML file at path: {repo_dir}/invalid-command/manifest.yml; 2 validation errors for Command"
            in caplog.text
        )
        assert sorted(commands.keys()) == ["download", "parser", "splitter"]

    def test_discover_commands_list_of_paths(self, commands_fixtures_path, tmpdir):
        repo_dir1 = str(tmpdir.mkdir("commands1"))
        repo_dir2 = str(tmpdir.mkdir("commands2"))
        copy_tree(commands_fixtures_path + "/commands/download", repo_dir1)
        copy_tree(commands_fixtures_path + "/commands/parser", repo_dir2)

        commands = discover_commands([repo_dir1, repo_dir2])

        assert sorted(commands.keys()) == sorted(
            ["download", "parser"]
        ), "Discovery should have found 1 command per repo path"


class TestDeserialization:
    def test_load_command_from_file(self, simple_command_manifest_path):
        command = Command.from_file(simple_command_manifest_path)

        assert command.name == "simple-command"
        assert command.path == os.path.dirname(simple_command_manifest_path)
        assert command.env_type == EnvType.PYTHON
        assert command.env == {
            "FOO": InputDetails(description="something", type=str),
            "OPTION_WITH_HYPHENS": InputDetails(description="something else"),
            "OUTPUT": InputDetails(description="the result of the command", type=str),
        }, "The env variable names should have been commanded to uppercase and hyphens replaced with underscores"
        assert command.run_command == "python run.py"
        assert command.test_command == "py.test"

    @pytest.mark.parametrize("env_type", [1, "not-a-valid-env-type"])
    def test_command_env_type_invalid(self, env_type):
        manifest = dedent(
            f"""
            name: simple-command
            type: command
            env_type: {env_type}
            path: /tmp
            run-command: python run.py
            """
        )
        with pytest.raises(ValidationError) as exc:
            Command(**yaml.load(manifest, yaml.FullLoader))
        assert "Input should be 'python' or 'bash'" in str(exc.value)

    def test_command_env_all_defaults(self):
        manifest = dedent(
            """
            name: simple-command
            type: command
            env_type: python
            path: /tmp
            env:
              - FOO
              - BAR
            run-command: python run.py
            """
        )
        command = Command(**yaml.load(manifest, yaml.FullLoader))

        assert command.env == {
            "FOO": InputDetails(description="N/A", required=True, default=None),
            "BAR": InputDetails(description="N/A", required=True, default=None),
        }, "The env variable names should have been commanded to InputDetails with defaults"

    def test_command_env_just_descriptions(self):
        manifest = dedent(
            """
            name: simple-command
            type: command
            env_type: python
            path: /tmp
            env:
              FOO: foo description
              BAR: bar description
              NOT-A-STRING: 1
            run-command: python run.py
            """
        )
        command = Command(**yaml.load(manifest, yaml.FullLoader))

        assert command.env == {
            "FOO": InputDetails(description="foo description"),
            "BAR": InputDetails(description="bar description"),
            "NOT_A_STRING": InputDetails(description="1"),
        }, "The env variable names should have been commanded to InputDetails with defaults"

    def test_command_env_list_of_keys(self):
        manifest = dedent(
            """
            name: simple-command
            type: command
            env_type: python
            path: /tmp
            env:
              - FOO
              - BAR
            run-command: python run.py
            """
        )
        command = Command(**yaml.load(manifest, yaml.FullLoader))

        assert command.env == {
            "FOO": InputDetails(description="N/A"),
            "BAR": InputDetails(description="N/A"),
        }, "The env variable names should have been commanded to InputDetails with defaults"

    def test_command_env_invalid(self):
        manifest = dedent(
            """
            name: simple-command
            type: command
            env_type: python
            path: /tmp
            env:
              - 1
              - GOOD
              - 2.2
              - 3-fine
            run-command: python run.py
            """
        )
        with pytest.raises(ValidationError) as exc:
            Command(**yaml.load(manifest, yaml.FullLoader))
        assert "Command env names must be strings, the following are invalid: 1, 2.2" in str(exc.value)

    def test_command_env_all_explicit(self):
        manifest = dedent(
            """
            name: simple-command
            type: command
            env_type: python
            path: /tmp
            env:
              FOO:
                description: foo description
                required: false
                default: booya
                type: string

              BAR:
                description: bar description
                required: true
                type: boolean
            run-command: python run.py
            """
        )
        command = Command(**yaml.load(manifest, yaml.FullLoader))

        assert command.env == {
            "FOO": InputDetails(description="foo description", required=False, default="booya", type=str),
            "BAR": InputDetails(description="bar description", required=True, default=None, type=bool),
        }, "The env variable names should have been commanded to InputDetails with defaults"

    def test_command_env_optional(self):
        manifest = dedent(
            """
            name: simple-command
            type: command
            env_type: python
            path: /tmp
            env:
              FOO:
                description: foo description
                optional: true
              BAR:
                description: bar description
                optional: false
            run-command: python run.py
            """
        )
        command = Command(**yaml.load(manifest, yaml.FullLoader))

        assert command.env == {
            "FOO": InputDetails(description="foo description", required=False),
            "BAR": InputDetails(description="bar description", required=True),
        }, "The env variable names should have been commanded to InputDetails with defaults"

    def test_command_env_specify_both_optional_and_required(self):
        manifest = dedent(
            """
            name: simple-command
            type: command
            env_type: python
            path: /tmp
            env:
              FOO:
                description: foo description
                optional: true
                required: true
            run-command: python run.py
            """
        )
        with pytest.raises(ValidationError) as exc:
            Command(**yaml.load(manifest, yaml.FullLoader))
        assert "Cannot specify both `required` and `optional`" in str(exc.value)


class TestExecuteCommand:
    @pytest.fixture(autouse=True)
    def mock_popen(self):
        with mock.patch("xetl.models.command.subprocess.Popen") as mock_popen:
            mock_popen.return_value.poll.return_value = 0
            mock_popen.return_value.returncode = 0
            mock_popen.return_value.stdout.readline.side_effect = [
                "Now executing command.",
                "Still executing.",
                "All done.",
                "",
            ]
            mock_popen.return_value.kill.return_value = None
            yield mock_popen

    @pytest.fixture
    def mock_logger(self):
        with mock.patch("xetl.models.command.logger") as mock_logger:
            yield mock_logger

    def test_execute_command(self, simple_command_manifest_path, mock_logger, mock_popen):
        command = Command.from_file(simple_command_manifest_path)
        task = Task(
            command="simple-command",
            env={
                "FOO": "bar",
                "OPTION_WITH_HYPHENS": "baz",
                "OUTPUT": "/tmp/data",
            },
        )

        command.execute(task, dryrun=False)

        assert mock_logger.method_calls == [
            call.info(f"Loading command at: {simple_command_manifest_path}"),
            call.info("Now executing command."),
            call.info("Still executing."),
            call.info("All done."),
        ]

        assert mock_popen.call_args[1]["cwd"] == os.path.dirname(
            simple_command_manifest_path
        ), "The cwd should have been set to the directory where the command manifest is stored"

    def test_execute_command_dryrun(self, simple_command_manifest_path, mock_logger):
        command = Command.from_file(simple_command_manifest_path)
        task = Task(
            command="simple-command",
            env={
                "FOO": "bar",
                "OPTION_WITH_HYPHENS": "baz",
                "OUTPUT": "/tmp/data",
            },
        )

        command.execute(task, dryrun=True)
        assert mock_logger.method_calls == [
            call.info(f"Loading command at: {simple_command_manifest_path}"),
            call.info("DRYRUN: Would execute with:"),
            call.info("  command: ['python', 'run.py']"),
            call.info(f"  cwd: {os.path.dirname(simple_command_manifest_path)}"),
            call.info("  env: FOO=bar, OPTION_WITH_HYPHENS=baz, OUTPUT=/tmp/data"),
        ]

    @pytest.mark.parametrize(
        "var_type, var_value",
        [
            ("str", "test"),
            ("int", 1),
            ("float", 1.23),
            ("bool", True),
        ],
    )
    def test_execute_command_valid_env_value_and_type(self, var_type, var_value):
        command = Command.from_yaml(
            command_with_env(
                f"""
                env:
                  INPUT:
                    type: {var_type}
                """
            ),
            path="/tmp",
        )
        task = Task(
            command="simple-command",
            env={
                "INPUT": var_value,
            },
        )
        command.execute(task, dryrun=True)

    @pytest.mark.parametrize(
        "var_type, var_value, message",
        [
            ("str", 1, "expected `str`, received `int`"),
            ("str", False, "expected `str`, received `bool`"),
            ("int", "one", "expected `int`, received `str`"),
            ("int", "one", "expected `int`, received `str`"),
            ("float", 1, "expected `float`, received `int`"),
            ("float", "one", "expected `float`, received `str`"),
            ("bool", 1, "expected `bool`, received `int`"),
        ],
    )
    def test_execute_command_invalid_env_value_types(self, var_type, var_value, message):
        command = Command.from_yaml(
            command_with_env(
                f"""
                env:
                  INPUT:
                    type: {var_type}
                """
            ),
            path="/tmp",
        )
        task = Task(
            command="simple-command",
            env={
                "INPUT": var_value,
            },
        )
        with pytest.raises(ValueError) as exc:
            command.execute(task, dryrun=True)
        assert str(exc.value) == (f"Invalid env values for command `simple-command`:\n - INPUT: {message}")

    @pytest.mark.parametrize("value", [1, 1.23, True, "string"])
    def test_execute_command_defaults_to_any_type(self, value, caplog):
        caplog.set_level("INFO")
        command = Command.from_yaml(
            command_with_env(
                f"""
                env:
                  INPUT: description, default has no type validation
                """
            ),
            path="/tmp",
        )
        task = Task(
            command="simple-command",
            env={
                "input": value,
            },
        )
        command.execute(task, dryrun=True)
        assert f"env: INPUT={str(value)}" in "\n".join(caplog.messages)

    def test_execute_command_unknown_env_variable(self, caplog):
        caplog.set_level("INFO")
        command = Command.from_yaml(
            command_with_env(
                f"""
                env:
                  INPUT1: description, default has no type validation
                  INPUT2: description, default has no type validation
                """
            ),
            path="/tmp",
        )
        task = Task(
            command="simple-command",
            env={
                "INPUT1": "value",
                "INPUT2": "value",
                "UNKNOWN1": "value",
                "UNKNOWN2": "value",
            },
        )
        command.execute(task, dryrun=True)

        assert (
            "Ignoring unknown env variables for command `simple-command`: UNKNOWN1, UNKNOWN2. Valid names are: INPUT1, INPUT2"
            in caplog.messages
        ), "\n".join(caplog.messages)

    def test_execute_command_valid_missing_required_fields(self):
        command = Command.from_yaml(
            command_with_env(
                f"""
                env:
                  REQUIRED_INPUT:
                    description: This field is required
                    required: true
                  NON_OPTIONAL_INPUT:
                    description: This field uses optional instead of required
                    optional: false
                  OPTIONAL:
                    description: This field is optional
                    optional: true
                """
            ),
            path="/tmp",
        )
        task = Task(
            command="simple-command",
        )
        with pytest.raises(ValueError) as exc:
            command.execute(task, dryrun=True)
        assert str(exc.value) == (
            "Missing required inputs for command `simple-command`: REQUIRED_INPUT, NON_OPTIONAL_INPUT"
        )

    def test_execute_command_with_bash_command(self, bash_command_command_manifest_path, mock_popen):
        command = Command.from_file(bash_command_command_manifest_path)
        task = Task(command="complex-command-command")

        command.execute(task, dryrun=False)

        popen_args = mock_popen.call_args[0][0]
        assert popen_args == ["/bin/bash", "-c", "ls -l ~/"], "Command and argument whould be split"
        assert mock_popen.call_args[1]["cwd"] == os.path.dirname(bash_command_command_manifest_path)

    def test_execute_command_with_complex_command(self, complex_command_command_manifest_path, mock_popen):
        command = Command.from_file(complex_command_command_manifest_path)
        task = Task(command="complex-command-command")

        command.execute(task, dryrun=False)

        popen_args = mock_popen.call_args[0][0]
        assert popen_args == [
            "/bin/bash",
            "-c",
            "echo \"hello world\" | awk '{print $2}'",
        ], "Words in strings should be kept intact and not split"
        assert mock_popen.call_args[1]["cwd"] == os.path.dirname(complex_command_command_manifest_path)
