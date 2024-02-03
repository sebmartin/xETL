import os
import re
from shutil import copytree
from textwrap import dedent

import mock
import pytest
import yaml
from mock import call
from pydantic import ValidationError
from xetl.models import EnvVariableType

from xetl.models.task import TaskInputDetails, Task, discover_tasks
from xetl.models.task_test_case import TaskTestCase


def copy_tree(src, dst):
    copytree(src, dst, dirs_exist_ok=True)


def task_file(task_yaml: str, tmpdir):
    path = os.path.join(tmpdir, "manifest.yml")
    with open(path, "w") as fd:
        fd.write(task_yaml)
    return path


def task_with_env(env: str) -> str:
    return dedent(
        """
        name: simple-task
        {env}
        run: python run.py
        """
    ).format(env=dedent(env))


@pytest.fixture
def simple_task_manifest_yml():
    return dedent(
        """
        name: simple-task
        tasks: /something
        env:
          FOO:
            description: something
            type: string
            required: true
          OPTION_WITH_HYPHENS:
            description: something else
          OUTPUT:
            description: the result of the task
            type: string
            required: true
        run: python run.py
        tests:
          simple-test:
            env:
              FOO: bar
              OUTPUT: /tmp/data
            verify: verify.py
        """
    )


@pytest.fixture
def simple_task_manifest_path(simple_task_manifest_yml, tmpdir):
    return task_file(simple_task_manifest_yml, tmpdir)


@pytest.fixture
def bash_task_task_manifest_yml():
    return dedent(
        """
        name: bash-task-task
        run: ls -l ~/
        test-command: py.test
        """
    )


@pytest.fixture
def bash_task_task_manifest_path(bash_task_task_manifest_yml, tmpdir):
    return task_file(bash_task_task_manifest_yml, tmpdir)


class TestDiscoverTasks:
    def test_discover_tasks(self, tasks_fixtures_path):
        tasks = discover_tasks(tasks_fixtures_path)

        names_and_paths = [(name, task.path) for name, task in tasks.items()]

        assert sorted(names_and_paths) == sorted(
            [
                ("splitter", "{repo_dir}/tasks/splitter".format(repo_dir=tasks_fixtures_path)),
                ("download", "{repo_dir}/tasks/download".format(repo_dir=tasks_fixtures_path)),
                ("parser", "{repo_dir}/tasks/parser".format(repo_dir=tasks_fixtures_path)),
            ]
        )

    def test_discover_tasks_ignore_dirs_without_manifests(self, tasks_fixtures_path, tmpdir):
        repo_dir = str(tmpdir.mkdir("tasks"))
        copy_tree(tasks_fixtures_path, repo_dir)

        os.mkdir(os.path.join(repo_dir, "not-a-task"))
        with open(os.path.join(repo_dir, "not-a-task", "manifest"), "w") as fd:
            fd.write("not really a manifest")

        tasks = discover_tasks(repo_dir)

        assert sorted(tasks.keys()) == sorted(["splitter", "download", "parser"])

    def test_discover_tasks_ignore_test_dirs(self, tasks_fixtures_path, simple_task_manifest_yml, tmpdir):
        repo_dir = tmpdir.mkdir("manifests")
        tests_dir = repo_dir.mkdir("tasks").mkdir("parser").mkdir("tests")
        nested_tests_dir = tests_dir.mkdir("nested").mkdir("deeply")

        copy_tree(tasks_fixtures_path, str(repo_dir))

        for path in [tests_dir, nested_tests_dir]:
            with open(os.path.join(str(path), "manifest.yml"), "w") as fd:
                fd.write(simple_task_manifest_yml)

        tasks = discover_tasks(repo_dir)

        def strip_tmpdir(path):
            return str(path).replace(str(tmpdir), "")

        discovered_paths = [strip_tmpdir(t.path) for t in tasks.values()]

        assert strip_tmpdir(tests_dir) not in discovered_paths, 'the "tests" directory was not skipped'
        assert strip_tmpdir(nested_tests_dir) not in discovered_paths, 'the nested "tests" directory was not skipped'
        assert len(discovered_paths) == 3, "there should be 3 discovered tasks"

    def test_discover_tasks_ignore_invalid_yaml_manifest(self, tasks_fixtures_path, tmpdir, caplog):
        repo_dir = str(tmpdir.mkdir("tasks"))
        copy_tree(tasks_fixtures_path, repo_dir)

        manifest_path = os.path.join(repo_dir, "invalid-yaml-task", "manifest.yml")
        os.mkdir(os.path.dirname(manifest_path))
        with open(manifest_path, "w") as fd:
            fd.write("not really a manifest")

        tasks = discover_tasks(repo_dir)

        assert (
            f"Skipping task at `{repo_dir}/invalid-yaml-task` due to error: Could not load YAML file at path: {manifest_path}; Failed to parse YAML, expected a dictionary"
            in caplog.text
        )
        assert sorted(tasks.keys()) == sorted(["splitter", "download", "parser"])

    def test_discover_tasks_ignore_unknown_errors(self, tasks_fixtures_path, tmpdir, caplog):
        repo_dir = str(tmpdir.mkdir("tasks"))
        copy_tree(tasks_fixtures_path, repo_dir)
        tasks = discover_tasks(repo_dir)

        with mock.patch(
            "xetl.models.task.Task.from_file",
            mock.Mock(side_effect=[Exception("Unknown exception :(~~")] + list(tasks.values())[1:]),
        ):
            tasks = discover_tasks(repo_dir)

        assert (
            f"Skipping task at `{repo_dir}/tasks/splitter` due to unexpected error: Unknown exception :(~~"
            in caplog.text
        )
        assert sorted(tasks.keys()) == sorted(["download", "parser"])

    @pytest.mark.parametrize("required_key", ["name", "run"])
    def test_discover_tasks_ignore_missing_required_manifest_field(
        self, required_key, tasks_fixtures_path, tmpdir, caplog
    ):
        repo_dir = str(tmpdir.mkdir("tasks"))
        copy_tree(tasks_fixtures_path, repo_dir)

        # comment out the parameterized required key
        yaml = re.sub(
            r"^([ \t]*{}\:)".format(required_key),
            r"# \1",
            dedent(
                """
                name: invalid-manifest-task
                run: python run.py
                """
            ),
            flags=re.MULTILINE,
        )
        os.mkdir(os.path.join(repo_dir, "invalid-task"))
        with open(os.path.join(repo_dir, "invalid-task", "manifest.yml"), "w") as fd:
            fd.write(yaml)

        tasks = discover_tasks(repo_dir)

        assert (
            f"Skipping task at `{repo_dir}/invalid-task` due to error: Could not load YAML file at path: {repo_dir}/invalid-task/manifest.yml; 1 validation error for Task"
            in caplog.text
        )
        assert sorted(tasks.keys()) == ["download", "parser", "splitter"]

    def test_discover_tasks_list_of_paths(self, tasks_fixtures_path, tmpdir):
        repo_dir1 = str(tmpdir.mkdir("tasks1"))
        repo_dir2 = str(tmpdir.mkdir("tasks2"))
        copy_tree(tasks_fixtures_path + "/tasks/download", repo_dir1)
        copy_tree(tasks_fixtures_path + "/tasks/parser", repo_dir2)

        tasks = discover_tasks([repo_dir1, repo_dir2])

        assert sorted(tasks.keys()) == sorted(
            ["download", "parser"]
        ), "Discovery should have found 1 task per repo path"


class TestDeserialization:
    def test_load_task_from_file(self, simple_task_manifest_path):
        task = Task.from_file(simple_task_manifest_path)

        assert task.name == "simple-task"
        assert task.path == os.path.dirname(simple_task_manifest_path)
        assert task.env == {
            "FOO": TaskInputDetails(description="something", type=str),
            "OPTION_WITH_HYPHENS": TaskInputDetails(description="something else"),
            "OUTPUT": TaskInputDetails(description="the result of the task", type=str),
        }, "The env variable names should have been parsed to uppercase and hyphens replaced with underscores"
        assert task.run == ["python", "run.py"]
        assert task.tests == {
            "simple-test": TaskTestCase(env={"FOO": "bar", "OUTPUT": "/tmp/data"}, verify=["verify.py"])
        }

    def test_task_env_optional_with_default_value(self):
        manifest = dedent(
            """
            name: simple-task
            path: /tmp
            env:
              VAR:
                optional: true
                default: booya
            run: python run.py
            """
        )
        task = Task(**yaml.load(manifest, yaml.FullLoader))

        assert task.env == {
            "VAR": TaskInputDetails(description=None, required=False, default="booya"),
        }, "The env variable names should have been parsed to InputDetails with defaults"

    def test_task_env_required_with_default_value_raises(self):
        manifest = dedent(
            """
            name: simple-task
            path: /tmp
            env:
              VAR1:
                required: true
                default: default1
              VAR2:
                optional: false
                default: default2
            run: python run.py
            """
        )
        with pytest.raises(ValidationError) as exc:
            Task(**yaml.load(manifest, yaml.FullLoader))
        assert (
            "The following task env variables are required but specify a default value which is invalid: VAR1, VAR2"
            in str(exc.value)
        )

    def test_task_env_default_implicitly_optional(self):
        manifest = dedent(
            """
            name: simple-task
            path: /tmp
            env:
              VAR1:
                default: booya
            run: python run.py
            """
        )
        task = Task(**yaml.load(manifest, yaml.FullLoader))
        assert task.env["VAR1"].required is False, "Should automatically be optional if a default is specified"

    def test_task_env_all_defaults(self):
        manifest = dedent(
            """
            name: simple-task
            path: /tmp
            env:
              - FOO
              - BAR
            run: python run.py
            """
        )
        task = Task(**yaml.load(manifest, yaml.FullLoader))

        assert task.env == {
            "FOO": TaskInputDetails(description=None, required=True, default=None),
            "BAR": TaskInputDetails(description=None, required=True, default=None),
        }, "The env variable names should have been parsed to InputDetails with defaults"

    def test_task_env_just_descriptions(self):
        manifest = dedent(
            """
            name: simple-task
            path: /tmp
            env:
              FOO: foo description
              BAR: bar description
              NOT-A-STRING: 1
            run: python run.py
            """
        )
        task = Task(**yaml.load(manifest, yaml.FullLoader))

        assert task.env == {
            "FOO": TaskInputDetails(description="foo description"),
            "BAR": TaskInputDetails(description="bar description"),
            "NOT_A_STRING": TaskInputDetails(description="1"),
        }, "The env variable names should have been parsed to InputDetails with defaults"

    def test_task_env_list_of_keys(self):
        manifest = dedent(
            """
            name: simple-task
            path: /tmp
            env:
              - FOO
              - BAR
            run: python run.py
            """
        )
        task = Task(**yaml.load(manifest, yaml.FullLoader))

        assert task.env == {
            "FOO": TaskInputDetails(description=None),
            "BAR": TaskInputDetails(description=None),
        }, "The env variable names should have been parsed to InputDetails with defaults"

    def test_task_env_invalid(self):
        manifest = dedent(
            """
            name: simple-task
            path: /tmp
            env:
              - 1
              - GOOD
              - 2.2
              - 3-fine
            run: python run.py
            """
        )
        with pytest.raises(ValidationError) as exc:
            Task(**yaml.load(manifest, yaml.FullLoader))
        assert "Task env names must be strings, the following are invalid: 1, 2.2" in str(exc.value)

    def test_task_env_all_explicit(self):
        manifest = dedent(
            """
            name: simple-task
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
            run: python run.py
            """
        )
        task = Task(**yaml.load(manifest, yaml.FullLoader))

        assert task.env == {
            "FOO": TaskInputDetails(description="foo description", required=False, default="booya", type=str),
            "BAR": TaskInputDetails(description="bar description", required=True, default=None, type=bool),
        }, "The env variable names should have been parsed to InputDetails with defaults"

    def test_task_env_optional(self):
        manifest = dedent(
            """
            name: simple-task
            path: /tmp
            env:
              FOO:
                description: foo description
                optional: true
              BAR:
                description: bar description
                optional: false
            run: python run.py
            """
        )
        task = Task(**yaml.load(manifest, yaml.FullLoader))

        assert task.env == {
            "FOO": TaskInputDetails(description="foo description", required=False),
            "BAR": TaskInputDetails(description="bar description", required=True),
        }, "The env variable names should have been parsed to InputDetails with defaults"

    def test_task_env_specify_both_optional_and_required(self):
        manifest = dedent(
            """
            name: simple-task
            path: /tmp
            env:
              FOO:
                description: foo description
                optional: true
                required: true
            run: python run.py
            """
        )
        with pytest.raises(ValidationError) as exc:
            Task(**yaml.load(manifest, yaml.FullLoader))
        assert "Cannot specify both `required` and `optional`" in str(exc.value)

    def test_task_run_string(self):
        manifest = dedent(
            """
            name: simple-task
            path: /tmp
            run: ./run.sh --foo bar
            """
        )
        task = Task(**yaml.load(manifest, yaml.FullLoader))
        assert task.run == ["./run.sh", "--foo", "bar"]

    @mock.patch("xetl.models.utils.run.sys.executable", "/home/user/.venv/python")
    def test_task_run_script_default_interpreter(self):
        manifest = dedent(
            """
            name: simple-task
            path: /tmp
            run:
              script: print("hello world")
            """
        )
        task = Task(**yaml.load(manifest, yaml.FullLoader))
        assert task.run == ["/home/user/.venv/python", "-c", 'print("hello world")']

    def test_task_run_script_and_interpreter(self):
        manifest = dedent(
            """
            name: simple-task
            path: /tmp
            run:
              interpreter: /bin/zsh -c
              script: echo "hello world" | awk '{print $2}'
            """
        )
        task = Task(**yaml.load(manifest, yaml.FullLoader))
        assert task.run == ["/bin/zsh", "-c", "echo \"hello world\" | awk '{print $2}'"]

    def test_task_run_script_multiline(self):
        manifest = dedent(
            """
            name: simple-task
            path: /tmp
            run:
              interpreter: /bin/bash -c
              script: |
                if [ -f /tmp/foo.txt ]; then
                    cat /tmp/foo.txt
                fi
            """
        )
        task = Task(**yaml.load(manifest, yaml.FullLoader))
        assert task.run == ["/bin/bash", "-c", "if [ -f /tmp/foo.txt ]; then\n    cat /tmp/foo.txt\nfi\n"]

    def test_task_run_string_and_script_defaults_to_run(self):
        manifest = dedent(
            """
            name: simple-task
            path: /tmp
            run: ./run.sh --foo bar
            script: print("hello world")
            """
        )
        task = Task(**yaml.load(manifest, yaml.FullLoader))
        assert task.run == ["./run.sh", "--foo", "bar"]

    def test_task_run_list(self):
        manifest = dedent(
            """
            name: simple-task
            path: /tmp
            run:
             - ./run.sh
             - --foo
             - bar
            """
        )
        task = Task(**yaml.load(manifest, yaml.FullLoader))
        assert task.run == ["./run.sh", "--foo", "bar"]

    def test_task_run_invalid_object(self):
        manifest = dedent(
            """
            name: simple-task
            path: /tmp
            run:
              foo: bar
            """
        )
        with pytest.raises(ValidationError) as exc:
            Task(**yaml.load(manifest, yaml.FullLoader))
        assert (
            "Task run command must be a string, a list of strings, or a script object, received: {'foo': 'bar'}"
            in str(exc.value)
        )

    def test_task_tests_command(self):
        manifest = dedent(
            """
            name: simple-task
            path: /tmp
            run: ./run.sh
            tests:
              my-test:
                env:
                  FOO: bar
                verify: verify.py
            """
        )
        task = Task(**yaml.load(manifest, yaml.FullLoader))
        assert task.tests == {"my-test": TaskTestCase(env={"FOO": "bar"}, verify=["verify.py"])}

    def test_task_tests_script(self):
        manifest = dedent(
            """
            name: simple-task
            path: /tmp
            run: ./run.sh
            tests:
              my-test:
                env:
                  FOO: bar
                verify:
                  interpreter: /bin/bash -c
                  script: |
                    if [ -f /tmp/foo.txt ]; then
                        cat /tmp/foo.txt
                    fi
            """
        )
        task = Task(**yaml.load(manifest, yaml.FullLoader))
        assert task.tests == {
            "my-test": TaskTestCase(
                env={"FOO": "bar"},
                verify=["/bin/bash", "-c", "if [ -f /tmp/foo.txt ]; then\n    cat /tmp/foo.txt\nfi\n"],
            )
        }

    def test_task_tests_invalid(self):
        manifest = dedent(
            """
            name: simple-task
            path: /tmp
            run: ./run.sh
            tests:
              my-test:
                env:
                  FOO: bar
                verify:
                  foo: bar
            """
        )
        with pytest.raises(ValidationError) as exc:
            Task(**yaml.load(manifest, yaml.FullLoader))
        assert (
            "Task test verify command must be a string, a list of strings, or a script object, received: {'foo': 'bar'}"
            in str(exc.value)
        )


class TestExecuteTask:
    @pytest.fixture(autouse=True)
    def mock_popen(self):
        with mock.patch("xetl.models.task.subprocess.Popen") as mock_popen:
            mock_popen.return_value.poll.return_value = 0
            mock_popen.return_value.returncode = 0
            mock_popen.return_value.stdout.readline.side_effect = [
                "Now executing task.",
                "Still executing.",
                "All done.",
                "",
            ]
            mock_popen.return_value.kill.return_value = None
            yield mock_popen

    @pytest.fixture
    def mock_logger(self):
        with mock.patch("xetl.models.task.logger") as mock_logger:
            yield mock_logger

    def test_execute_task(self, simple_task_manifest_path, mock_logger, mock_popen):
        task = Task.from_file(simple_task_manifest_path)
        env: dict[str, EnvVariableType] = {
            "FOO": "bar",
            "OPTION_WITH_HYPHENS": "baz",
            "OUTPUT": "/tmp/data",
        }

        task.execute(env, dryrun=False)

        assert mock_logger.method_calls == [
            call.info(f"Loading task at: {simple_task_manifest_path}"),
            call.info("Now executing task."),
            call.info("Still executing."),
            call.info("All done."),
        ]

        assert mock_popen.call_args[1]["cwd"] == os.path.dirname(
            simple_task_manifest_path
        ), "The cwd should have been set to the directory where the task manifest is stored"

    def test_execute_task_kills_process_on_unexpected_error(self, simple_task_manifest_path, mock_logger, mock_popen):
        task = Task.from_file(simple_task_manifest_path)
        env: dict[str, EnvVariableType] = {
            "FOO": "bar",
            "OPTION_WITH_HYPHENS": "baz",
            "OUTPUT": "/tmp/data",
        }

        mock_popen.return_value.stdout.readline.side_effect = Exception("Something went wrong")
        mock_popen.return_value.poll.return_value = None

        with pytest.raises(Exception) as exc:
            task.execute(env, dryrun=False)

        assert str(exc.value) == "Something went wrong"
        mock_popen.return_value.kill.assert_called_once()

    def test_execute_task_dryrun(self, simple_task_manifest_path, mock_logger):
        task = Task.from_file(simple_task_manifest_path)
        env: dict[str, EnvVariableType] = {
            "FOO": "bar",
            "OPTION_WITH_HYPHENS": "baz",
            "OUTPUT": "/tmp/data",
        }

        task.execute(env, dryrun=True)
        assert mock_logger.method_calls == [
            call.info(f"Loading task at: {simple_task_manifest_path}"),
            call.info("DRYRUN: Would execute with:"),
            call.info("  run: python run.py"),
            call.info(f"  cwd: {os.path.dirname(simple_task_manifest_path)}"),
            call.info("  env: FOO=bar, OPTION_WITH_HYPHENS=baz, OUTPUT=/tmp/data"),
        ]

    def test_execute_task_with_default_env_values(self, simple_task_manifest_path, mock_logger):
        task = Task.from_yaml(
            task_with_env(
                """
                env:
                  INPUT:
                    optional: true
                    default: default-value
                """
            ),
            path="/tmp",
        )
        task.execute({}, dryrun=True)
        assert mock_logger.method_calls == [
            call.info("DRYRUN: Would execute with:"),
            call.info("  run: python run.py"),
            call.info("  cwd: /tmp"),
            call.info("  env: INPUT=default-value"),
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
    def test_execute_task_valid_env_value_and_type(self, var_type, var_value):
        task = Task.from_yaml(
            task_with_env(
                f"""
                env:
                  INPUT:
                    type: {var_type}
                """
            ),
            path="/tmp",
        )
        env = {
            "INPUT": var_value,
        }
        task.execute(env, dryrun=True)

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
    def test_execute_task_invalid_env_value_types(self, var_type, var_value, message):
        task = Task.from_yaml(
            task_with_env(
                f"""
                env:
                  INPUT:
                    type: {var_type}
                """
            ),
            path="/tmp",
        )
        env = {
            "INPUT": var_value,
        }
        with pytest.raises(ValueError) as exc:
            task.execute(env, dryrun=True)
        assert str(exc.value) == (f"Invalid env values for task `simple-task`:\n - INPUT: {message}")

    @pytest.mark.parametrize("value", [1, 1.23, True, "string"])
    def test_execute_task_defaults_to_any_type(self, value, caplog):
        task = Task.from_yaml(
            task_with_env(
                """
                env:
                  INPUT: description, default has no type validation
                """
            ),
            path="/tmp",
        )
        env = {
            "INPUT": value,
        }
        task.execute(env, dryrun=True)
        assert f"env: INPUT={str(value)}" in "\n".join(caplog.messages)

    @pytest.mark.parametrize("key", ["some-input", "SOME_INPUT", "Some-Input"])
    def test_execute_normalizes_env_keys(self, key, caplog):
        task = Task.from_yaml(
            task_with_env(
                """
                env:
                  SOME-INPUT: description, default has no type validation
                """
            ),
            path="/tmp",
        )
        env: dict[str, EnvVariableType] = {
            key: "value",
        }
        task.execute(env, dryrun=True)
        assert "env: SOME_INPUT=value" in "\n".join(caplog.messages)

    def test_execute_task_unknown_env_variable(self, caplog):
        task = Task.from_yaml(
            task_with_env(
                """
                env:
                  INPUT1: description, default has no type validation
                  INPUT2: description, default has no type validation
                """
            ),
            path="/tmp",
        )
        env: dict[str, EnvVariableType] = {
            "INPUT1": "value",
            "INPUT2": "value",
            "UNKNOWN1": "value",
            "UNKNOWN2": "value",
        }
        task.execute(env, dryrun=True)

        assert (
            "Ignoring unexpected env variables for task `simple-task`: UNKNOWN1, UNKNOWN2. Valid names are: INPUT1, INPUT2"
            in caplog.messages
        ), "\n".join(caplog.messages)

    def test_execute_task_valid_missing_required_fields(self):
        task = Task.from_yaml(
            task_with_env(
                """
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
        with pytest.raises(ValueError) as exc:
            task.execute({}, dryrun=True)
        assert str(exc.value) == ("Missing required inputs for task `simple-task`: REQUIRED_INPUT, NON_OPTIONAL_INPUT")

    def test_execute_task_with_bash_task(self, bash_task_task_manifest_path, mock_popen):
        task = Task.from_file(bash_task_task_manifest_path)

        task.execute({}, dryrun=False)

        popen_args = mock_popen.call_args[0][0]
        assert popen_args == ["ls", "-l", "~/"]
        assert mock_popen.call_args[1]["cwd"] == os.path.dirname(bash_task_task_manifest_path)


class TestEndToEnd:
    def test_execute_complex_bash_command(self, tmpdir, caplog):
        task_yaml = dedent(
            """
            name: complex-task-task
            run: /bin/bash -c "echo 'hello world' | awk '{print $2}'"
            """
        )
        manifest = task_file(task_yaml, tmpdir)
        task = Task.from_file(manifest)

        caplog.clear()
        res = task.execute({}, dryrun=False)
        assert caplog.messages == ["world"], "Should have printed the second word of the string"
        assert res == 0

    def test_execute_bash_script(self, tmp_path, caplog):
        bash_script = dedent(
            """
            #!/bin/bash
            echo "hello $NAME" | awk '{print $2}'
            """
        ).strip()
        script_path = tmp_path / "hello.sh"
        script_path.write_text(bash_script, encoding="utf-8")
        script_path.chmod(0o755)

        task_yaml = dedent(
            """
            name: complex-task-task
            env:
              NAME: string
            run: ./hello.sh
            """
        )
        manifest = task_file(task_yaml, tmp_path)
        task = Task.from_file(manifest)

        caplog.clear()
        res = task.execute(env={"NAME": "Steve"}, dryrun=False)
        assert caplog.messages == ["Steve"], "Should have printed the name from the env variable"
        assert res == 0
