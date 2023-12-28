import os
import re
from shutil import copytree
from textwrap import dedent

import mock
import pytest
import yaml
from mock import call
from pydantic import ValidationError

from xetl.models.command import Command
from xetl.models.task import EnvType, InputDetails, Task, discover_tasks


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
        type: task
        env-type: python
        {env}
        run-task: python run.py
        """
    ).format(env=dedent(env))


@pytest.fixture
def simple_task_manifest_yml():
    return dedent(
        """
        name: simple-task
        type: task
        env-type: python
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
        run-task: python run.py
        test-task: py.test
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
        env-type: bash
        run-task: ls -l ~/
        """
    )


@pytest.fixture
def bash_task_task_manifest_path(bash_task_task_manifest_yml, tmpdir):
    return task_file(bash_task_task_manifest_yml, tmpdir)


@pytest.fixture
def complex_task_task_manifest_yml():
    return dedent(
        """
        name: complex-task-task
        type: task
        env-type: bash
        run-task: echo "hello world" | awk '{print $2}'
        """
    )


@pytest.fixture
def complex_task_task_manifest_path(complex_task_task_manifest_yml, tmpdir):
    return task_file(complex_task_task_manifest_yml, tmpdir)


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
            f"Skipping task due to error: Could not load YAML file at path: {manifest_path}; Failed to parse YAML, expected a dictionary"
            in caplog.text
        )
        assert sorted(tasks.keys()) == sorted(["splitter", "download", "parser"])

    @pytest.mark.parametrize("required_key", ["name", "run-task"])
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
                type: task
                run-task: python run.py
                """
            ),
            flags=re.MULTILINE,
        )
        os.mkdir(os.path.join(repo_dir, "invalid-task"))
        with open(os.path.join(repo_dir, "invalid-task", "manifest.yml"), "w") as fd:
            fd.write(yaml)

        tasks = discover_tasks(repo_dir)

        assert (
            f"Skipping task due to error: Could not load YAML file at path: {repo_dir}/invalid-task/manifest.yml; 2 validation errors for Task"
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
        assert task.env_type == EnvType.PYTHON
        assert task.env == {
            "FOO": InputDetails(description="something", type=str),
            "OPTION_WITH_HYPHENS": InputDetails(description="something else"),
            "OUTPUT": InputDetails(description="the result of the task", type=str),
        }, "The env variable names should have been tasked to uppercase and hyphens replaced with underscores"
        assert task.run_task == "python run.py"
        assert task.test_task == "py.test"

    @pytest.mark.parametrize("env_type", [1, "not-a-valid-env-type"])
    def test_task_env_type_invalid(self, env_type):
        manifest = dedent(
            f"""
            name: simple-task
            type: task
            env_type: {env_type}
            path: /tmp
            run-task: python run.py
            """
        )
        with pytest.raises(ValidationError) as exc:
            Task(**yaml.load(manifest, yaml.FullLoader))
        assert "Input should be 'python' or 'bash'" in str(exc.value)

    def test_task_env_all_defaults(self):
        manifest = dedent(
            """
            name: simple-task
            type: task
            env_type: python
            path: /tmp
            env:
              - FOO
              - BAR
            run-task: python run.py
            """
        )
        task = Task(**yaml.load(manifest, yaml.FullLoader))

        assert task.env == {
            "FOO": InputDetails(description="N/A", required=True, default=None),
            "BAR": InputDetails(description="N/A", required=True, default=None),
        }, "The env variable names should have been tasked to InputDetails with defaults"

    def test_task_env_just_descriptions(self):
        manifest = dedent(
            """
            name: simple-task
            type: task
            env_type: python
            path: /tmp
            env:
              FOO: foo description
              BAR: bar description
              NOT-A-STRING: 1
            run-task: python run.py
            """
        )
        task = Task(**yaml.load(manifest, yaml.FullLoader))

        assert task.env == {
            "FOO": InputDetails(description="foo description"),
            "BAR": InputDetails(description="bar description"),
            "NOT_A_STRING": InputDetails(description="1"),
        }, "The env variable names should have been tasked to InputDetails with defaults"

    def test_task_env_list_of_keys(self):
        manifest = dedent(
            """
            name: simple-task
            type: task
            env_type: python
            path: /tmp
            env:
              - FOO
              - BAR
            run-task: python run.py
            """
        )
        task = Task(**yaml.load(manifest, yaml.FullLoader))

        assert task.env == {
            "FOO": InputDetails(description="N/A"),
            "BAR": InputDetails(description="N/A"),
        }, "The env variable names should have been tasked to InputDetails with defaults"

    def test_task_env_invalid(self):
        manifest = dedent(
            """
            name: simple-task
            type: task
            env_type: python
            path: /tmp
            env:
              - 1
              - GOOD
              - 2.2
              - 3-fine
            run-task: python run.py
            """
        )
        with pytest.raises(ValidationError) as exc:
            Task(**yaml.load(manifest, yaml.FullLoader))
        assert "Task env names must be strings, the following are invalid: 1, 2.2" in str(exc.value)

    def test_task_env_all_explicit(self):
        manifest = dedent(
            """
            name: simple-task
            type: task
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
            run-task: python run.py
            """
        )
        task = Task(**yaml.load(manifest, yaml.FullLoader))

        assert task.env == {
            "FOO": InputDetails(description="foo description", required=False, default="booya", type=str),
            "BAR": InputDetails(description="bar description", required=True, default=None, type=bool),
        }, "The env variable names should have been tasked to InputDetails with defaults"

    def test_task_env_optional(self):
        manifest = dedent(
            """
            name: simple-task
            type: task
            env_type: python
            path: /tmp
            env:
              FOO:
                description: foo description
                optional: true
              BAR:
                description: bar description
                optional: false
            run-task: python run.py
            """
        )
        task = Task(**yaml.load(manifest, yaml.FullLoader))

        assert task.env == {
            "FOO": InputDetails(description="foo description", required=False),
            "BAR": InputDetails(description="bar description", required=True),
        }, "The env variable names should have been tasked to InputDetails with defaults"

    def test_task_env_specify_both_optional_and_required(self):
        manifest = dedent(
            """
            name: simple-task
            type: task
            env_type: python
            path: /tmp
            env:
              FOO:
                description: foo description
                optional: true
                required: true
            run-task: python run.py
            """
        )
        with pytest.raises(ValidationError) as exc:
            Task(**yaml.load(manifest, yaml.FullLoader))
        assert "Cannot specify both `required` and `optional`" in str(exc.value)


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
        command = Command(
            task="simple-task",
            env={
                "FOO": "bar",
                "OPTION_WITH_HYPHENS": "baz",
                "OUTPUT": "/tmp/data",
            },
        )

        task.execute(command, dryrun=False)

        assert mock_logger.method_calls == [
            call.info(f"Loading task at: {simple_task_manifest_path}"),
            call.info("Now executing task."),
            call.info("Still executing."),
            call.info("All done."),
        ]

        assert mock_popen.call_args[1]["cwd"] == os.path.dirname(
            simple_task_manifest_path
        ), "The cwd should have been set to the directory where the task manifest is stored"

    def test_execute_task_dryrun(self, simple_task_manifest_path, mock_logger):
        task = Task.from_file(simple_task_manifest_path)
        command = Command(
            task="simple-task",
            env={
                "FOO": "bar",
                "OPTION_WITH_HYPHENS": "baz",
                "OUTPUT": "/tmp/data",
            },
        )

        task.execute(command, dryrun=True)
        assert mock_logger.method_calls == [
            call.info(f"Loading task at: {simple_task_manifest_path}"),
            call.info("DRYRUN: Would execute with:"),
            call.info("  task: ['python', 'run.py']"),
            call.info(f"  cwd: {os.path.dirname(simple_task_manifest_path)}"),
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
        command = Command(
            task="simple-task",
            env={
                "INPUT": var_value,
            },
        )
        task.execute(command, dryrun=True)

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
        command = Command(
            task="simple-task",
            env={
                "INPUT": var_value,
            },
        )
        with pytest.raises(ValueError) as exc:
            task.execute(command, dryrun=True)
        assert str(exc.value) == (f"Invalid env values for task `simple-task`:\n - INPUT: {message}")

    @pytest.mark.parametrize("value", [1, 1.23, True, "string"])
    def test_execute_task_defaults_to_any_type(self, value, caplog):
        caplog.set_level("INFO")
        task = Task.from_yaml(
            task_with_env(
                f"""
                env:
                  INPUT: description, default has no type validation
                """
            ),
            path="/tmp",
        )
        command = Command(
            task="simple-task",
            env={
                "input": value,
            },
        )
        task.execute(command, dryrun=True)
        assert f"env: INPUT={str(value)}" in "\n".join(caplog.messages)

    def test_execute_task_unknown_env_variable(self, caplog):
        caplog.set_level("INFO")
        task = Task.from_yaml(
            task_with_env(
                f"""
                env:
                  INPUT1: description, default has no type validation
                  INPUT2: description, default has no type validation
                """
            ),
            path="/tmp",
        )
        command = Command(
            task="simple-task",
            env={
                "INPUT1": "value",
                "INPUT2": "value",
                "UNKNOWN1": "value",
                "UNKNOWN2": "value",
            },
        )
        task.execute(command, dryrun=True)

        assert (
            "Ignoring unknown env variables for task `simple-task`: UNKNOWN1, UNKNOWN2. Valid names are: INPUT1, INPUT2"
            in caplog.messages
        ), "\n".join(caplog.messages)

    def test_execute_task_valid_missing_required_fields(self):
        task = Task.from_yaml(
            task_with_env(
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
        command = Command(
            task="simple-task",
        )
        with pytest.raises(ValueError) as exc:
            task.execute(command, dryrun=True)
        assert str(exc.value) == ("Missing required inputs for task `simple-task`: REQUIRED_INPUT, NON_OPTIONAL_INPUT")

    def test_execute_task_with_bash_task(self, bash_task_task_manifest_path, mock_popen):
        task = Task.from_file(bash_task_task_manifest_path)
        command = Command(task="complex-task-task")

        task.execute(command, dryrun=False)

        popen_args = mock_popen.call_args[0][0]
        assert popen_args == ["/bin/bash", "-c", "ls -l ~/"], "Task and argument whould be split"
        assert mock_popen.call_args[1]["cwd"] == os.path.dirname(bash_task_task_manifest_path)

    def test_execute_task_with_complex_task(self, complex_task_task_manifest_path, mock_popen):
        task = Task.from_file(complex_task_task_manifest_path)
        command = Command(task="complex-task-task")

        task.execute(command, dryrun=False)

        popen_args = mock_popen.call_args[0][0]
        assert popen_args == [
            "/bin/bash",
            "-c",
            "echo \"hello world\" | awk '{print $2}'",
        ], "Words in strings should be kept intact and not split"
        assert mock_popen.call_args[1]["cwd"] == os.path.dirname(complex_task_task_manifest_path)
