from distutils.dir_util import copy_tree
import os
import re
from textwrap import dedent
from typing import Any

import mock
from mock import call
from pydantic import ValidationError
import pytest
import yaml
from metl.models.step import Step

from metl.models.transform import EnvType, InputDetails, Transform, discover_transforms


def transform_file(transform_yaml: str, tmpdir):
    path = os.path.join(tmpdir, "manifest.yml")
    with open(path, "w") as fd:
        fd.write(transform_yaml)
    return path


def transform_with_env(env: str) -> str:
    return dedent(
        """
        name: simple-transform
        type: transform
        env-type: python
        {env}
        run-command: python run.py
        """
    ).format(env=dedent(env))


@pytest.fixture
def simple_transform_manifest_yml():
    return dedent(
        """
        name: simple-transform
        type: transform
        env-type: python
        transforms: /something
        env:
          FOO:
            description: something
            type: string
            required: true
          OPTION_WITH_HYPHENS:
            description: something else
          OUTPUT:
            description: the result of the transform
            type: string
            required: true
        run-command: python run.py
        test-command: py.test
        """
    )


@pytest.fixture
def simple_transform_manifest_path(simple_transform_manifest_yml, tmpdir):
    return transform_file(simple_transform_manifest_yml, tmpdir)


@pytest.fixture
def bash_command_transform_manifest_yml():
    return dedent(
        """
        name: bash-command-transform
        env-type: bash
        run-command: ls -l ~/
        """
    )


@pytest.fixture
def bash_command_transform_manifest_path(bash_command_transform_manifest_yml, tmpdir):
    return transform_file(bash_command_transform_manifest_yml, tmpdir)


@pytest.fixture
def complex_command_transform_manifest_yml():
    return dedent(
        """
        name: complex-command-transform
        type: transform
        env-type: bash
        run-command: echo "hello world" | awk '{print $2}'
        """
    )


@pytest.fixture
def complex_command_transform_manifest_path(complex_command_transform_manifest_yml, tmpdir):
    return transform_file(complex_command_transform_manifest_yml, tmpdir)


class TestDiscoverTransforms:
    def test_discover_transforms(self, transforms_fixtures_path):
        transforms = discover_transforms(transforms_fixtures_path)

        names_and_paths = [(name, transform.path) for name, transform in transforms.items()]

        assert sorted(names_and_paths) == sorted(
            [
                ("splitter", "{repo_dir}/transforms/splitter".format(repo_dir=transforms_fixtures_path)),
                ("download", "{repo_dir}/transforms/download".format(repo_dir=transforms_fixtures_path)),
                ("parser", "{repo_dir}/transforms/parser".format(repo_dir=transforms_fixtures_path)),
            ]
        )

    def test_discover_transforms_ignore_dirs_without_manifests(self, transforms_fixtures_path, tmpdir):
        repo_dir = str(tmpdir.mkdir("transforms"))
        copy_tree(transforms_fixtures_path, repo_dir)

        os.mkdir(os.path.join(repo_dir, "not-a-transform"))
        with open(os.path.join(repo_dir, "not-a-transform", "manifest"), "w") as fd:
            fd.write("not really a manifest")

        transforms = discover_transforms(repo_dir)

        assert sorted(transforms.keys()) == sorted(["splitter", "download", "parser"])

    def test_discover_transforms_ignore_test_dirs(
        self, transforms_fixtures_path, simple_transform_manifest_yml, tmpdir
    ):
        repo_dir = tmpdir.mkdir("manifests")
        tests_dir = repo_dir.mkdir("transforms").mkdir("parser").mkdir("tests")
        nested_tests_dir = tests_dir.mkdir("nested").mkdir("deeply")

        copy_tree(transforms_fixtures_path, str(repo_dir))

        for path in [tests_dir, nested_tests_dir]:
            with open(os.path.join(str(path), "manifest.yml"), "w") as fd:
                fd.write(simple_transform_manifest_yml)

        transforms = discover_transforms(repo_dir)

        def strip_tmpdir(path):
            return str(path).replace(str(tmpdir), "")

        discovered_paths = [strip_tmpdir(t.path) for t in transforms.values()]

        assert strip_tmpdir(tests_dir) not in discovered_paths, 'the "tests" directory was not skipped'
        assert strip_tmpdir(nested_tests_dir) not in discovered_paths, 'the nested "tests" directory was not skipped'
        assert len(discovered_paths) == 3, "there should be 3 discovered transforms"

    def test_discover_transforms_ignore_invalid_yaml_manifest(self, transforms_fixtures_path, tmpdir, caplog):
        repo_dir = str(tmpdir.mkdir("transforms"))
        copy_tree(transforms_fixtures_path, repo_dir)

        manifest_path = os.path.join(repo_dir, "invalid-yaml-transform", "manifest.yml")
        os.mkdir(os.path.dirname(manifest_path))
        with open(manifest_path, "w") as fd:
            fd.write("not really a manifest")

        transforms = discover_transforms(repo_dir)

        assert (
            f"Skipping transform due to error: Could not load YAML file at path: {manifest_path}; Failed to parse YAML, expected a dictionary"
            in caplog.text
        )
        assert sorted(transforms.keys()) == sorted(["splitter", "download", "parser"])

    @pytest.mark.parametrize("required_key", ["name", "run-command"])
    def test_discover_transforms_ignore_missing_required_manifest_field(
        self, required_key, transforms_fixtures_path, tmpdir, caplog
    ):
        repo_dir = str(tmpdir.mkdir("transforms"))
        copy_tree(transforms_fixtures_path, repo_dir)

        # comment out the parameterized required key
        yaml = re.sub(
            r"^([ \t]*{}\:)".format(required_key),
            r"# \1",
            dedent(
                """
                name: invalid-manifest-transform
                type: transform
                run-command: python run.py
                """
            ),
            flags=re.MULTILINE,
        )
        os.mkdir(os.path.join(repo_dir, "invalid-transform"))
        with open(os.path.join(repo_dir, "invalid-transform", "manifest.yml"), "w") as fd:
            fd.write(yaml)

        transforms = discover_transforms(repo_dir)

        assert (
            f"Skipping transform due to error: Could not load YAML file at path: {repo_dir}/invalid-transform/manifest.yml; 2 validation errors for Transform"
            in caplog.text
        )
        assert sorted(transforms.keys()) == ["download", "parser", "splitter"]

    def test_discover_transforms_list_of_paths(self, transforms_fixtures_path, tmpdir):
        repo_dir1 = str(tmpdir.mkdir("transforms1"))
        repo_dir2 = str(tmpdir.mkdir("transforms2"))
        copy_tree(transforms_fixtures_path + "/transforms/download", repo_dir1)
        copy_tree(transforms_fixtures_path + "/transforms/parser", repo_dir2)

        transforms = discover_transforms([repo_dir1, repo_dir2])

        assert sorted(transforms.keys()) == sorted(
            ["download", "parser"]
        ), "Discovery should have found 1 transform per repo path"


class TestDeserialization:
    def test_load_transform_from_file(self, simple_transform_manifest_path):
        transform = Transform.from_file(simple_transform_manifest_path)

        assert transform.name == "simple-transform"
        assert transform.path == os.path.dirname(simple_transform_manifest_path)
        assert transform.env_type == EnvType.PYTHON
        assert transform.env == {
            "FOO": InputDetails(description="something", type=str),
            "OPTION_WITH_HYPHENS": InputDetails(description="something else"),
            "OUTPUT": InputDetails(description="the result of the transform", type=str),
        }, "The env variable names should have been transformed to uppercase and hyphens replaced with underscores"
        assert transform.run_command == "python run.py"
        assert transform.test_command == "py.test"

    @pytest.mark.parametrize("env_type", [1, "not-a-valid-env-type"])
    def test_transform_env_type_invalid(self, env_type):
        manifest = dedent(
            f"""
            name: simple-transform
            type: transform
            env_type: {env_type}
            path: /tmp
            run-command: python run.py
            """
        )
        with pytest.raises(ValidationError) as exc:
            Transform(**yaml.load(manifest, yaml.FullLoader))
        assert "Input should be 'python' or 'bash'" in str(exc.value)

    def test_transform_env_all_defaults(self):
        manifest = dedent(
            """
            name: simple-transform
            type: transform
            env_type: python
            path: /tmp
            env:
              - FOO
              - BAR
            run-command: python run.py
            """
        )
        transform = Transform(**yaml.load(manifest, yaml.FullLoader))

        assert transform.env == {
            "FOO": InputDetails(description="N/A", required=True, default=None),
            "BAR": InputDetails(description="N/A", required=True, default=None),
        }, "The env variable names should have been transformed to InputDetails with defaults"

    def test_transform_env_just_descriptions(self):
        manifest = dedent(
            """
            name: simple-transform
            type: transform
            env_type: python
            path: /tmp
            env:
              FOO: foo description
              BAR: bar description
              NOT-A-STRING: 1
            run-command: python run.py
            """
        )
        transform = Transform(**yaml.load(manifest, yaml.FullLoader))

        assert transform.env == {
            "FOO": InputDetails(description="foo description"),
            "BAR": InputDetails(description="bar description"),
            "NOT_A_STRING": InputDetails(description="1"),
        }, "The env variable names should have been transformed to InputDetails with defaults"

    def test_transform_env_list_of_keys(self):
        manifest = dedent(
            """
            name: simple-transform
            type: transform
            env_type: python
            path: /tmp
            env:
              - FOO
              - BAR
            run-command: python run.py
            """
        )
        transform = Transform(**yaml.load(manifest, yaml.FullLoader))

        assert transform.env == {
            "FOO": InputDetails(description="N/A"),
            "BAR": InputDetails(description="N/A"),
        }, "The env variable names should have been transformed to InputDetails with defaults"

    def test_transform_env_invalid(self):
        manifest = dedent(
            """
            name: simple-transform
            type: transform
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
            Transform(**yaml.load(manifest, yaml.FullLoader))
        assert "Transform env names must be strings, the following are invalid: 1, 2.2" in str(exc.value)

    def test_transform_env_all_explicit(self):
        manifest = dedent(
            """
            name: simple-transform
            type: transform
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
        transform = Transform(**yaml.load(manifest, yaml.FullLoader))

        assert transform.env == {
            "FOO": InputDetails(description="foo description", required=False, default="booya", type=str),
            "BAR": InputDetails(description="bar description", required=True, default=None, type=bool),
        }, "The env variable names should have been transformed to InputDetails with defaults"

    def test_transform_env_optional(self):
        manifest = dedent(
            """
            name: simple-transform
            type: transform
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
        transform = Transform(**yaml.load(manifest, yaml.FullLoader))

        assert transform.env == {
            "FOO": InputDetails(description="foo description", required=False),
            "BAR": InputDetails(description="bar description", required=True),
        }, "The env variable names should have been transformed to InputDetails with defaults"

    def test_transform_env_specify_both_optional_and_required(self):
        manifest = dedent(
            """
            name: simple-transform
            type: transform
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
            Transform(**yaml.load(manifest, yaml.FullLoader))
        assert "Cannot specify both `required` and `optional`" in str(exc.value)


class TestExecuteTransform:
    @pytest.fixture(autouse=True)
    def mock_popen(self):
        with mock.patch("metl.models.transform.subprocess.Popen") as mock_popen:
            mock_popen.return_value.poll.return_value = 0
            mock_popen.return_value.returncode = 0
            mock_popen.return_value.stdout.readline.side_effect = [
                "Now executing transform.",
                "Still executing.",
                "All done.",
                "",
            ]
            mock_popen.return_value.kill.return_value = None
            yield mock_popen

    @pytest.fixture
    def mock_logger(self):
        with mock.patch("metl.models.transform.logger") as mock_logger:
            yield mock_logger

    def test_execute_transform(self, simple_transform_manifest_path, mock_logger, mock_popen):
        transform = Transform.from_file(simple_transform_manifest_path)
        step = Step(
            transform="simple-transform",
            env={
                "FOO": "bar",
                "OPTION_WITH_HYPHENS": "baz",
                "OUTPUT": "/tmp/data",
            },
        )

        transform.execute(step, dryrun=False)

        assert mock_logger.method_calls == [
            call.info(f"Loading transform at: {simple_transform_manifest_path}"),
            call.info("Now executing transform."),
            call.info("Still executing."),
            call.info("All done."),
        ]

        assert mock_popen.call_args[1]["cwd"] == os.path.dirname(
            simple_transform_manifest_path
        ), "The cwd should have been set to the directory where the transform manifest is stored"

    def test_execute_transform_dryrun(self, simple_transform_manifest_path, mock_logger):
        transform = Transform.from_file(simple_transform_manifest_path)
        step = Step(
            transform="simple-transform",
            env={
                "FOO": "bar",
                "OPTION_WITH_HYPHENS": "baz",
                "OUTPUT": "/tmp/data",
            },
        )

        transform.execute(step, dryrun=True)
        assert mock_logger.method_calls == [
            call.info(f"Loading transform at: {simple_transform_manifest_path}"),
            call.info("DRYRUN: Would execute with:"),
            call.info("  command: ['python', 'run.py']"),
            call.info(f"  cwd: {os.path.dirname(simple_transform_manifest_path)}"),
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
    def test_execute_transform_valid_env_value_and_type(self, var_type, var_value):
        transform = Transform.from_yaml(
            transform_with_env(
                f"""
                env:
                  INPUT:
                    type: {var_type}
                """
            ),
            path="/tmp",
        )
        step = Step(
            transform="simple-transform",
            env={
                "INPUT": var_value,
            },
        )
        transform.execute(step, dryrun=True)

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
    def test_execute_transform_invalid_env_value_types(self, var_type, var_value, message):
        transform = Transform.from_yaml(
            transform_with_env(
                f"""
                env:
                  INPUT:
                    type: {var_type}
                """
            ),
            path="/tmp",
        )
        step = Step(
            transform="simple-transform",
            env={
                "INPUT": var_value,
            },
        )
        with pytest.raises(ValueError) as exc:
            transform.execute(step, dryrun=True)
        assert str(exc.value) == (f"Invalid env values for transform `simple-transform`:\n - INPUT: {message}")

    @pytest.mark.parametrize("value", [1, 1.23, True, "string"])
    def test_execute_transform_defaults_to_any_type(self, value, caplog):
        caplog.set_level("INFO")
        transform = Transform.from_yaml(
            transform_with_env(
                f"""
                env:
                  INPUT: description, default has no type validation
                """
            ),
            path="/tmp",
        )
        step = Step(
            transform="simple-transform",
            env={
                "input": value,
            },
        )
        transform.execute(step, dryrun=True)
        assert f"env: INPUT={str(value)}" in "\n".join(caplog.messages)

    def test_execute_transform_valid_missiong_required_fields(self):
        transform = Transform.from_yaml(
            transform_with_env(
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
        step = Step(
            transform="simple-transform",
        )
        with pytest.raises(ValueError) as exc:
            transform.execute(step, dryrun=True)
        assert str(exc.value) == (
            "Missing required inputs for transform `simple-transform`: REQUIRED_INPUT, NON_OPTIONAL_INPUT"
        )

    def test_execute_transform_with_bash_command(self, bash_command_transform_manifest_path, mock_popen):
        transform = Transform.from_file(bash_command_transform_manifest_path)
        step = Step(transform="complex-command-transform")

        transform.execute(step, dryrun=False)

        popen_args = mock_popen.call_args[0][0]
        assert popen_args == ["/bin/bash", "-c", "ls -l ~/"], "Command and argument whould be split"
        assert mock_popen.call_args[1]["cwd"] == os.path.dirname(bash_command_transform_manifest_path)

    def test_execute_transform_with_complex_command(self, complex_command_transform_manifest_path, mock_popen):
        transform = Transform.from_file(complex_command_transform_manifest_path)
        step = Step(transform="complex-command-transform")

        transform.execute(step, dryrun=False)

        popen_args = mock_popen.call_args[0][0]
        assert popen_args == [
            "/bin/bash",
            "-c",
            "echo \"hello world\" | awk '{print $2}'",
        ], "Words in strings should be kept intact and not split"
        assert mock_popen.call_args[1]["cwd"] == os.path.dirname(complex_command_transform_manifest_path)
