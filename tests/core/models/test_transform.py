from distutils.dir_util import copy_tree
import os
import re
from textwrap import dedent

import mock
from mock import call
import pytest
from metl.core.models.app import Step

from metl.core.models.transform import EnvType, Transform, discover_transforms


def transform_file(transform_yaml: str, tmpdir):
    path = os.path.join(tmpdir, "manifest.yml")
    with open(path, "w") as fd:
        fd.write(transform_yaml)
    return path


@pytest.fixture
def simple_transform_manifest_yml():
    return dedent(
        """
        name: simple-transform
        type: transform
        env-type: python
        env:
          foo: bar
          option-with-hyphens: value
          output: /tmp/data/
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

    @mock.patch("metl.core.models.transform.load_transform_at_path")
    def test_discover_transforms_ignore_test_dirs(
        self, load_transform_at_path, transforms_fixtures_path, simple_transform_manifest_yml, tmpdir
    ):
        repo_dir = tmpdir.mkdir("manifests")
        tests_dir = repo_dir.mkdir("transforms").mkdir("parser").mkdir("tests")
        nested_tests_dir = tests_dir.mkdir("nested").mkdir("deeply")

        copy_tree(transforms_fixtures_path, str(repo_dir))

        for path in [tests_dir, nested_tests_dir]:
            with open(os.path.join(str(path), "manifest.yml"), "w") as fd:
                fd.write(simple_transform_manifest_yml)

        discover_transforms(repo_dir)

        loaded_paths = [c[0][0] for c in load_transform_at_path.call_args_list]
        assert tests_dir not in loaded_paths, 'the "tests" directory was not skipped'
        assert nested_tests_dir not in loaded_paths, 'the nested "tests" directory was not skipped'

    def test_discover_transforms_ignore_invalid_yaml_manifest(self, transforms_fixtures_path, tmpdir):
        repo_dir = str(tmpdir.mkdir("transforms"))
        copy_tree(transforms_fixtures_path, repo_dir)

        os.mkdir(os.path.join(repo_dir, "invalid-yaml-transform"))
        with open(os.path.join(repo_dir, "invalid-yaml-transform", "manifest.yml"), "w") as fd:
            fd.write("not really a manifest")

        transforms = discover_transforms(repo_dir)

        assert sorted(transforms.keys()) == sorted(["splitter", "download", "parser"])

    @pytest.mark.parametrize("required_key", ["name", "run-command"])
    def test_discover_transforms_ignore_missing_required_manifest_field(
        self, required_key, transforms_fixtures_path, tmpdir
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

        assert sorted(transforms.keys()) == ["download", "parser", "splitter"]


class TestDeserialization:
    def test_load_transform_from_file(self, simple_transform_manifest_path):
        transform = Transform.from_file(simple_transform_manifest_path)

        assert transform.name == "simple-transform"
        assert transform.path == os.path.dirname(simple_transform_manifest_path)
        assert transform.env_type == EnvType.PYTHON
        assert transform.env == {
            "FOO": "bar",
            "OPTION_WITH_HYPHENS": "value",
            "OUTPUT": "/tmp/data/",
        }, "The env variable names should have been transformed to uppercase and hyphens replaced with underscores"
        assert transform.run_command == "python run.py"
        assert transform.test_command == "py.test"


class TestExecuteTransform:
    @pytest.fixture(autouse=True)
    def mock_popen(self):
        with mock.patch("metl.core.models.transform.subprocess.Popen") as mock_popen:
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
        with mock.patch("metl.core.models.transform.logger") as mock_logger:
            yield mock_logger

    def test_execute_transform(self, simple_transform_manifest_path, mock_logger, mock_popen):
        transform = Transform.from_file(simple_transform_manifest_path)
        step = Step(
            transform="simple-transform",
        )

        transform.execute(step, dryrun=False)

        assert mock_logger.method_calls == [
            call.info(f"Loading transform at: {simple_transform_manifest_path}"),
            call.info("Now executing transform."),
            call.info("Still executing."),
            call.info("All done."),
        ]

        assert mock_popen.call_args[1]["shell"] is False, "A python transform should run with shell=False"
        assert mock_popen.call_args[1]["cwd"] == os.path.dirname(
            simple_transform_manifest_path
        ), "The cwd should have been set to the directory where the transform manifest is stored"

    def test_execute_transform_dryrun(self, simple_transform_manifest_path, mock_logger):
        transform = Transform.from_file(simple_transform_manifest_path)
        step = Step(
            transform="simple-transform",
            env={
                "foo": "bar",
                "option-with-hyphens": "baz",
                "output": "/tmp/data",
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

    def test_execute_transform_invalid_args(self, simple_transform_manifest_path, mock_logger):
        transform = Transform.from_file(simple_transform_manifest_path)
        step = Step(
            transform="simple-transform",
            env={
                "foo": "bar",
                "invalid-arg": "baz",
                "another_unknown": "fooz",
                "output": "/tmp/data",
            },
        )

        with pytest.raises(ValueError) as exc:
            transform.execute(step)
        assert str(exc.value) == (
            "Invalid input for transform `simple-transform`: INVALID_ARG, ANOTHER_UNKNOWN. "
            "Valid inputs are: FOO, OPTION_WITH_HYPHENS, OUTPUT"
        )

    def test_execute_transform_with_bash_command(self, bash_command_transform_manifest_path, mock_popen):
        transform = Transform.from_file(bash_command_transform_manifest_path)
        step = Step(transform="complex-command-transform")

        transform.execute(step, dryrun=False)

        popen_args = mock_popen.call_args[0][0]
        assert popen_args == ["ls", "-l", "~/"], "Command and argument whould be split"
        assert mock_popen.call_args[1]["shell"] is True, "A bash transform should run with shell=True"
        assert mock_popen.call_args[1]["cwd"] == os.path.dirname(bash_command_transform_manifest_path)

    def test_execute_transform_with_complex_command(self, complex_command_transform_manifest_path, mock_popen):
        transform = Transform.from_file(complex_command_transform_manifest_path)
        step = Step(transform="complex-command-transform")

        transform.execute(step, dryrun=False)

        popen_args = mock_popen.call_args[0][0]
        assert popen_args == [
            "echo",
            "hello world",
            "|",
            "awk",
            "{print $2}",
        ], "Words in strings should be kept intact and not split"
        assert mock_popen.call_args[1]["shell"] is True, "A bash transform should run with shell=True"
        assert mock_popen.call_args[1]["cwd"] == os.path.dirname(complex_command_transform_manifest_path)
