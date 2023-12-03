import os
import re
import subprocess
from textwrap import dedent
import pytest
import mock
import yaml

from metl import runner
from metl.models.step import Step
from metl.models.transform import Transform, TransformFailure, UnknownTransformError


def parse_yaml(yaml_str):
    return yaml.load(yaml_str, yaml.FullLoader)


def app_file(app_yaml: str, tmpdir):
    path = os.path.join(tmpdir, "app.yml")
    with open(path, "w") as fd:
        fd.write(app_yaml)
    return path


def strip_dates(string):
    return re.sub(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+", "2023-11-23 21:36:52.983", string)


@mock.patch("subprocess.run", mock.Mock())
class TestAppManifest(object):
    @mock.patch("metl.runner.execute_job_step", return_value=0)
    def test_run_app_simple_job(self, execute_job_step, app_manifest_simple_path, transforms_fixtures_path):
        runner.run_app(app_manifest_simple_path, transforms_repo_path=transforms_fixtures_path)

        assert execute_job_step.call_count == 1, "`execute_job_step` was called an unexpected number of times"
        actual_steps = [call[1].get("step") or call[0][0] for call in execute_job_step.call_args_list]
        actual_transforms = [call[1].get("transforms") or call[0][1] for call in execute_job_step.call_args_list]
        actual_dryruns = [call[1].get("dryrun") or call[0][2] for call in execute_job_step.call_args_list]

        assert actual_steps == [
            Step(
                transform="download",
                env={
                    "BASE_URL": "http://example.com/data",
                    "THROTTLE": 1000,
                    "OUTPUT": "/tmp/data",
                },
            )
        ]
        actual_transform = actual_transforms[0]
        assert all(
            actual_transform == p for p in actual_transforms
        ), "Each call to `execute_job_step` should have passed the same transforms dict"
        assert sorted(actual_transform.keys()) == ["download", "parser", "splitter"]
        assert all(isinstance(t, Transform) for t in actual_transform.values())
        assert all(dryrun is False for dryrun in actual_dryruns)

    @mock.patch("metl.runner.execute_job_steps")
    @pytest.mark.parametrize("dryrun", [True, False])
    def test_run_app_multiple_single_step_jobs(
        self, execute_job_steps, dryrun, app_manifest_multiple_single_step_jobs_path, transforms_fixtures_path
    ):
        runner.run_app(
            app_manifest_multiple_single_step_jobs_path, dryrun=dryrun, transforms_repo_path=transforms_fixtures_path
        )

        assert execute_job_steps.call_count == 2, "`execute_job_steps` was called an unexpected number of times"

        # check the job_name argument for each call
        actual_job_names = [call[1].get("job_name") or call[0][0] for call in execute_job_steps.call_args_list]
        assert actual_job_names == ["download", "split"]

        # check the steps argument for each call
        actual_steps = [call[1].get("steps") or call[0][1] for call in execute_job_steps.call_args_list]
        actual_steps_names = [[step.transform for step in steps] for steps in actual_steps]
        assert actual_steps_names == [["download"], ["splitter"]]

        # check the transforms argument for each call
        actual_transforms = [call[1].get("transforms") or call[0][2] for call in execute_job_steps.call_args_list]
        actual_transform = actual_transforms[0]
        assert sorted(actual_transform.keys()) == ["download", "parser", "splitter"]
        assert all(
            actual_transform == p for p in actual_transforms
        ), "Each call to `execute_job_steps` should have passed the same transforms dict"

        # check the dryrun argument for each call
        actual_dryruns = [call[1].get("dryrun") or call[0][3] for call in execute_job_steps.call_args_list]
        assert all(actual_dryrun == dryrun for actual_dryrun in actual_dryruns), "Unexpected dryruns: {}".format(
            list(actual_dryruns)
        )

        assert all(actual_dryrun == dryrun for actual_dryrun in actual_dryruns), "Unexpected dryruns: {}".format(
            list(actual_dryruns)
        )

    @mock.patch("metl.runner.execute_job_steps")
    def test_run_app_one_job_multiple_steps(
        self, execute_job_steps, app_manifest_single_multiple_step_job_path, transforms_fixtures_path, tmpdir
    ):
        runner.run_app(app_manifest_single_multiple_step_job_path, transforms_repo_path=transforms_fixtures_path)

        assert execute_job_steps.call_count == 1, "`execute_job_steps` was called an unexpected number of times"

        # check the job_name argument for each call
        actual_job_names = [call[1].get("job_name") or call[0][0] for call in execute_job_steps.call_args_list]
        assert actual_job_names == ["download"]

        # check the steps argument for each call
        actual_steps = [call[1].get("steps") or call[0][1] for call in execute_job_steps.call_args_list]
        actual_steps_names = [[step.transform for step in steps] for steps in actual_steps]
        assert actual_steps_names == [["download", "splitter"]]

        # check the transforms argument for each call
        actual_transforms = [call[1].get("transforms") or call[0][2] for call in execute_job_steps.call_args_list]
        actual_transform = actual_transforms[0]
        assert sorted(actual_transform.keys()) == ["download", "parser", "splitter"]
        assert all(
            actual_transform == p for p in actual_transforms
        ), "Each call to `execute_job_steps` should have passed the same transforms dict"

    @mock.patch("metl.runner.execute_job_step", return_value=127)
    def test_run_stops_if_step_fails(
        self, execute_job_step, app_manifest_single_multiple_step_job_path, transforms_fixtures_path, tmpdir
    ):
        with pytest.raises(TransformFailure) as excinfo:
            runner.run_app(app_manifest_single_multiple_step_job_path, transforms_repo_path=transforms_fixtures_path)

        assert execute_job_step.call_count == 1, "execute_job_step() should have only been called once"
        assert excinfo.value.returncode == 127, "The exception should contain the return code of the failed transform"

    @mock.patch("metl.models.transform.Transform.execute", return_value=127)
    def test_run_app_with_unknown_transform(
        self, transform_execute, app_manifest_simple, transforms_fixtures_path, tmpdir
    ):
        manifest = app_manifest_simple.replace("transform: download", "transform: unknown")
        with pytest.raises(UnknownTransformError) as excinfo:
            runner.run_app(app_file(manifest, tmpdir), transforms_repo_path=transforms_fixtures_path)

        assert str(excinfo.value) == "Unknown transform `unknown`, should be one of: ['download', 'parser', 'splitter']"
        transform_execute.assert_not_called()

    @pytest.mark.parametrize(
        "skip_to, expected_steps",
        [
            ("download-1", [["download-1", "splitter-1"], ["download-2", "splitter-2"]]),
            ("download-2", [["download-2", "splitter-2"]]),
            ("download-1.splitter-1", [["splitter-1"], ["download-2", "splitter-2"]]),
            ("download-2.splitter-2", [["splitter-2"]]),
        ],
        ids=["skip-to-job-1", "skip-to-job-2", "skip-to-job1-step-2", "skip-to-job2-step-2"],
    )
    @mock.patch("metl.runner.execute_job_steps")
    def test_run_app_skip_to(
        self,
        execute_job_steps,
        skip_to,
        expected_steps,
        app_manifest_multiple_jobs_with_multiples_path,
        transforms_fixtures_path,
    ):
        runner.run_app(
            app_manifest_multiple_jobs_with_multiples_path,
            skip_to=skip_to,
            transforms_repo_path=transforms_fixtures_path,
        )

        actual_steps = [call[1].get("steps") or call[0][1] for call in execute_job_steps.call_args_list]
        actual_steps_names = [[step.name for step in steps] for steps in actual_steps]
        assert actual_steps_names == expected_steps

    @mock.patch("metl.runner.execute_job_step", return_value=0)
    def test_run_app_skipped_steps_still_resolve(self, execute_job_step, tmpdir):
        app_manifest = dedent(
            """
            name: Multiple job manifest
            data: /data
            jobs:
              download:
                - name: skipped
                  transform: download
                  env:
                    BASE_URL: http://example.com/data1
                    THROTTLE: 1000
                    OUTPUT: /tmp/data1/source
                - name: references-skipped
                  transform: splitter
                  env:
                    SOURCE: ${previous.env.OUTPUT}
                    OUTPUT: /tmp/data1/splits
            """
        )
        runner.run_app(app_file(app_manifest, tmpdir), skip_to="download.references-skipped")
        assert execute_job_step.call_count == 1, "execute_job_step() should have only been called once"
        executed_step = execute_job_step.call_args[0][0]
        assert executed_step.env["SOURCE"] == "/tmp/data1/source"


class TestRunnerEndToEnd:
    def test_run_app_dryrun(self, tmpdir):
        output_dir = tmpdir.mkdir("output")
        input_dir = tmpdir.mkdir("input")
        input_dir.join("file1.txt").write_text("file1", encoding="utf-8")
        input_dir.join("file2.txt").write_text("file2", encoding="utf-8")

        app = dedent(
            f"""
            name: test-app
            description: A test app to run end-to-end tests on
            data: {output_dir}
            jobs:
              main:
                - name: list
                  transform: list-files
                  env:
                    PATH: {input_dir}
                    OUTPUT: $data/files.txt
                - name: cat
                  transform: cat-files
                  env:
                    FILES: ${{previous.env.OUTPUT}}
                    OUTPUT: $data/cat.txt
            """
        )
        (tmpdir / "app.yml").write_text(app, encoding="utf-8")

        transforms_repo_path = tmpdir.mkdir("transforms")
        list_files_transform = dedent(
            """
            name: list-files
            description: List files in a directory
            env-type: bash
            env:
              PATH: Path to list files in
              OUTPUT: File to write list of files to
            run-command: ls -la $PATH > $OUTPUT
            """
        )
        (transforms_repo_path.mkdir("list-files") / "manifest.yml").write_text(list_files_transform, encoding="utf-8")

        cat_files_transform = dedent(
            """
            name: cat-files
            description: Concatenate files listed in an input file
            env-type: bash
            env:
              FILES: File containing filenames to concatenate
              OUTPUT: File to write concatenated files to
            run-command: cat $FILES | xargs cat > $OUTPUT
            """
        )
        (transforms_repo_path.mkdir("cat-files") / "manifest.yml").write_text(cat_files_transform, encoding="utf-8")

        # runner.run_app(str(tmpdir / "app.yml"), transforms_repo_path=transforms_repo_path)
        result = subprocess.run(
            [
                ".venv/bin/python",
                "-m",
                "metl",
                # "--help"
                str(tmpdir / "app.yml"),
                "--transforms",
                str(transforms_repo_path),
                "--dryrun",
            ],
            capture_output=True,
        )

        expected_output = dedent(
            """
            ╭──╴Running app: {data_dir}/app.yml ╶╴╴╶ ╶
            │ Loading app manifest at: {data_dir}/app.yml
            │ Manifest parsed as:
            │ Discovering transforms at: {data_dir}/transforms
            │ Loading transform at: {data_dir}/transforms/list-files/manifest.yml
            │ Loading transform at: {data_dir}/transforms/cat-files/manifest.yml
            │ Available transforms detected:
            ╔══╸Running job: main ═╴╴╶ ╶
            ║ Running step: #1
            ║   name: list
            ║   description: null
            ║   transform: list-files
            ║   env:
            ║     PATH: {data_dir}/input
            ║     OUTPUT: {data_dir}/output/files.txt
            ║   skip: false
            ║┏━━╸Running transform: list-files ━╴╴╶ ╶
            ║┃2023-11-23 21:36:52.982┊ DRYRUN: Would execute with:
            ║┃2023-11-23 21:36:52.982┊   command: ['ls', '-la', '$PATH', '>', '$OUTPUT']
            ║┃2023-11-23 21:36:52.982┊   cwd: {data_dir}/transforms/list-files
            ║┃2023-11-23 21:36:52.982┊   env: PATH={data_dir}/input, OUTPUT={data_dir}/output/files.txt
            ║┗━━╸Return code: 0 ━╴╴╶ ╶
            ║{space}
            ║ Running step: #2
            ║   name: cat
            ║   description: null
            ║   transform: cat-files
            ║   env:
            ║     FILES: {data_dir}/output/files.txt
            ║     OUTPUT: {data_dir}/output/cat.txt
            ║   skip: false
            ║┏━━╸Running transform: cat-files ━╴╴╶ ╶
            ║┃2023-11-23 21:36:52.983┊ DRYRUN: Would execute with:
            ║┃2023-11-23 21:36:52.983┊   command: ['cat', '$FILES', '|', 'xargs', 'cat', '>', '$OUTPUT']
            ║┃2023-11-23 21:36:52.983┊   cwd: {data_dir}/transforms/cat-files
            ║┃2023-11-23 21:36:52.983┊   env: FILES={data_dir}/output/files.txt, OUTPUT={data_dir}/output/cat.txt
            ║┗━━╸Return code: 0 ━╴╴╶ ╶
            │ Done! \\o/
            """
        ).format(data_dir=str(tmpdir), space=" ")
        actual_result = result.stderr.decode("utf-8")
        assert strip_dates(actual_result.strip()) == strip_dates(expected_output.strip())
