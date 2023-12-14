import os
import re
import subprocess
from textwrap import dedent
import pytest
import mock
import yaml

from metl import engine
from metl.models.step import Step
from metl.models.transform import Transform, TransformFailure, UnknownTransformError


def app_file(app_yaml: str, tmpdir):
    path = os.path.join(tmpdir, "app.yml")
    with open(path, "w") as fd:
        fd.write(app_yaml)
    return path


def strip_dates(string):
    return re.sub(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+", "2023-11-23 21:36:52.983", string)


@mock.patch("subprocess.run", mock.Mock())
class TestAppManifest(object):
    @mock.patch("metl.engine.execute_job_step", return_value=0)
    def test_execute_app_simple_job(self, execute_job_step, app_manifest_simple_path, transforms_fixtures_path):
        engine.execute_app(app_manifest_simple_path)

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

    @mock.patch("metl.engine.execute_job_steps")
    @pytest.mark.parametrize("dryrun", [True, False])
    def test_execute_app_multiple_single_step_jobs(
        self, execute_job_steps, dryrun, app_manifest_multiple_single_step_jobs_path, transforms_fixtures_path
    ):
        engine.execute_app(app_manifest_multiple_single_step_jobs_path, dryrun=dryrun)

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

    @mock.patch("metl.engine.execute_job_steps")
    def test_execute_app_one_job_multiple_steps(
        self, execute_job_steps, app_manifest_single_multiple_step_job_path, transforms_fixtures_path, tmpdir
    ):
        engine.execute_app(app_manifest_single_multiple_step_job_path)

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

    @mock.patch("metl.engine.execute_job_step", return_value=127)
    def test_execute_app_stops_if_step_fails(
        self, execute_job_step, app_manifest_single_multiple_step_job_path, transforms_fixtures_path, tmpdir
    ):
        with pytest.raises(TransformFailure) as excinfo:
            engine.execute_app(app_manifest_single_multiple_step_job_path)

        assert execute_job_step.call_count == 1, "execute_job_step() should have only been called once"
        assert excinfo.value.returncode == 127, "The exception should contain the return code of the failed transform"

    @mock.patch("metl.engine.execute_job_steps")
    def test_execute_app_without_transforms_path_warns(self, execute_job_steps, tmpdir, caplog):
        manifest = dedent(
            """
            name: Job without manifests
            data: /data
            jobs: {}
            """
        )
        engine.execute_app(app_file(manifest, str(tmpdir)))
        assert (
            "The property `transforms` is not defined in the app manifest, no transforms will be available"
            in caplog.messages
        )

    @mock.patch("metl.engine.execute_job_steps")
    def test_execute_app_no_transforms_found(self, execute_job_steps, tmpdir, caplog):
        manifest = dedent(
            """
            name: Job without manifests
            data: /data
            transforms: /tmp/does-not-exist
            jobs: {}
            """
        )
        engine.execute_app(app_file(manifest, str(tmpdir)))
        assert "Could not find any transforms at paths ['/tmp/does-not-exist']" in caplog.messages

    @mock.patch("metl.models.transform.Transform.execute", return_value=127)
    def test_execute_app_with_unknown_transform(
        self, transform_execute, app_manifest_simple, transforms_fixtures_path, tmpdir
    ):
        manifest = app_manifest_simple.replace("transform: download", "transform: unknown")
        with pytest.raises(UnknownTransformError) as excinfo:
            engine.execute_app(app_file(manifest, tmpdir))

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
    @mock.patch("metl.engine.execute_job_steps")
    def test_execute_app_skip_to(
        self,
        execute_job_steps,
        skip_to,
        expected_steps,
        app_manifest_multiple_jobs_with_multiples_path,
    ):
        engine.execute_app(app_manifest_multiple_jobs_with_multiples_path, skip_to=skip_to)

        actual_steps = [call[1].get("steps") or call[0][1] for call in execute_job_steps.call_args_list]
        actual_steps_names = [[step.name for step in steps] for steps in actual_steps]
        assert actual_steps_names == expected_steps

    @mock.patch("metl.engine.execute_job_step", return_value=0)
    def test_execute_app_skipped_steps_still_resolve(self, execute_job_step, tmpdir):
        app_manifest = dedent(
            """
            name: Multiple job manifest
            data: /data
            jobs:
              download:
                - name: skipped
                  transform: download
                  skip: true
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
        engine.execute_app(app_file(app_manifest, tmpdir))
        assert execute_job_step.call_count == 1, "execute_job_step() should have only been called once"
        executed_step = execute_job_step.call_args[0][0]
        assert executed_step.env["SOURCE"] == "/tmp/data1/source"


class TestEngineEndToEnd:
    @pytest.fixture
    def transforms_repo_path(self, tmpdir):
        return tmpdir.mkdir("transforms")

    @pytest.fixture
    def output_dir(self, tmpdir):
        return tmpdir.mkdir("output")

    @pytest.fixture
    def app_manifest(self, transforms_repo_path, output_dir, tmpdir):
        app = dedent(
            f"""
            name: test-app
            description: A test app to run end-to-end tests on
            data: {output_dir}
            transforms: {transforms_repo_path}
            jobs:
              main:
                - name: print-env
                  transform: print-env
                  env:
                    INPUT1: 100
                    INPUT2: false
                    TEMP_FILE: ${{tmp.file}}
                    OUTPUT: $data/env.txt
                - name: filter-env
                  transform: filter
                  env:
                    FILE: ${{previous.env.OUTPUT}}
                    PATTERN: -i input
                    OUTPUT: $data/result.txt
            """
        )
        app_path = tmpdir / "app.yml"
        (app_path).write_text(app, encoding="utf-8")
        return app_path

    @pytest.fixture
    def print_env_transform(self, transforms_repo_path):
        print_env_transform = dedent(
            f"""
            name: print-env
            description: Prints all env variables
            env-type: bash
            env:
              OUTPUT:
                description: File to write env values to
                type: string
              TEMP_FILE:
                description: File to write temp values to
                type: string
              INPUT1:
                description: First input variable
                type: int
              INPUT2:
                description: Second input variable
                type: bool
            run-command: |
              echo "Temp values stored at $TEMP_FILE"
              /usr/bin/env > $TEMP_FILE
              ls "$TEMP_FILE"
              cat $TEMP_FILE > $OUTPUT
            """
        )
        print_env_transform_path = transforms_repo_path.mkdir("print-env") / "manifest.yml"
        (print_env_transform_path).write_text(print_env_transform, encoding="utf-8")
        return print_env_transform_path

    @pytest.fixture
    def filter_env_transform(self, transforms_repo_path):
        filter_env_transform = dedent(
            """
            name: filter
            description: Concatenate files listed in an input file
            env-type: bash
            env:
              FILE:
                descriptiong: File to filter lines from
                type: string
              PATTERN:
                description: Pattern to filter lines with
                type: string
              OUTPUT:
                description: File to write concatenated files to
                type: string
            run-command: cat $FILE | grep $PATTERN | tee $OUTPUT
            """
        )
        filter_env_transform_path = transforms_repo_path.mkdir("filter") / "manifest.yml"
        (filter_env_transform_path).write_text(filter_env_transform, encoding="utf-8")
        return filter_env_transform_path

    def test_execute_bash_app(self, app_manifest, print_env_transform, filter_env_transform, output_dir, tmpdir):
        result = subprocess.run(
            [
                ".venv/bin/python",
                "-m",
                "metl",
                str(app_manifest),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        # Print the output if it wasn't successful
        assert result.returncode == 0, result.stdout.decode("utf-8")

        # Test resulting files
        assert os.path.exists(str(output_dir / "env.txt")), "The first step's file should have been created"
        assert os.path.exists(str(output_dir / "result.txt")), "The final result file should have been created"
        with open(str(output_dir / "result.txt"), "r") as fd:
            assert fd.readlines() == ["INPUT1=100\n", "INPUT2=False\n"]

        # Test output
        if tmp_path_match := re.search(r"[^ ]*output/tmp/\w*", result.stdout.decode("utf-8")):
            tmp_file = tmp_path_match.group(0)
        else:
            tmp_file = "/tmp"
        expected_output = dedent(
            """
            Loading app manifest at: {data_dir}/app.yml
            ╭──╴Executing app: test-app ╶╴╴╶ ╶
            │ Parsed manifest for app: test-app
            │ Discovering transforms at paths: ['{data_dir}/transforms']
            │ Loading transform at: {data_dir}/transforms/print-env/manifest.yml
            │ Loading transform at: {data_dir}/transforms/filter/manifest.yml
            │ Available transforms detected:
            │  - print-env
            │  - filter
            ╔══╸Executing job: main ═╴╴╶ ╶
            ║ Executing step 1 of 2
            ║   name: print-env
            ║   description: null
            ║   transform: print-env
            ║   env:
            ║     INPUT1: 100
            ║     INPUT2: false
            ║     TEMP_FILE: {tmp_file}
            ║     OUTPUT: {data_dir}/output/env.txt
            ║   skip: false
            ║┏━━╸Executing transform: print-env ━╴╴╶ ╶
            ║┃2023-11-23 21:36:52.983┊ Temp values stored at {tmp_file}
            ║┃2023-11-23 21:36:52.983┊ {tmp_file}
            ║┗━━╸Return code: 0 ━╴╴╶ ╶
            ║{space}
            ║ Executing step 2 of 2
            ║   name: filter-env
            ║   description: null
            ║   transform: filter
            ║   env:
            ║     FILE: {data_dir}/output/env.txt
            ║     PATTERN: -i input
            ║     OUTPUT: {data_dir}/output/result.txt
            ║   skip: false
            ║┏━━╸Executing transform: filter ━╴╴╶ ╶
            ║┃2023-11-23 21:36:52.983┊ INPUT1=100
            ║┃2023-11-23 21:36:52.983┊ INPUT2=False
            ║┗━━╸Return code: 0 ━╴╴╶ ╶
            │ Done! \\o/
            """
        ).format(data_dir=str(tmpdir), space=" ", tmp_file=tmp_file)
        actual_result = result.stdout.decode("utf-8")
        assert strip_dates(actual_result.strip()) == strip_dates(expected_output.strip())

    def test_execute_bash_app_dryrun(self, app_manifest, print_env_transform, filter_env_transform, output_dir, tmpdir):
        result = subprocess.run(
            [
                ".venv/bin/python",
                "-m",
                "metl",
                str(tmpdir / "app.yml"),
                "--dryrun",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        # Print the output if it wasn't successful
        assert result.returncode == 0, result.stdout.decode("utf-8")

        if tmp_path_match := re.search(r"[^ ]*output/tmp/\w*", result.stdout.decode("utf-8")):
            tmp_file = tmp_path_match.group(0)
        else:
            tmp_file = "/tmp"
        expected_output = dedent(
            """
            Loading app manifest at: {data_dir}/app.yml
            ╭──╴Executing app: test-app ╶╴╴╶ ╶
            │ Manifest parsed as:
            │   name: test-app
            │   description: A test app to run end-to-end tests on
            │   data: {data_dir}/output
            │   transforms:
            │   - {data_dir}/transforms
            │   jobs:
            │     main:
            │     - name: print-env
            │       transform: print-env
            │       env:
            │         INPUT1: 100
            │         INPUT2: false
            │         TEMP_FILE: {tmp_file}
            │         OUTPUT: {data_dir}/output/env.txt
            │     - name: filter-env
            │       transform: filter
            │       env:
            │         FILE: {data_dir}/output/env.txt
            │         PATTERN: -i input
            │         OUTPUT: {data_dir}/output/result.txt
            │ Discovering transforms at paths: ['{data_dir}/transforms']
            │ Loading transform at: {data_dir}/transforms/print-env/manifest.yml
            │ Loading transform at: {data_dir}/transforms/filter/manifest.yml
            │ Available transforms detected:
            │  - print-env
            │  - filter
            ╔══╸Executing job: main ═╴╴╶ ╶
            ║ Executing step 1 of 2
            ║   name: print-env
            ║   description: null
            ║   transform: print-env
            ║   env:
            ║     INPUT1: 100
            ║     INPUT2: false
            ║     TEMP_FILE: {tmp_file}
            ║     OUTPUT: {data_dir}/output/env.txt
            ║   skip: false
            ║┏━━╸Executing transform: print-env ━╴╴╶ ╶
            ║┃2023-12-12 21:46:35.601┊ DRYRUN: Would execute with:
            ║┃2023-12-12 21:46:35.601┊   command: ['/bin/bash', '-c', 'echo "Temp values stored at $TEMP_FILE"\\n/usr/bin/env > $TEMP_FILE\\nls "$TEMP_FILE"\\ncat $TEMP_FILE > $OUTPUT\\n']
            ║┃2023-12-12 21:46:35.601┊   cwd: {data_dir}/transforms/print-env
            ║┃2023-12-12 21:46:35.601┊   env: INPUT1=100, INPUT2=False, TEMP_FILE={tmp_file}, OUTPUT={data_dir}/output/env.txt
            ║┗━━╸Return code: 0 ━╴╴╶ ╶
            ║{space}
            ║ Executing step 2 of 2
            ║   name: filter-env
            ║   description: null
            ║   transform: filter
            ║   env:
            ║     FILE: {data_dir}/output/env.txt
            ║     PATTERN: -i input
            ║     OUTPUT: {data_dir}/output/result.txt
            ║   skip: false
            ║┏━━╸Executing transform: filter ━╴╴╶ ╶
            ║┃2023-12-12 21:46:35.602┊ DRYRUN: Would execute with:
            ║┃2023-12-12 21:46:35.603┊   command: ['/bin/bash', '-c', 'cat $FILE | grep $PATTERN | tee $OUTPUT']
            ║┃2023-12-12 21:46:35.603┊   cwd: {data_dir}/transforms/filter
            ║┃2023-12-12 21:46:35.603┊   env: FILE={data_dir}/output/env.txt, PATTERN=-i input, OUTPUT={data_dir}/output/result.txt
            ║┗━━╸Return code: 0 ━╴╴╶ ╶
            │ Done! \\o/
            """
        ).format(data_dir=str(tmpdir), space=" ", tmp_file=tmp_file)
        actual_result = result.stdout.decode("utf-8")
        assert strip_dates(actual_result.strip()) == strip_dates(expected_output.strip())

    def test_execute_with_failure(self, output_dir, transforms_repo_path, tmpdir):
        app = dedent(
            f"""
            name: test-app
            description: A test app to run end-to-end tests on
            data: {output_dir}
            transforms: {transforms_repo_path}
            jobs:
              main:
                - name: fail
                  transform: fail
            """
        )
        app_path = tmpdir / "app.yml"
        (app_path).write_text(app, encoding="utf-8")

        filter_env_transform = dedent(
            """
            name: fail
            description: This is a transform that always fails
            env-type: bash
            run-command: cat /file/that/doesnt/exist
            """
        )
        filter_env_transform_path = transforms_repo_path.mkdir("filter") / "manifest.yml"
        (filter_env_transform_path).write_text(filter_env_transform, encoding="utf-8")

        result = subprocess.run(
            [
                ".venv/bin/python",
                "-m",
                "metl",
                str(tmpdir / "app.yml"),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        expected_return_code = 1
        assert result.returncode == expected_return_code, result.stdout.decode("utf-8")

        expected_output = dedent(
            """
            Loading app manifest at: {data_dir}/app.yml
            ╭──╴Executing app: test-app ╶╴╴╶ ╶
            │ Parsed manifest for app: test-app
            │ Discovering transforms at paths: ['{data_dir}/transforms']
            │ Loading transform at: {data_dir}/transforms/filter/manifest.yml
            │ Available transforms detected:
            │  - fail
            ╔══╸Executing job: main ═╴╴╶ ╶
            ║ Executing step 1 of 1
            ║   name: fail
            ║   description: null
            ║   transform: fail
            ║   env: {{}}
            ║   skip: false
            ║┏━━╸Executing transform: fail ━╴╴╶ ╶
            ║┃2023-11-23 21:36:52.983┊ cat: /file/that/doesnt/exist: No such file or directory
            ║┗━━╸Return code: {error_code} ━╴╴╶ ╶
            Transform failed, terminating job.
            """
        ).format(data_dir=str(tmpdir), space=" ", error_code=expected_return_code)
        actual_result = result.stdout.decode("utf-8")
        assert strip_dates(actual_result.strip()) == strip_dates(expected_output.strip())
