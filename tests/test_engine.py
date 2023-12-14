import os
from textwrap import dedent
import pytest
import mock

from metl import engine
from metl.models.step import Step
from metl.models.transform import Transform, TransformFailure, UnknownTransformError


def app_file(app_yaml: str, tmpdir):
    path = os.path.join(tmpdir, "app.yml")
    with open(path, "w") as fd:
        fd.write(app_yaml)
    return path


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
