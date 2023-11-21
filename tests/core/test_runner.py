import os
from textwrap import dedent
import pytest
import mock
import yaml

from metl.core import runner
from metl.core.models.app import Step
from metl.core.models.transform import Transform


def parse_yaml(yaml_str):
    return yaml.load(yaml_str, yaml.FullLoader)


@mock.patch("subprocess.run", mock.Mock())
class TestAppManifest(object):
    @mock.patch("metl.core.runner.execute_transform")
    def test_run_app_simple_job(self, execute_transform, app_manifest_simple_path, transforms_fixtures_path):
        runner.run_app(app_manifest_simple_path, transforms_repo_path=transforms_fixtures_path)

        assert execute_transform.call_count == 1, "`execute_transform` was called an unexpected number of times"
        actual_steps = [call[1].get("step") or call[0][0] for call in execute_transform.call_args_list]
        actual_transforms = [call[1].get("transforms") or call[0][1] for call in execute_transform.call_args_list]
        actual_dryruns = [call[1].get("dryrun") or call[0][2] for call in execute_transform.call_args_list]

        assert actual_steps == [
            Step(
                transform="download",
                args={"base_url": "http://example.com/data", "throttle": 1000},
                output="/tmp/data/morgues",
            )
        ]
        actual_transform = actual_transforms[0]
        assert all(
            actual_transform == p for p in actual_transforms
        ), "Each call to `execute_transform` should have passed the same transforms dict"
        assert sorted(actual_transform.keys()) == ["morgue-splitter", "morgues-download", "parser"]
        assert all(isinstance(t, Transform) for t in actual_transform.values())
        assert all(dryrun == False for dryrun in actual_dryruns)

    @mock.patch("metl.core.runner.execute_job_steps")
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
        assert sorted(actual_transform.keys()) == ["morgue-splitter", "morgues-download", "parser"]
        assert all(
            actual_transform == p for p in actual_transforms
        ), "Each call to `execute_transform` should have passed the same transforms dict"

        # check the dryrun argument for each call
        actual_dryruns = [call[1].get("dryrun") or call[0][3] for call in execute_job_steps.call_args_list]
        assert all(actual_dryrun == dryrun for actual_dryrun in actual_dryruns), "Unexpected dryruns: {}".format(
            list(actual_dryruns)
        )

        assert all(actual_dryrun == dryrun for actual_dryrun in actual_dryruns), "Unexpected dryruns: {}".format(
            list(actual_dryruns)
        )

    @mock.patch("metl.core.runner.execute_job_steps")
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
        assert sorted(actual_transform.keys()) == ["morgue-splitter", "morgues-download", "parser"]
        assert all(
            actual_transform == p for p in actual_transforms
        ), "Each call to `execute_transform` should have passed the same transforms dict"

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
    @mock.patch("metl.core.runner.execute_job_steps")
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
