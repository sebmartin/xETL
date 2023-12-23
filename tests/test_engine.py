import os
from textwrap import dedent
import pytest
import mock

from xetl import engine
from xetl.models.task import Task
from xetl.models.command import Command, CommandFailure, UnknownCommandError


def job_file(job_yaml: str, tmpdir):
    path = os.path.join(tmpdir, "job.yml")
    with open(path, "w") as fd:
        fd.write(job_yaml)
    return path


@mock.patch("subprocess.run", mock.Mock())
class TestJobManifest(object):
    @mock.patch("xetl.engine.execute_job_task", return_value=0)
    def test_execute_job_simple_job(self, execute_job_task, job_manifest_simple_path):
        engine.execute_job(job_manifest_simple_path)

        assert execute_job_task.call_count == 1, "`execute_job_task` was called an unexpected number of times"
        actual_tasks = [call[1].get("task") or call[0][0] for call in execute_job_task.call_args_list]
        actual_commands = [call[1].get("commands") or call[0][1] for call in execute_job_task.call_args_list]
        actual_dryruns = [call[1].get("dryrun") or call[0][2] for call in execute_job_task.call_args_list]

        assert actual_tasks == [
            Task(
                command="download",
                env={
                    "JOB_VAR": "job-var-value",
                    "BASE_URL": "http://example.com/data",
                    "THROTTLE": 1000,
                    "OUTPUT": "/tmp/data",
                },
            )
        ]
        actual_command = actual_commands[0]
        assert all(
            actual_command == p for p in actual_commands
        ), "Each call to `execute_job_task` should have passed the same commands dict"
        assert sorted(actual_command.keys()) == ["download", "parser", "splitter"]
        assert all(isinstance(t, Command) for t in actual_command.values())
        assert all(dryrun is False for dryrun in actual_dryruns)

    @mock.patch("xetl.engine.execute_job_tasks")
    def test_execute_job_multiple_tasks(
        self, execute_job_tasks, job_manifest_multiple_tasks_path, commands_fixtures_path, tmpdir
    ):
        engine.execute_job(job_manifest_multiple_tasks_path)

        assert execute_job_tasks.call_count == 1, "`execute_job_tasks` was called an unexpected number of times"

        # check the job_name argument for each call
        actual_job_names = [call[1].get("job_name") or call[0][0] for call in execute_job_tasks.call_args_list]
        assert actual_job_names == ["Multiple job manifest"]

        # check the tasks argument for each call
        actual_tasks = [call[1].get("tasks") or call[0][1] for call in execute_job_tasks.call_args_list]
        actual_tasks_names = [[task.command for task in tasks] for tasks in actual_tasks]
        assert actual_tasks_names == [["download", "splitter"]]

        # check the commands argument for each call
        actual_commands = [call[1].get("commands") or call[0][2] for call in execute_job_tasks.call_args_list]
        actual_command = actual_commands[0]
        assert sorted(actual_command.keys()) == ["download", "parser", "splitter"]
        assert all(
            actual_command == p for p in actual_commands
        ), "Each call to `execute_job_tasks` should have passed the same commands dict"

    @mock.patch("xetl.engine.execute_job_task", return_value=127)
    def test_execute_job_stops_if_task_fails(
        self, execute_job_task, job_manifest_multiple_tasks_path, commands_fixtures_path, tmpdir
    ):
        with pytest.raises(CommandFailure) as excinfo:
            engine.execute_job(job_manifest_multiple_tasks_path)

        assert execute_job_task.call_count == 1, "execute_job_task() should have only been called once"
        assert excinfo.value.returncode == 127, "The exception should contain the return code of the failed command"

    @mock.patch("xetl.engine.execute_job_tasks")
    def test_execute_job_without_commands_path_warns(self, execute_job_tasks, tmpdir, caplog):
        manifest = dedent(
            """
            name: Job without manifests
            data: /data
            tasks: []
            """
        )
        engine.execute_job(job_file(manifest, str(tmpdir)))
        assert (
            "The property `commands` is not defined in the job manifest, no commands will be available"
            in caplog.messages
        )

    @mock.patch("xetl.engine.execute_job_tasks")
    def test_execute_job_no_commands_found(self, execute_job_tasks, tmpdir, caplog):
        manifest = dedent(
            """
            name: Job without manifests
            data: /data
            commands: /tmp/does-not-exist
            tasks: []
            """
        )
        engine.execute_job(job_file(manifest, str(tmpdir)))
        assert "Could not find any commands at paths ['/tmp/does-not-exist']" in caplog.messages

    @mock.patch("xetl.models.command.Command.execute", return_value=127)
    def test_execute_job_with_unknown_command(
        self, command_execute, job_manifest_simple, commands_fixtures_path, tmpdir
    ):
        manifest = job_manifest_simple.replace("command: download", "command: unknown")
        with pytest.raises(UnknownCommandError) as excinfo:
            engine.execute_job(job_file(manifest, tmpdir))

        assert str(excinfo.value) == "Unknown command `unknown`, should be one of: ['download', 'parser', 'splitter']"
        command_execute.assert_not_called()

    @pytest.mark.parametrize(
        "skip_to, expected_tasks",
        [
            ("download", [["Download", "Splitter"]]),
            ("splitter", [["Splitter"]]),
            ("SPLITTER", [["Splitter"]]),
        ],
        ids=["skip-to-task-1", "skip-to-task-2", "case-insensitive"],
    )
    @mock.patch("xetl.engine.execute_job_tasks")
    def test_execute_job_skip_to(
        self,
        execute_job_tasks,
        skip_to,
        expected_tasks,
        job_manifest_multiple_tasks_path,
    ):
        engine.execute_job(job_manifest_multiple_tasks_path, skip_to=skip_to)

        actual_tasks = [call[1].get("tasks") or call[0][1] for call in execute_job_tasks.call_args_list]
        actual_tasks_names = [[task.name for task in tasks] for tasks in actual_tasks]
        assert actual_tasks_names == expected_tasks

    @mock.patch("xetl.engine.execute_job_task", return_value=0)
    def test_execute_job_skipped_tasks_still_resolve(self, execute_job_task, tmpdir):
        job_manifest = dedent(
            """
            name: Multiple job manifest
            data: /data
            tasks:
              - name: skipped
                command: download
                skip: true
                env:
                  BASE_URL: http://example.com/data1
                  THROTTLE: 1000
                  OUTPUT: /tmp/data1/source
              - name: references-skipped
                command: splitter
                env:
                  SOURCE: ${previous.OUTPUT}
                  OUTPUT: /tmp/data1/splits
            """
        )
        engine.execute_job(job_file(job_manifest, tmpdir))
        assert execute_job_task.call_count == 1, "execute_job_task() should have only been called once"
        executed_task = execute_job_task.call_args[0][0]
        assert executed_task.env["SOURCE"] == "/tmp/data1/source"
