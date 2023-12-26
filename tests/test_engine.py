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
    @mock.patch("xetl.models.command.Command.execute", return_value=0, autospec=True)
    def test_execute_job_simple_job(self, command_execute, job_manifest_simple_path):
        engine.execute_job(job_manifest_simple_path)

        comands_and_tasks = [
            f"command: {call.args[0].name}, task: {call.args[1].name}, dryrun: {call.args[2]}"
            for call in command_execute.call_args_list
        ]
        assert comands_and_tasks == [
            "command: download, task: Download, dryrun: False",
        ]

    @mock.patch("xetl.models.command.Command.execute", return_value=0, autospec=True)
    def test_execute_job_multiple_tasks(
        self, command_execute, job_manifest_multiple_tasks_path, commands_fixtures_path, tmpdir
    ):
        engine.execute_job(job_manifest_multiple_tasks_path)

        comands_and_tasks = [
            f"command: {call.args[0].name}, task: {call.args[1].name}, dryrun: {call.args[2]}"
            for call in command_execute.call_args_list
        ]
        assert comands_and_tasks == [
            "command: download, task: Download, dryrun: False",
            "command: splitter, task: Splitter, dryrun: False",
        ]

    @mock.patch("xetl.models.command.Command.execute", return_value=127)
    def test_execute_job_stops_if_task_fails(
        self, command_execute, job_manifest_multiple_tasks_path, commands_fixtures_path, tmpdir
    ):
        with pytest.raises(CommandFailure) as excinfo:
            engine.execute_job(job_manifest_multiple_tasks_path)

        assert command_execute.call_count == 1, "Command.execute() should have only been called once"
        assert excinfo.value.returncode == 127, "The exception should contain the return code of the failed command"

    @mock.patch("xetl.models.command.Command.execute")
    def test_execute_job_without_commands_path_warns(self, execute_command, tmpdir, caplog):
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

    @mock.patch("xetl.models.command.Command.execute")
    def test_execute_job_no_commands_found(self, execute_command, tmpdir, caplog):
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
        "skip_to, expected_executed_tasks",
        [
            (None, ["Download", "Splitter"]),
            ("download", ["Download", "Splitter"]),
            ("splitter", ["Splitter"]),
            ("SPLITTER", ["Splitter"]),
        ],
        ids=["not-set", "skip-to-task-1", "skip-to-task-2", "case-insensitive"],
    )
    @mock.patch("xetl.models.command.Command.execute", return_value=0)
    def test_execute_job_skip_to(
        self,
        command_execute,
        skip_to,
        expected_executed_tasks,
        job_manifest_multiple_tasks_path,
    ):
        engine.execute_job(job_manifest_multiple_tasks_path, skip_to=skip_to)

        actual_executed_tasks = [
            (call.kwargs.get("task") or call.args[0]).name for call in command_execute.call_args_list
        ]
        assert actual_executed_tasks == expected_executed_tasks

    @mock.patch("xetl.models.command.Command.execute", return_value=0)
    def test_execute_job_skipped_tasks_still_resolve(self, command_execute, commands_fixtures_path, tmpdir):
        job_manifest = dedent(
            f"""
            name: Multiple job manifest
            data: /data
            commands: {commands_fixtures_path}
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
                  FILES: $data/files
                  SOURCE: ${{previous.OUTPUT}}
                  OUTPUT: /tmp/data1/splits
            """
        )
        engine.execute_job(job_file(job_manifest, tmpdir))
        assert command_execute.call_count == 1, "Command.execute() should have only been called once"
        executed_task = command_execute.call_args[0][0]
        assert executed_task.env["SOURCE"] == "/tmp/data1/source"
