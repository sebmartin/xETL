import os
from textwrap import dedent
import pytest
import mock

from xetl import engine
from xetl.models.command import Command
from xetl.models.task import Task, TaskFailure, UnknownTaskError


def job_file(job_yaml: str, tmpdir):
    path = os.path.join(tmpdir, "job.yml")
    with open(path, "w") as fd:
        fd.write(job_yaml)
    return path


@pytest.fixture(autouse=True)
def mock_subprocess_run():
    with mock.patch("subprocess.run", mock.Mock()) as mock_run:
        yield mock_run


@mock.patch("xetl.models.task.Task.execute", return_value=0, autospec=True)
def test_execute_job_simple_job(task_execute, job_manifest_simple_path):
    engine.execute_job(job_manifest_simple_path)

    comands_and_commands = [
        f"task: {call.args[0].name}, command: {call.args[1].name}, dryrun: {call.args[2]}"
        for call in task_execute.call_args_list
    ]
    assert comands_and_commands == [
        "task: download, command: Download, dryrun: False",
    ]


@mock.patch("xetl.models.task.Task.execute", return_value=0, autospec=True)
def test_execute_job_multiple_commands(task_execute, job_manifest_multiple_commands_path, tasks_fixtures_path, tmpdir):
    engine.execute_job(job_manifest_multiple_commands_path)

    comands_and_commands = [
        f"task: {call.args[0].name}, command: {call.args[1].name}, dryrun: {call.args[2]}"
        for call in task_execute.call_args_list
    ]
    assert comands_and_commands == [
        "task: download, command: Download, dryrun: False",
        "task: splitter, command: Splitter, dryrun: False",
    ]


@mock.patch("xetl.models.task.Task.execute", return_value=127)
def test_execute_job_stops_if_command_fails(
    task_execute, job_manifest_multiple_commands_path, tasks_fixtures_path, tmpdir
):
    with pytest.raises(TaskFailure) as excinfo:
        engine.execute_job(job_manifest_multiple_commands_path)

    assert task_execute.call_count == 1, "Task.execute() should have only been called once"
    assert excinfo.value.returncode == 127, "The exception should contain the return code of the failed task"


@mock.patch("xetl.models.task.Task.execute")
def test_execute_job_without_tasks_path_warns(execute_task, tmpdir, caplog):
    manifest = dedent(
        """
        name: Job without manifests
        data: /data
        commands: []
        """
    )
    engine.execute_job(job_file(manifest, str(tmpdir)))
    assert "The property `tasks` is not defined in the job manifest, no tasks will be available" in caplog.messages


@mock.patch("xetl.models.task.Task.execute")
def test_execute_job_no_tasks_found(execute_task, tmpdir, caplog):
    manifest = dedent(
        """
        name: Job without manifests
        data: /data
        tasks: /tmp/does-not-exist
        commands: []
        """
    )
    engine.execute_job(job_file(manifest, str(tmpdir)))
    assert "Could not find any tasks at paths ['/tmp/does-not-exist']" in caplog.messages


@mock.patch("xetl.models.task.Task.execute", return_value=127)
def test_execute_job_with_unknown_task(task_execute, job_manifest_simple, tasks_fixtures_path, tmpdir):
    manifest = job_manifest_simple.replace("task: download", "task: unknown")
    with pytest.raises(UnknownTaskError) as excinfo:
        engine.execute_job(job_file(manifest, tmpdir))

    assert str(excinfo.value) == "Unknown task `unknown`, should be one of: ['download', 'parser', 'splitter']"
    task_execute.assert_not_called()


@pytest.mark.parametrize(
    "skip_to, expected_executed_commands",
    [
        (None, ["Download", "Splitter"]),
        ("download", ["Download", "Splitter"]),
        ("splitter", ["Splitter"]),
        ("SPLITTER", ["Splitter"]),
    ],
    ids=["not-set", "skip-to-command-1", "skip-to-command-2", "case-insensitive"],
)
@mock.patch("xetl.models.task.Task.execute", return_value=0)
def test_execute_job_skip_to(
    task_execute,
    skip_to,
    expected_executed_commands,
    job_manifest_multiple_commands_path,
):
    engine.execute_job(job_manifest_multiple_commands_path, skip_to=skip_to)

    actual_executed_commands = [
        (call.kwargs.get("command") or call.args[0]).name for call in task_execute.call_args_list
    ]
    assert actual_executed_commands == expected_executed_commands


@mock.patch("xetl.models.task.Task.execute", return_value=0)
def test_execute_job_skipped_commands_still_resolve(task_execute, tasks_fixtures_path, tmpdir):
    job_manifest = dedent(
        f"""
        name: Multiple job manifest
        data: /data
        tasks: {tasks_fixtures_path}
        commands:
          - name: skipped
            task: download
            skip: true
            env:
              BASE_URL: http://example.com/data1
              THROTTLE: 1000
              OUTPUT: /tmp/data1/source
          - name: references-skipped
            task: splitter
            env:
              FILES: $data/files
              SOURCE: ${{previous.OUTPUT}}
              OUTPUT: /tmp/data1/splits
        """
    )
    engine.execute_job(job_file(job_manifest, tmpdir))
    assert task_execute.call_count == 1, "Task.execute() should have only been called once"
    executed_command = task_execute.call_args[0][0]
    assert executed_command.env["SOURCE"] == "/tmp/data1/source"


@mock.patch("xetl.models.task.Task.validate_inputs", side_effect=ValueError("Invalid inputs"))
def test_execute_validates_before_executing(
    validate_inputs,
    mock_subprocess_run,
    job_manifest_multiple_commands_path,
):
    with pytest.raises(ValueError):
        engine.execute_job(job_manifest_multiple_commands_path)
    mock_subprocess_run.assert_not_called()
