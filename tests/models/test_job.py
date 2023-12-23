import os
import re
from textwrap import dedent

import mock
from pydantic import ValidationError
import pytest

from xetl.models.job import Job
from xetl.models.utils.io import InvalidManifestError, ManifestLoadError


def fake_expanduser(path):
    return re.sub(r"^~", "/User/username", path)


def fake_abspath(path):
    return f"/absolute/path/to/{path}"


def test_job_from_file(job_manifest_simple_path):
    assert isinstance(Job.from_file(job_manifest_simple_path), Job)


def test_job_from_file_not_found(tmpdir):
    job_file = tmpdir / "not-found" / "job.yml"
    with pytest.raises(ManifestLoadError) as exc:
        Job.from_file(str(job_file))
    assert str(exc.value) == f"Failed to load file; [Errno 2] No such file or directory: '{job_file}'"


@pytest.mark.parametrize(
    "value, error",
    [
        (
            "a string",
            "Failed to parse YAML, expected a dictionary",
        ),
        (
            b"\x00",
            "Failed to parse YAML; unacceptable character #x0000: special characters are not allowed\n"
            '  in "<unicode string>", position 0',
        ),
    ],
)
def test_job_from_file_invalid_yaml(value, error, tmpdir):
    job_file = tmpdir / "job.yml"
    job_file.write(value)
    with pytest.raises(ManifestLoadError) as exc:
        Job.from_file(str(job_file))
    assert str(exc.value) == "Error while parsing YAML at path: {path}; {error}".format(path=job_file, error=error)


@pytest.mark.parametrize(
    "value, error",
    [
        ("a string", "Failed to parse YAML, expected a dictionary"),
        (
            b"\x00",
            'Failed to parse YAML; unacceptable character #x0000: special characters are not allowed\n  in "<byte string>", position 0',
        ),
    ],
)
def test_job_from_yaml_invalid_yaml(value, error, tmpdir):
    with pytest.raises(InvalidManifestError) as exc:
        Job.from_yaml(value)
    assert str(exc.value) == error


@pytest.mark.parametrize("env", ["BASE_URL", "base-url", "Base_Url", "base_url"])
def test_conform_env_keys(env):
    manifest = dedent(
        f"""
        name: Single composed job manifest
        data: /data
        tasks:
          - command: download
            env:
              {env}: http://example.com/data
        """
    )
    job = Job.from_yaml(manifest)

    assert "BASE_URL" in job.tasks[0].env
    assert job.tasks[0].env["BASE_URL"] == "http://example.com/data"


@pytest.mark.parametrize(
    "env_item",
    ["not-a-dict", "1", "null", "true", " - 1", "- foo: bar"],
)
def test_conform_env_invalid_values(env_item):
    manifest = dedent(
        f"""
        name: Single composed job manifest
        data: /data
        tasks:
          - command: download
            env:
              {env_item}
        """
    )
    with pytest.raises(ValidationError) as exc:
        Job.from_yaml(manifest)
    assert "tasks.0.env\n  Input should be a valid dictionary" in str(exc.value)


@mock.patch.dict("xetl.models.job.os.environ", {"VAR1": "host-var1-value", "VAR2": "host-var2-value"}, clear=True)
def test_host_env_default_dont_inherit():
    manifest = dedent(
        """
        name: Job does not inherit
        data: /data
        tasks: []
        """
    )
    job = Job.from_yaml(manifest)
    assert job.env == {}, "Job should not inherit host env by default"


@pytest.mark.parametrize("all", ["'*'", "\n - '*'", "\n - V1\n - '*'"])
@mock.patch.dict("xetl.models.job.os.environ", {"VAR1": "host-var1-value", "VAR2": "host-var2-value"}, clear=True)
def test_host_env_inherit_all(all):
    manifest = dedent(
        """
        name: Job does not inherit
        data: /data
        host-env: {all}
        env:
          VAR3: job-var3-value
        tasks: []
        """
    ).format(all=all)
    job = Job.from_yaml(manifest)
    assert job.env.get("VAR1") == "host-var1-value", "VAR1 should be set to the HOST env value"
    assert job.env.get("VAR2") == "host-var2-value", "VAR1 should be set to the HOST env value"
    assert job.env.get("VAR3") == "job-var3-value", "VAR1 should be set to the JOB env value"


def test_host_env_inherit_all_mixed_warns(caplog):
    caplog.set_level("WARNING")
    manifest = dedent(
        """
        name: Job does not inherit
        data: /data
        host-env:
          - VAR1
          - '*'
        tasks: []
        """
    ).format(all=all)
    Job.from_yaml(manifest)

    assert (
        "The `*` value in `host-env` was specified alongside other values. All host environment variables will be inherited."
        in caplog.text
    ), "Should have logged a warning about mixing '*' with other values"


@mock.patch.dict("xetl.models.job.os.environ", {"VAR1": "host-var1-value", "VAR2": "host-var2-value"}, clear=True)
def test_host_env_subset():
    manifest = dedent(
        """
        name: Job does not inherit
        data: /data
        host-env:
          - VAR1
        tasks: []
        """
    )
    job = Job.from_yaml(manifest)
    assert job.env.get("VAR1") == "host-var1-value", "VAR1 should have been loaded from the HOST env"
    assert job.env.get("VAR2") == None, "VAR2 should NOT have been loaded from the HOST env"


@mock.patch.dict("xetl.models.job.os.environ", {"VAR1": "host-var1-value", "VAR2": "host-var2-value"}, clear=True)
def test_host_env_job_overrides_host_env():
    manifest = dedent(
        """
        name: Job does not inherit
        data: /data
        host-env: "*"
        env:
          VAR1: job-var1-value
        tasks: []
        """
    )
    job = Job.from_yaml(manifest)
    assert job.env.get("VAR1") == "job-var1-value", "VAR1 should have been overridden by the JOB env value"
    assert job.env.get("VAR2") == "host-var2-value", "VAR2 should have been loaded from the HOST env"


@mock.patch.dict("xetl.models.job.os.environ", {"HOST_VAR": "host-var-value"}, clear=True)
def test_task_env_inherits_host_and_job_env():
    manifest = dedent(
        f"""
        name: Job does not inherit
        data: /data
        host-env:
          - HOST_VAR
        env:
          JOB_VAR: job-var-value
        tasks:
          - command: command1
            env:
              STEP_VAR: task-var-value
        """
    )
    job = Job.from_yaml(manifest)
    assert job.tasks[0].env.get("HOST_VAR") == "host-var-value", "The HOST var should have been inherited by the task"
    assert job.tasks[0].env.get("JOB_VAR") == "job-var-value", "The JOB var should have been inherited by the task"
    assert job.tasks[0].env.get("STEP_VAR") == "task-var-value", "The STEP var should have been inherited by the task"


@pytest.mark.parametrize(
    "placeholder, resolved",
    [
        ("${VAR1}", "second-task-var1-value"),
        ("${Var1}", "second-task-var1-value"),
        ("${JOB_VAR}", "job-var-value"),
        ("${Job_var}", "job-var-value"),
        ("${Job-var}", "job-var-value"),
        ("${JOB-VAR}", "job-var-value"),
        ("${previous.VAR1}", "first-task-var1-value"),
        ("${previous.Var1}", "first-task-var1-value"),
        ("${previous.JOB_VAR}", "job-var-value"),
        ("${first-task.VAR1}", "first-task-var1-value"),
        ("${first_task.VAR1}", "first-task-var1-value"),
        ("${first-task.JOB_VAR}", "job-var-value"),
        ("~/relative/path/", "/User/username/relative/path/"),
    ],
)
@mock.patch("xetl.models.job.os.path.expanduser", side_effect=fake_expanduser)
def test_resolve_placeholders(_, placeholder, resolved):
    manifest = dedent(
        f"""
        name: Single composed job manifest
        data: /data
        env:
          VAR1: job-var1-value
          JOB_VAR: job-var-value
        tasks:
          - name: first-task
            command: command1
            env:
              VAR1: first-task-var1-value
              VAR_INT: 123
              VAR_FLOAT: 123.4
              VAR_BOOL: true
          - name: second-task
            command: command2
            env:
              VAR1: second-task-var1-value
              VAR2: {placeholder}
        """
    )
    job = Job.from_yaml(manifest)

    assert job.tasks[1].env["VAR2"] == resolved


@pytest.mark.parametrize(
    "placeholder, resolved",
    [
        ("${VAR_INT}", 123),
        ("${VAR_FLOAT}", 123.4),
        ("${VAR_BOOL}", True),
        ("'text: ${VAR_INT}'", "text: 123"),
        ("'text: ${VAR_FLOAT}'", "text: 123.4"),
        ("'text: ${VAR_BOOL}'", "text: True"),
    ],
)
def test_resolve_placeholders_non_string_types(placeholder, resolved):
    manifest = dedent(
        f"""
        name: Job with non-string variable values
        data: /data
        env:
          VAR_INT: 123
          VAR_FLOAT: 123.4
          VAR_BOOL: true
        tasks:
          - name: first-task
            command: command1
            env:
              VAR: {placeholder}
        """
    )
    job = Job.from_yaml(manifest)

    assert job.tasks[0].env["VAR"] == resolved


@pytest.mark.parametrize(
    "placeholder, resolved",
    [
        ("'[${VAR}$vAr]'", "[valuevalue]"),
        ("${VAR}${var}", "valuevalue"),
        ("'[${var}]'", "[value]"),
        ("$var$job-var", "valuejob-var-value"),
        ("${VAR}/${JOB_VAR}", "value/job-var-value"),
        ("$VAR/$$$JOB_VAR", "value/$job-var-value"),
        ("$$$VAR/$$$JOB_VAR/$$", "$value/$job-var-value/$"),
        ("$$$${VAR}", "$${VAR}"),
        ("$$$$VAR", "$$VAR"),
        ("$$${VAR}", "$value"),
        # the usecases below are specially crafted to have the placeholder `${VAR}` be 1 character
        # more than the `value` so that the logic could get confused with the adjacent literal `$`
        ("${VAR}/$$${JOB_VAR}", "value/$job-var-value"),
        ("${VAR}//$${JOB_VAR}", "value//${JOB_VAR}"),
        ("'[$data] *${VAR}* $$${JOB_VAR}$'", "[/data] *value* $job-var-value$"),
    ],
)
def test_resolve_placeholders_complex_matches(placeholder, resolved):
    manifest = dedent(
        f"""
        name: Job with complex placeholder matches
        data: /data
        env:
          JOB_VAR: job-var-value
        tasks:
          - name: first-task
            command: command1
            env:
              VAR: value
              PLACEHOLDER: {placeholder}
        """
    )
    job = Job.from_yaml(manifest)

    assert job.tasks[0].env["PLACEHOLDER"] == resolved


@pytest.mark.parametrize("null_value", ["null", "~"])
def test_resolve_placeholders_none_value(null_value):
    manifest = dedent(
        f"""
        name: Job with complex placeholder matches
        data: /data
        env:
          JOB_VAR: {null_value}
        tasks:
          - name: first-task
            command: command1
            env:
              PLAIN: $JOB_VAR
              EMBEDDED: this is $JOB_VAR
        """
    )
    job = Job.from_yaml(manifest)

    assert job.tasks[0].env["PLAIN"] == None
    assert job.tasks[0].env["EMBEDDED"] == "this is null", "None should be converted to a string as 'null'"


@mock.patch.dict("xetl.models.job.os.environ", {"HOST_VAR": "host-var-value"}, clear=True)
def test_resolve_placeholders_recursive_matches():
    manifest = dedent(
        """
        name: Job with complex placeholder matches
        data: /resolved-data-path
        host-env: "*"
        env:
          JOB_VAR: job-var-value
        tasks:
          - name: first-task
            command: command1
            env:
              VAR1: $data
              VAR2: "${VAR1}" # would-be-resolved variable
              VAR3: "${VAR3}" # self-referencing
              VAR4: "${VAR5}" # yet-to-be-resolved variable
              VAR5: ${JOB_VAR}
              VAR6: ${HOST_VAR}
        """
    )
    job = Job.from_yaml(manifest)

    assert job.tasks[0].env == {
        "JOB_VAR": "job-var-value",
        "HOST_VAR": "host-var-value",
        "VAR1": "/resolved-data-path",
        "VAR2": "$data",  # pre-resolution value
        "VAR3": "${VAR3}",  # pre-resolution value
        "VAR4": "${JOB_VAR}",  # pre-resolution value
        "VAR5": "job-var-value",
        "VAR6": "host-var-value",
    }, "Only variables referencing other envs (job or host) are resolved"


@mock.patch("xetl.models.job.os.path.abspath", side_effect=fake_abspath)
def test_resolve_placeholders_expands_relative_data_dir(_):
    manifest = dedent(
        """
        name: Single composed job manifest
        data: relative/data/path
        tasks:
          - name: downloader
            command: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: $data/downloader/output
        """
    )
    job = Job.from_yaml(manifest)

    assert job.data == "/absolute/path/to/relative/data/path"
    assert job.tasks[0].env["OUTPUT"] == f"{job.data}/downloader/output"


def test_resolve_doesnt_expand_absolute_data_dir():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data/path
        tasks:
          - name: downloader
            command: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: $data/downloader/output
        """
    )
    job = Job.from_yaml(manifest)

    assert job.data == "/data/path"
    assert job.tasks[0].env["OUTPUT"] == f"{job.data}/downloader/output"


def test_resolve_unknown_env_variable_no_vars_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        tasks:
          - name: downloader
            command: ${unknown.something}
        """
    )
    with pytest.raises(ValueError) as exc_info:
        Job.from_yaml(manifest)

    actual_error = str(exc_info.value).split(" [type=value_error")[0]
    assert (
        actual_error
        == dedent(
            """
            1 validation error for Job
              Value error, Invalid name `unknown` in `${unknown.something}`. The first must be one of:
             - variable in the current task's env: No env variables defined
             - name of a previous task: No previous tasks defined
            """
        ).strip()
    ), str(exc_info.value)


def test_resolve_unknown_env_variable_no_previous_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        env:
          JOB_VAR: job-var-value
        tasks:
          - name: downloader
            command: download
            env:
              VAR1: http://example.com/data
              VAR2: $unknown/foo/bar/baz
        """
    )
    with pytest.raises(ValueError) as exc_info:
        Job.from_yaml(manifest)

    actual_error = str(exc_info.value).split(" [type=value_error")[0]
    assert (
        actual_error
        == dedent(
            """
            1 validation error for Job
              Value error, Invalid name `unknown` in `$unknown`. The first must be one of:
             - variable in the current task's env: JOB_VAR, VAR1, VAR2
             - name of a previous task: No previous tasks defined
            """
        ).strip()
    ), str(exc_info.value)


def test_resolve_unknown_env_variable_no_current_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        tasks:
          - name: first
            command: first
            env:
              VAR1: http://example.com/data
          - name: second
            command: $unknown
        """
    )
    with pytest.raises(ValueError) as exc_info:
        Job.from_yaml(manifest)

    actual_error = str(exc_info.value).split(" [type=value_error")[0]
    assert (
        actual_error
        == dedent(
            """
            1 validation error for Job
              Value error, Invalid name `unknown` in `$unknown`. The first must be one of:
             - variable in the current task's env: No env variables defined
             - name of a previous task: first, previous
            """
        ).strip()
    ), str(exc_info.value)


def test_resolve_unknown_env_variable_with_previous_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        env:
          JOB_VAR: job-var-value
        tasks:
          - name: first
            command: first
            env:
              VAR1: http://example.com/data
          - name: second
            command: second
            env:
              VAR1: http://example.com/data
              VAR2: $unknown/foo/bar/baz
        """
    )
    with pytest.raises(ValueError) as exc_info:
        Job.from_yaml(manifest)

    actual_error = str(exc_info.value).split(" [type=value_error")[0]
    assert (
        actual_error
        == dedent(
            """
            1 validation error for Job
              Value error, Invalid name `unknown` in `$unknown`. The first must be one of:
             - variable in the current task's env: JOB_VAR, VAR1, VAR2
             - name of a previous task: first, previous
            """
        ).strip()
    ), str(exc_info.value)


def test_resolve_incomplete_variable_path_raises():
    manifest = dedent(
        f"""
        name: Single composed job manifest
        data: /data
        tasks:
          - name: downloader1
            command: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: $data/foo
          - name: downloader2
            command: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: ${{previous}} # missing env key
        """
    )
    with pytest.raises(ValueError) as exc_info:
        Job.from_yaml(manifest)
    assert (
        "Incomplete key path, variable must reference a leaf value: `${previous}` -- did you forget to wrap the variable names in curly braces?"
        in str(exc_info.value)
    )


def test_resolve_tmp_dir(tmpdir):
    data_path = str(tmpdir.mkdir("data"))
    manifest = dedent(
        f"""
        name: Single composed job manifest
        data: {data_path}
        tasks:
          - name: downloader
            command: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: ${{tmp.dir}}
          - name: splitter
            command: split
            env:
              FOO: ${{previous.OUTPUT}}
              OUTPUT: ${{tmp.dir}}
        """
    )
    job = Job.from_yaml(manifest)

    assert all(
        isinstance(task.env["OUTPUT"], str) and task.env["OUTPUT"].startswith(data_path + "/tmp/") for task in job.tasks
    ), f"All tasks should output to a tmp directory: {[t.env['output'] for t in job.tasks]}"
    assert all(os.path.isdir(task.env["OUTPUT"]) for task in job.tasks), "Each output should be a directory"  # type: ignore
    assert job.tasks[0].env["OUTPUT"] != job.tasks[1].env["OUTPUT"], "Every tmp value should be a different value"
    assert job.tasks[1].env["FOO"] == job.tasks[0].env["OUTPUT"], "References to tmp dir should be the same value"


def test_resolve_tmp_file(tmpdir):
    data_path = str(tmpdir.mkdir("data"))
    manifest = dedent(
        f"""
        name: Single composed job manifest
        data: {data_path}
        tasks:
          - name: downloader
            command: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: ${{tmp.file}}
          - name: splitter
            command: split
            env:
              FOO: ${{previous.OUTPUT}}
              OUTPUT: ${{tmp.file}}
        """
    )
    job = Job.from_yaml(manifest)

    assert all(
        str(task.env["OUTPUT"]).startswith(data_path + "/tmp/") for task in job.tasks
    ), "All tasks should output to a tmp directory"
    assert all(os.path.isfile(str(task.env["OUTPUT"])) for task in job.tasks), "Each output should be a directory"
    assert job.tasks[0].env["OUTPUT"] != job.tasks[1].env["OUTPUT"], "Every tmp value should be a different value"
    assert job.tasks[1].env["FOO"] == job.tasks[0].env["OUTPUT"], "References to tmp file should be the same value"


def test_resolve_variable_previous_unknown_variable_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        tasks:
          - name: downloader
            command: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: /data/output1
          - name: splitter
            command: split
            env:
              FOO: ${previous.unknown}
              OUTPUT: /data/output2
        """
    )

    with pytest.raises(ValueError) as exc_info:
        Job.from_yaml(manifest)
    actual_error = str(exc_info.value).split(" [type=value_error")[0]
    assert actual_error.strip() == (
        "1 validation error for Job\n  Value error, Invalid placeholder `unknown` in ${previous.unknown}. "
        "Valid keys are: `BASE_URL`, `OUTPUT`"
    ), str(exc_info.value)


def test_resolve_variable_previous_output_first_task_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        tasks:
          - name: splitter
            command: split
            env:
              FOO: ${previous.output}
              OUTPUT: /data/output
        """
    )

    with pytest.raises(Exception) as exc_info:
        Job.from_yaml(manifest)
    assert "Cannot use $previous placeholder on the first task" in str(exc_info.value)


def test_resolve_variable_chained_placeholders():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        tasks:
          - name: downloader1
            command: download
            env:
              BASE_URL: http://example.com$data
              OUTPUT: /tmp/data/d1
          - name: downloader2
            command: download
            env:
              BASE_URL: ${downloader1.base_url}
              OUTPUT: /tmp/data/d2
          - name: downloader3
            command: download
            env:
              BASE_URL: ${downloader2.base_url}
              OUTPUT: /tmp/data/d3
    """
    )

    job = Job.from_yaml(manifest)

    actual_base_urls = [task.env["BASE_URL"] for task in job.tasks]
    assert actual_base_urls == ["http://example.com/data"] * 3


def test_resolve_variable_circular_placeholders_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        tasks:
          - name: downloader1
            command: download
            env:
              BASE_URL: http://example.com$data
              OUTPUT: ${downloader2.output}
          - name: downloader2
            command: download
            env:
              BASE_URL: http://example.com$data
              OUTPUT: ${downloader1.output}
    """
    )

    with pytest.raises(Exception) as exc:
        Job.from_yaml(manifest)
    actual_message = str(exc.value).split(" [type=value_error")[0]
    assert (
        actual_message
        == dedent(
            """
            1 validation error for Job
              Value error, Invalid name `downloader2` in `${downloader2.output}`. The first must be one of:
             - variable in the current task's env: BASE_URL, OUTPUT
             - name of a previous task: No previous tasks defined
            """
        ).strip()
    )
