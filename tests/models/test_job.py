import logging
import os
import re
from textwrap import dedent

import mock
import pytest
from pydantic import ValidationError

from xetl.models.job import Job
from xetl.models.utils.io import InvalidManifestError, ManifestLoadError, parse_yaml


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
        commands:
          - task: download
            env:
              {env}: http://example.com/data
        """
    )
    job = Job.from_yaml(manifest)

    assert "BASE_URL" in job.commands[0].env
    assert job.commands[0].env["BASE_URL"] == "http://example.com/data"


@pytest.mark.parametrize(
    "env_item",
    ["not-a-dict", "1", "null", "true", " - 1", "- foo: bar"],
)
def test_conform_env_invalid_values(env_item):
    manifest = dedent(
        f"""
        name: Single composed job manifest
        data: /data
        commands:
          - task: download
            env:
              {env_item}
        """
    )
    with pytest.raises(ValidationError) as exc:
        Job.from_yaml(manifest)
    assert "commands.0.env\n  Input should be a valid dictionary" in str(exc.value)


@mock.patch.dict("xetl.models.job.os.environ", {"VAR1": "host-var1-value", "VAR2": "host-var2-value"}, clear=True)
def test_host_env_default_inherit_defined():
    manifest = dedent(
        """
        name: Job does not inherit
        data: /data
        env:
          VAR1: job-var1-value
        commands: []
        """
    )
    job = Job.from_yaml(manifest)
    assert "VAR2" not in job.env, "Job should not inherit VAR2 since its not defined in `env`"
    assert job.env == {"VAR1": "host-var1-value"}, "Job should not inherit host env by default"


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
        commands: []
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
        commands: []
        """
    ).format(all=all)
    Job.from_yaml(manifest)

    assert (
        "The `*` value in `job.host_env` was specified alongside other values. All host environment variables will be inherited."
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
        commands: []
        """
    )
    job = Job.from_yaml(manifest)
    assert job.env.get("VAR1") == "host-var1-value", "VAR1 should have been loaded from the HOST env"
    assert "VAR2" not in job.env, "VAR2 should NOT have been loaded from the HOST env"


@mock.patch.dict("xetl.models.job.os.environ", {"VAR1": "host-var1-value", "VAR2": "host-var2-value"}, clear=True)
def test_host_env_not_used_warns(caplog):
    manifest = dedent(
        """
        name: Job does not inherit
        data: /data
        host-env:
          - NOT_SET
          - SET_BY_JOB
        env:
          SET_BY_JOB: set-by-job
        commands: []
        """
    )
    Job.from_yaml(manifest)
    assert (
        "SET_BY_JOB" not in caplog.text
    ), "Should not have logged a warning about SET_BY_JOB since it has a default value defined in `job.env`"
    assert (
        "xetl.models.job",
        logging.WARNING,
        "The following host environment variables did not receive a value: NOT_SET",
    ) in caplog.record_tuples


@mock.patch.dict("xetl.models.job.os.environ", {"VAR1": "host-var1-value", "VAR2": "host-var2-value"}, clear=True)
def test_host_env_overrides_job_env():
    manifest = dedent(
        """
        name: Job does not inherit
        data: /data
        host-env: "*"
        env:
          VAR1: job-var1-value
        commands: []
        """
    )
    job = Job.from_yaml(manifest)
    assert job.env.get("VAR1") == "host-var1-value", "VAR1 should have been overridden by the JOB env value"
    assert job.env.get("VAR2") == "host-var2-value", "VAR2 should have been loaded from the HOST env"


@pytest.mark.parametrize("host_env", ("[]", "null"))
@mock.patch.dict("xetl.models.job.os.environ", {"VAR1": "host-var1-value", "VAR2": "host-var2-value"}, clear=True)
def test_host_env_not_allowed(host_env):
    manifest = dedent(
        f"""
        name: Job does not inherit
        data: /data
        host-env: {host_env}
        env:
          VAR1: job-var1-value
        commands: []
        """
    )
    job = Job.from_yaml(manifest)
    assert job.env == {"VAR1": "job-var1-value"}, "All host env values should have been ignored"


@mock.patch.dict("xetl.models.job.os.environ", {"HOST_VAR": "host-var-value"}, clear=True)
def test_command_env_inherits_host_and_job_env():
    manifest = dedent(
        """
        name: Job does not inherit
        data: /data
        host-env:
          - HOST_VAR
        env:
          JOB_VAR: job-var-value
        commands:
          - task: task1
            env:
              STEP_VAR: command-var-value
        """
    )
    job = Job.from_yaml(manifest)
    assert (
        job.commands[0].env.get("HOST_VAR") == "host-var-value"
    ), "The HOST var should have been inherited by the command"
    assert (
        job.commands[0].env.get("JOB_VAR") == "job-var-value"
    ), "The JOB var should have been inherited by the command"
    assert (
        job.commands[0].env.get("STEP_VAR") == "command-var-value"
    ), "The STEP var should have been inherited by the command"


def test_command_env_names_are_conformed():
    manifest = dedent(
        """
        name: Job does not inherit
        data: /data
        commands:
          - task: task1
            env:
              var-one: 1
              var_two: 2
              VAR_THREE: 3
              VAR-FOUR: 4
              VarFive: 5
        """
    )
    job = Job.from_yaml(manifest)
    assert job.commands[0].env == {
        "VAR_ONE": 1,
        "VAR_TWO": 2,
        "VAR_THREE": 3,
        "VAR_FOUR": 4,
        "VARFIVE": 5,
    }, "The command env should have been conformed to uppercase and underscores"


@pytest.mark.parametrize(
    "command_name",
    ["has some spaces", "invalid@char", "invalid$char", "invalid&char", "invalid:char"],
)
def test_command_invalid_name_raises(command_name):
    manifest = dedent(
        f"""
        name: Job does not inherit
        data: /data
        commands:
          - task: task1
            name: {command_name}
        """
    )
    with pytest.raises(ValidationError) as exc:
        Job.from_yaml(manifest)
    assert (
        f"Command name '{command_name}' contains invalid characters. Only letters, numbers, dashes, and underscores are allowed."
        in str(exc.value)
    ), str(exc.value)


@pytest.mark.parametrize(
    "placeholder, resolved",
    [
        ("${VAR1}", "second-command-var1-value"),
        ("${Var1}", "second-command-var1-value"),
        ("${JOB_VAR}", "job-var-value"),
        ("${Job_var}", "job-var-value"),
        ("${Job-var}", "job-var-value"),
        ("${JOB-VAR}", "job-var-value"),
        ("${previous.env.VAR1}", "first-command-var1-value"),
        ("${previous.env.Var1}", "first-command-var1-value"),
        ("${previous.env.JOB_VAR}", "job-var-value"),
        ("${first-command.env.VAR1}", "first-command-var1-value"),
        ("${first_command.env.VAR1}", "first-command-var1-value"),
        ("${first-command.env.JOB_VAR}", "job-var-value"),
        ("~/relative/path/", "/User/username/relative/path/"),
        ("${job.basedir}", "/path/to/job"),
        ("${JOB.Env.VAR1}", "job-var1-value"),
        ("${job.commands.0.env.VAR1}", "first-command-var1-value"),
        ("${self.name}", "second-command"),
        ("${}", "${}"),
    ],
)
@mock.patch("xetl.models.job.os.path.expanduser", side_effect=fake_expanduser)
def test_resolve_placeholders(_, placeholder, resolved):
    manifest = dedent(
        f"""
        name: Single composed job manifest
        data: /data
        basedir: /path/to/job
        env:
          VAR1: job-var1-value
          JOB_VAR: job-var-value
        commands:
          - name: first-command
            task: task1
            env:
              VAR1: first-command-var1-value
              VAR_INT: 123
              VAR_FLOAT: 123.4
              VAR_BOOL: true
          - name: second-command
            task: task2
            env:
              VAR1: second-command-var1-value
              VAR2: {placeholder}
        """
    )
    job = Job.from_yaml(manifest)

    assert job.commands[1].env["VAR2"] == resolved


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
        commands:
          - name: first-command
            task: task1
            env:
              VAR: {placeholder}
        """
    )
    job = Job.from_yaml(manifest)

    assert job.commands[0].env["VAR"] == resolved


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
        ("'[${job.DATA}] *${VAR}* $$${JOB_VAR}$'", "[/data] *value* $job-var-value$"),
        ("'[${job.data}] *${VAR}* $$${JOB_VAR}$'", "[/data] *value* $job-var-value$"),
        ("'[${JOB.BASEDIR}] *${VAR}* $$${JOB_VAR}$'", "[/path/to/job] *value* $job-var-value$"),
        ("'[${job.basedir}] *${VAR}* $$${JOB_VAR}$'", "[/path/to/job] *value* $job-var-value$"),
    ],
)
def test_resolve_placeholders_complex_matches(placeholder, resolved, tmp_path):
    manifest = dedent(
        f"""
        name: Job with complex placeholder matches
        data: /data
        env:
          JOB_VAR: job-var-value
        commands:
          - name: first-command
            task: task1
            env:
              VAR: value
              PLACEHOLDER: {placeholder}
        """
    )
    job_dict = parse_yaml(manifest)
    job_dict["basedir"] = "/path/to/job"
    job = Job(**job_dict)

    assert job.commands[0].env["PLACEHOLDER"] == resolved


@pytest.mark.parametrize("null_value", ["null", "~"])
def test_resolve_placeholders_none_value(null_value):
    manifest = dedent(
        f"""
        name: Job with complex placeholder matches
        data: /data
        env:
          JOB_VAR: {null_value}
        commands:
          - name: first-command
            task: task1
            env:
              PLAIN: $JOB_VAR
              EMBEDDED: this is $JOB_VAR
        """
    )
    job = Job.from_yaml(manifest)

    assert job.commands[0].env["PLAIN"] is None
    assert job.commands[0].env["EMBEDDED"] == "this is null", "None should be converted to a string as 'null'"


@mock.patch.dict("xetl.models.job.os.environ", {"HOST_VAR": "host-var-value"}, clear=True)
def test_resolve_placeholders_unresolved_self_env_values():
    manifest = dedent(
        """
        name: Job with complex placeholder matches
        data: /resolved-data-path
        host-env: "*"
        env:
          JOB_VAR: job-var-value
        commands:
          - name: first-command
            task: task1
            env:
              VAR6: ${job.data}
              VAR5: "${VAR6}" # previously resolved variable
              VAR4: "${VAR4}" # self-referencing, not yet resolved
              VAR3: "${VAR2}" # later variable, not yet resolved
              VAR2: ${JOB_VAR}
              VAR1: ${HOST_VAR}
        """
    )
    job = Job.from_yaml(manifest)

    assert job.commands[0].env == {
        "JOB_VAR": "job-var-value",
        "HOST_VAR": "host-var-value",
        "VAR6": "/resolved-data-path",
        "VAR5": "/resolved-data-path",
        "VAR4": "${VAR4}",  # unresolved value
        "VAR3": "${JOB_VAR}",  # unresolved value
        "VAR2": "job-var-value",
        "VAR1": "host-var-value",
    }, "Only variables referencing other envs (job or host) are resolved"


def test_resolve_rejects_relative_data_dir_when_loaded_from_string():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: relative/data/path
        commands:
          - name: downloader
            task: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: ${job.data}/downloader/output
        """
    )
    with pytest.raises(ValueError) as exc:
        job = Job.from_yaml(manifest)
        print(f"job.data = {job.data}")
    assert "Relative paths cannot be used when the job manifest is loaded from a string: relative/data/path" in str(
        exc.value
    )


def test_resolve_rejects_relative_tasks_dir_when_loaded_from_string():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /absolute/data/path
        tasks:
          - /absolute/path/is/ok
          - relative/path/is/not/ok
        commands:
          - name: downloader
            task: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: ${job.data}/downloader/output
        """
    )
    with pytest.raises(ValueError) as exc:
        Job.from_yaml(manifest)
    assert (
        "Relative paths cannot be used when the job manifest is loaded from a string: relative/path/is/not/ok"
        in str(exc.value)
    )


def test_from_file_expands_relative_data_dir_to_file(tmp_path):
    manifest = dedent(
        """
        name: Single composed job manifest
        data: relative/data/path
        commands:
          - name: downloader
            task: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: ${job.data}/downloader/output
        """
    )
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    job_file = job_dir / "job.yml"
    job_file.write_text(manifest)
    job = Job.from_file(str(job_file))

    assert job.data == f"{job_dir}/relative/data/path"
    assert job.commands[0].env["OUTPUT"] == f"{job.data}/downloader/output"
    assert job.basedir == str(job_dir)


def test_from_file_expands_relative_tasks_dir_to_file(tmp_path):
    manifest = dedent(
        """
        name: Single composed job manifest
        data: relative/data/path
        tasks: relative/tasks
        commands: []
        """
    )
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    job_file = job_dir / "job.yml"
    job_file.write_text(manifest)
    job = Job.from_file(str(job_file))

    assert job.tasks == [f"{job_dir}/relative/tasks"]


def test_resolve_doesnt_expand_absolute_data_dir():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data/path
        commands:
          - name: downloader
            task: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: ${job.data}/downloader/output
        """
    )
    job = Job.from_yaml(manifest)

    assert job.data == "/data/path"
    assert job.commands[0].env["OUTPUT"] == f"{job.data}/downloader/output"


@mock.patch.dict("xetl.models.job.os.environ", {}, clear=True)
def test_resolve_job_env_with_default():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: "$DATA_PATH/path"  # resolved from env
        host-env:
          - DATA_PATH
        env:
          DATA_PATH: /data/job-env
        commands:
          - name: downloader
            task: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: ${job.data}/downloader/output
        """
    )
    job = Job.from_yaml(manifest)

    assert job.data == "/data/job-env/path", "should have resolved from the job's env"
    assert job.commands[0].env["OUTPUT"] == f"{job.data}/downloader/output"


@mock.patch.dict("xetl.models.job.os.environ", {"DATA_PATH": "/data/host-env"}, clear=True)
def test_resolve_job_env_from_host_env():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: $DATA_PATH/path
        host-env:
          - DATA_PATH
        env:
          DATA_PATH: /data/job-env
        commands:
          - name: downloader
            task: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: ${job.data}/downloader/output
        """
    )
    job = Job.from_yaml(manifest)

    assert job.data == "/data/host-env/path", "host env should have overridden the job's env"
    assert job.commands[0].env["OUTPUT"] == f"{job.data}/downloader/output"


def test_resolve_unknown_env_variable_no_vars_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        commands:
          - name: downloader
            task: ${unknown.something}
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
              Value error, Invalid name `unknown` in `${unknown.something}`. The first name must be one of:
             - variable name in the current command's env: No env variables defined
             - name of a previous command: No previous commands defined
             - `self` to reference the current command (e.g. ${self.name})
             - `job` to reference the Job (e.g. ${job.data})
             - `previous` to reference the previous command (e.g. ${previous.OUTPUT})
             - `tmp.dir` to create a temporary directory
             - `tmp.file` to create a temporary file
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
        commands:
          - name: downloader
            task: download
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
              Value error, Invalid name `unknown` in `$unknown`. The first name must be one of:
             - variable name in the current command's env: JOB_VAR, VAR1, VAR2
             - name of a previous command: No previous commands defined
             - `self` to reference the current command (e.g. ${self.name})
             - `job` to reference the Job (e.g. ${job.data})
             - `previous` to reference the previous command (e.g. ${previous.OUTPUT})
             - `tmp.dir` to create a temporary directory
             - `tmp.file` to create a temporary file
            """
        ).strip()
    ), str(exc_info.value)


def test_resolve_unknown_env_variable_no_current_env_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        commands:
          - name: first
            task: first
            env:
              VAR1: http://example.com/data
          - name: second
            task: $unknown
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
              Value error, Invalid name `unknown` in `$unknown`. The first name must be one of:
             - variable name in the current command's env: No env variables defined
             - name of a previous command: first
             - `self` to reference the current command (e.g. ${self.name})
             - `job` to reference the Job (e.g. ${job.data})
             - `previous` to reference the previous command (e.g. ${previous.OUTPUT})
             - `tmp.dir` to create a temporary directory
             - `tmp.file` to create a temporary file
            """
        ).strip()
    ), str(exc_info.value)


def test_resolve_unknown_env_variable_with_previous_and_current_env_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        env:
          JOB_VAR: job-var-value
        commands:
          - name: first
            task: first
            env:
              VAR1: http://example.com/data
          - name: second
            task: second
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
              Value error, Invalid name `unknown` in `$unknown`. The first name must be one of:
             - variable name in the current command's env: JOB_VAR, VAR1, VAR2
             - name of a previous command: first
             - `self` to reference the current command (e.g. ${self.name})
             - `job` to reference the Job (e.g. ${job.data})
             - `previous` to reference the previous command (e.g. ${previous.OUTPUT})
             - `tmp.dir` to create a temporary directory
             - `tmp.file` to create a temporary file
            """
        ).strip()
    ), str(exc_info.value)


def test_resolve_incomplete_variable_path_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        commands:
          - name: downloader1
            task: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: ${job.data}/foo
          - name: downloader2
            task: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: ${previous} # missing env key
        """
    )
    with pytest.raises(ValueError) as exc_info:
        Job.from_yaml(manifest)
    assert (
        "Incomplete key path, variable must reference a leaf value: `${previous}` -- did you forget to wrap the variable names in curly braces?"
        in str(exc_info.value)
    )


def test_resolve_too_many_variable_paths_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        commands:
          - name: downloader1
            task: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: ${job.data}/foo
          - name: downloader2
            task: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: ${previous.env.OUTPUT.something} # too long!
        """
    )
    with pytest.raises(ValueError) as exc_info:
        Job.from_yaml(manifest)
    assert (
        "Invalid placeholder in ${previous.env.OUTPUT.something}. Could not drill in beyond `output` as it does not refer to an object or a list."
        in str(exc_info.value)
    )


def test_resolve_tmp_dir(tmpdir):
    data_path = str(tmpdir.mkdir("data"))
    manifest = dedent(
        f"""
        name: Single composed job manifest
        data: {data_path}
        commands:
          - name: downloader
            task: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: ${{tmp.dir}}
          - name: splitter
            task: split
            env:
              FOO: ${{previous.env.OUTPUT}}
              OUTPUT: ${{tmp.dir}}
        """
    )
    job = Job.from_yaml(manifest)

    assert all(
        isinstance(command.env["OUTPUT"], str) and command.env["OUTPUT"].startswith(data_path + "/tmp/")
        for command in job.commands
    ), f"All commands should output to a tmp directory: {[t.env['output'] for t in job.commands]}"
    assert all(os.path.isdir(command.env["OUTPUT"]) for command in job.commands), "Each output should be a directory"  # type: ignore
    assert job.commands[0].env["OUTPUT"] != job.commands[1].env["OUTPUT"], "Every tmp value should be a different value"
    assert job.commands[1].env["FOO"] == job.commands[0].env["OUTPUT"], "References to tmp dir should be the same value"


def test_resolve_tmp_file(tmpdir):
    data_path = str(tmpdir.mkdir("data"))
    manifest = dedent(
        f"""
        name: Single composed job manifest
        data: {data_path}
        commands:
          - name: downloader
            task: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: ${{tmp.file}}
          - name: splitter
            task: split
            env:
              FOO: ${{previous.env.OUTPUT}}
              OUTPUT: ${{tmp.file}}
        """
    )
    job = Job.from_yaml(manifest)

    assert all(
        str(command.env["OUTPUT"]).startswith(data_path + "/tmp/") for command in job.commands
    ), "All commands should output to a tmp directory"
    assert all(os.path.isfile(str(command.env["OUTPUT"])) for command in job.commands), "Each output should be a file"
    assert job.commands[0].env["OUTPUT"] != job.commands[1].env["OUTPUT"], "Every tmp value should be a different value"
    assert job.commands[1].env["FOO"] == job.commands[0].env["OUTPUT"], "References to tmp dir should be the same value"


def test_resolve_tmp_unknown(tmpdir):
    data_path = str(tmpdir.mkdir("data"))
    manifest = dedent(
        f"""
        name: Single composed job manifest
        data: {data_path}
        commands:
          - name: downloader
            task: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: ${{tmp.unknown}}
        """
    )
    with pytest.raises(ValueError) as exc_info:
        Job.from_yaml(manifest)
    assert "Invalid use of ${tmp} placeholder in `${tmp.unknown}`. Expected `tmp.dir` or `tmp.file`" in str(
        exc_info.value
    )


def test_resolve_variable_previous_unknown_variable_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        commands:
          - name: downloader
            task: download
            env:
              BASE_URL: http://example.com/data
              OUTPUT: /data/output1
          - name: splitter
            task: split
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
        "Valid keys are: `description`, `env`, `name`, `skip`, `task`"
    ), str(exc_info.value)


def test_resolve_variable_previous_output_first_command_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        commands:
          - name: splitter
            task: split
            env:
              FOO: ${previous.env.output}
              OUTPUT: /data/output
        """
    )

    with pytest.raises(Exception) as exc_info:
        Job.from_yaml(manifest)
    assert "Cannot use ${previous} placeholder on the first command" in str(exc_info.value)


def test_resolve_variable_chained_placeholders():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        commands:
          - name: downloader1
            task: download
            env:
              BASE_URL: http://example.com${job.data}
              OUTPUT: /tmp/data/d1
          - name: downloader2
            task: download
            env:
              BASE_URL: ${downloader1.env.base_url}
              OUTPUT: /tmp/data/d2
          - name: downloader3
            task: download
            env:
              BASE_URL: ${downloader2.env.base_url}
              OUTPUT: /tmp/data/d3
    """
    )

    job = Job.from_yaml(manifest)

    actual_base_urls = [command.env["BASE_URL"] for command in job.commands]
    assert actual_base_urls == ["http://example.com/data"] * 3


def test_resolve_variable_circular_placeholders_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        commands:
          - name: downloader1
            task: download
            env:
              BASE_URL: http://example.com${job.DATA}
              OUTPUT: ${downloader2.env.output}
          - name: downloader2
            task: download
            env:
              BASE_URL: http://example.com${job.DATA}
              OUTPUT: ${downloader1.env.output}
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
              Value error, Invalid name `downloader2` in `${downloader2.env.output}`. The first name must be one of:
             - variable name in the current command's env: BASE_URL, OUTPUT
             - name of a previous command: No previous commands defined
             - `self` to reference the current command (e.g. ${self.name})
             - `job` to reference the Job (e.g. ${job.data})
             - `previous` to reference the previous command (e.g. ${previous.OUTPUT})
             - `tmp.dir` to create a temporary directory
             - `tmp.file` to create a temporary file
            """
        ).strip()
    )
