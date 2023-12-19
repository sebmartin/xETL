import os
import re
from textwrap import dedent

import mock
from pydantic import ValidationError
import pytest

from xetl.models.app import App
from xetl.models.utils.io import InvalidManifestError, ManifestLoadError


def fake_expanduser(path):
    return re.sub(r"^~", "/User/username", path)


def fake_abspath(path):
    return f"/absolute/path/to/{path}"


def test_app_from_file(app_manifest_simple_path):
    assert isinstance(App.from_file(app_manifest_simple_path), App)


def test_app_from_file_not_found(tmpdir):
    app_file = tmpdir / "not-found" / "app.yml"
    with pytest.raises(ManifestLoadError) as exc:
        App.from_file(str(app_file))
    assert str(exc.value) == f"Failed to load file; [Errno 2] No such file or directory: '{app_file}'"


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
def test_app_from_file_invalid_yaml(value, error, tmpdir):
    app_file = tmpdir / "app.yml"
    app_file.write(value)
    with pytest.raises(ManifestLoadError) as exc:
        App.from_file(str(app_file))
    assert str(exc.value) == "Error while parsing YAML at path: {path}; {error}".format(path=app_file, error=error)


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
def test_app_from_yaml_invalid_yaml(value, error, tmpdir):
    with pytest.raises(InvalidManifestError) as exc:
        App.from_yaml(value)
    assert str(exc.value) == error


@pytest.mark.parametrize("env", ["BASE_URL", "base-url", "Base_Url", "base_url"])
def test_conform_env_keys(env):
    manifest = dedent(
        f"""
        name: Single composed job manifest
        data: /data
        jobs:
          job1:
            - transform: download
              env:
                {env}: http://example.com/data
        """
    )
    app = App.from_yaml(manifest)

    assert "BASE_URL" in app.jobs["job1"][0].env
    assert app.jobs["job1"][0].env["BASE_URL"] == "http://example.com/data"


@pytest.mark.parametrize(
    "env_item",
    ["not-a-dict", "1", "null", "true", " - 1", "- foo: bar"],
)
def test_conform_env_invalid_values(env_item):
    manifest = dedent(
        f"""
        name: Single composed job manifest
        data: /data
        jobs:
          job1:
            - transform: download
              env:
                {env_item}
        """
    )
    with pytest.raises(ValidationError) as exc:
        App.from_yaml(manifest)
    assert "jobs.job1.0.env\n  Input should be a valid dictionary" in str(exc.value)


@mock.patch.dict("xetl.models.app.os.environ", {"VAR1": "host-var1-value", "VAR2": "host-var2-value"}, clear=True)
def test_host_env_default_dont_inherit():
    manifest = dedent(
        """
        name: App does not inherit
        data: /data
        jobs: {}
        """
    )
    app = App.from_yaml(manifest)
    assert app.env == {}, "App should not inherit host env by default"


@pytest.mark.parametrize("all", ["'*'", "\n - '*'", "\n - V1\n - '*'"])
@mock.patch.dict("xetl.models.app.os.environ", {"VAR1": "host-var1-value", "VAR2": "host-var2-value"}, clear=True)
def test_host_env_inherit_all(all):
    manifest = dedent(
        """
        name: App does not inherit
        data: /data
        host-env: {all}
        env:
          VAR3: app-var3-value
        jobs: {{}}
        """
    ).format(all=all)
    app = App.from_yaml(manifest)
    assert app.env.get("VAR1") == "host-var1-value", "VAR1 should be set to the HOST env value"
    assert app.env.get("VAR2") == "host-var2-value", "VAR1 should be set to the HOST env value"
    assert app.env.get("VAR3") == "app-var3-value", "VAR1 should be set to the APP env value"


def test_host_env_inherit_all_mixed_warns(caplog):
    caplog.set_level("WARNING")
    manifest = dedent(
        """
        name: App does not inherit
        data: /data
        host-env:
          - VAR1
          - '*'
        jobs: {{}}
        """
    ).format(all=all)
    App.from_yaml(manifest)

    assert (
        "The `*` value in `host-env` was specified alongside other values. All host environment variables will be inherited."
        in caplog.text
    ), "Should have logged a warning about mixing '*' with other values"


@mock.patch.dict("xetl.models.app.os.environ", {"VAR1": "host-var1-value", "VAR2": "host-var2-value"}, clear=True)
def test_host_env_subset():
    manifest = dedent(
        """
        name: App does not inherit
        data: /data
        host-env:
          - VAR1
        jobs: {}
        """
    )
    app = App.from_yaml(manifest)
    assert app.env.get("VAR1") == "host-var1-value", "VAR1 should have been loaded from the HOST env"
    assert app.env.get("VAR2") == None, "VAR2 should NOT have been loaded from the HOST env"


@mock.patch.dict("xetl.models.app.os.environ", {"VAR1": "host-var1-value", "VAR2": "host-var2-value"}, clear=True)
def test_host_env_app_overrides_host_env():
    manifest = dedent(
        """
        name: App does not inherit
        data: /data
        host-env: "*"
        env:
          VAR1: app-var1-value
        jobs: {}
        """
    )
    app = App.from_yaml(manifest)
    assert app.env.get("VAR1") == "app-var1-value", "VAR1 should have been overridden by the APP env value"
    assert app.env.get("VAR2") == "host-var2-value", "VAR2 should have been loaded from the HOST env"


@mock.patch.dict("xetl.models.app.os.environ", {"HOST_VAR": "host-var-value"}, clear=True)
def test_step_env_inherits_host_and_app_env():
    manifest = dedent(
        f"""
        name: App does not inherit
        data: /data
        host-env:
          - HOST_VAR
        env:
          APP_VAR: app-var-value
        jobs:
          job1:
            - transform: transform1
              env:
                STEP_VAR: step-var-value
        """
    )
    app = App.from_yaml(manifest)
    assert (
        app.jobs["job1"][0].env.get("HOST_VAR") == "host-var-value"
    ), "The HOST var should have been inherited by the step"
    assert (
        app.jobs["job1"][0].env.get("APP_VAR") == "app-var-value"
    ), "The APP var should have been inherited by the step"
    assert (
        app.jobs["job1"][0].env.get("STEP_VAR") == "step-var-value"
    ), "The STEP var should have been inherited by the step"


@pytest.mark.parametrize(
    "placeholder, resolved",
    [
        ("${VAR1}", "second-step-var1-value"),
        ("${Var1}", "second-step-var1-value"),
        ("${APP_VAR}", "app-var-value"),
        ("${App_var}", "app-var-value"),
        ("${App-var}", "app-var-value"),
        ("${APP-VAR}", "app-var-value"),
        ("${previous.VAR1}", "first-step-var1-value"),
        ("${previous.Var1}", "first-step-var1-value"),
        ("${previous.APP_VAR}", "app-var-value"),
        ("${first-step.VAR1}", "first-step-var1-value"),
        ("${first_step.VAR1}", "first-step-var1-value"),
        ("${first-step.APP_VAR}", "app-var-value"),
        ("~/relative/path/", "/User/username/relative/path/"),
    ],
)
@mock.patch("xetl.models.app.os.path.expanduser", side_effect=fake_expanduser)
def test_resolve_placeholders(_, placeholder, resolved):
    manifest = dedent(
        f"""
        name: Single composed job manifest
        data: /data
        env:
          VAR1: app-var1-value
          APP_VAR: app-var-value
        jobs:
          job1:
            - name: first-step
              transform: transform1
              env:
                VAR1: first-step-var1-value
                VAR_INT: 123
                VAR_FLOAT: 123.4
                VAR_BOOL: true
            - name: second-step
              transform: transform2
              env:
                VAR1: second-step-var1-value
                VAR2: {placeholder}
        """
    )
    app = App.from_yaml(manifest)

    assert app.jobs["job1"][1].env["VAR2"] == resolved


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
        jobs:
          job1:
            - name: first-step
              transform: transform1
              env:
                VAR: {placeholder}
        """
    )
    app = App.from_yaml(manifest)

    assert app.jobs["job1"][0].env["VAR"] == resolved


@pytest.mark.parametrize(
    "placeholder, resolved",
    [
        ("'[${VAR}$vAr]'", "[valuevalue]"),
        ("${VAR}${var}", "valuevalue"),
        ("'[${var}]'", "[value]"),
        ("$var$app-var", "valueapp-var-value"),
        ("${VAR}/${APP_VAR}", "value/app-var-value"),
        ("$VAR/$$$APP_VAR", "value/$app-var-value"),
        ("$$$VAR/$$$APP_VAR/$$", "$value/$app-var-value/$"),
        ("$$$${VAR}", "$${VAR}"),
        ("$$$$VAR", "$$VAR"),
        ("$$${VAR}", "$value"),
        # the usecases below are specially crafted to have the placeholder `${VAR}` be 1 character
        # more than the `value` so that the logic could get confused with the adjacent literal `$`
        ("${VAR}/$$${APP_VAR}", "value/$app-var-value"),
        ("${VAR}//$${APP_VAR}", "value//${APP_VAR}"),
        ("'[$data] *${VAR}* $$${APP_VAR}$'", "[/data] *value* $app-var-value$"),
    ],
)
def test_resolve_placeholders_complex_matches(placeholder, resolved):
    manifest = dedent(
        f"""
        name: Job with complex placeholder matches
        data: /data
        env:
          APP_VAR: app-var-value
        jobs:
          job1:
            - name: first-step
              transform: transform1
              env:
                VAR: value
                PLACEHOLDER: {placeholder}
        """
    )
    app = App.from_yaml(manifest)

    assert app.jobs["job1"][0].env["PLACEHOLDER"] == resolved


@pytest.mark.parametrize("null_value", ["null", "~"])
def test_resolve_placeholders_none_value(null_value):
    manifest = dedent(
        f"""
        name: Job with complex placeholder matches
        data: /data
        env:
          APP_VAR: {null_value}
        jobs:
          job1:
            - name: first-step
              transform: transform1
              env:
                PLAIN: $APP_VAR
                EMBEDDED: this is $APP_VAR
        """
    )
    app = App.from_yaml(manifest)

    assert app.jobs["job1"][0].env["PLAIN"] == None
    assert app.jobs["job1"][0].env["EMBEDDED"] == "this is null", "None should be converted to a string as 'null'"


@mock.patch.dict("xetl.models.app.os.environ", {"HOST_VAR": "host-var-value"}, clear=True)
def test_resolve_placeholders_recursive_matches():
    manifest = dedent(
        """
        name: Job with complex placeholder matches
        data: /resolved-data-path
        host-env: "*"
        env:
          APP_VAR: app-var-value
        jobs:
          job1:
            - name: first-step
              transform: transform1
              env:
                VAR1: $data
                VAR2: "${VAR1}" # would-be-resolved variable
                VAR3: "${VAR3}" # self-referencing
                VAR4: "${VAR5}" # yet-to-be-resolved variable
                VAR5: ${APP_VAR}
                VAR6: ${HOST_VAR}
        """
    )
    app = App.from_yaml(manifest)

    assert app.jobs["job1"][0].env == {
        "APP_VAR": "app-var-value",
        "HOST_VAR": "host-var-value",
        "VAR1": "/resolved-data-path",
        "VAR2": "$data",  # pre-resolution value
        "VAR3": "${VAR3}",  # pre-resolution value
        "VAR4": "${APP_VAR}",  # pre-resolution value
        "VAR5": "app-var-value",
        "VAR6": "host-var-value",
    }, "Only variables referencing other envs (app or host) are resolved"


@mock.patch("xetl.models.app.os.path.abspath", side_effect=fake_abspath)
def test_resolve_placeholders_expands_relative_data_dir(_):
    manifest = dedent(
        """
        name: Single composed job manifest
        data: relative/data/path
        jobs:
          job1:
            - name: downloader
              transform: download
              env:
                BASE_URL: http://example.com/data
                OUTPUT: $data/downloader/output
        """
    )
    app = App.from_yaml(manifest)

    assert app.data == "/absolute/path/to/relative/data/path"
    assert app.jobs["job1"][0].env["OUTPUT"] == f"{app.data}/downloader/output"


def test_resolve_doesnt_expand_absolute_data_dir():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data/path
        jobs:
          job1:
            - name: downloader
              transform: download
              env:
                BASE_URL: http://example.com/data
                OUTPUT: $data/downloader/output
        """
    )
    app = App.from_yaml(manifest)

    assert app.data == "/data/path"
    assert app.jobs["job1"][0].env["OUTPUT"] == f"{app.data}/downloader/output"


def test_resolve_unknown_env_variable_no_vars_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        jobs:
          job1:
            - name: downloader
              transform: ${unknown.something}
        """
    )
    with pytest.raises(ValueError) as exc_info:
        App.from_yaml(manifest)

    actual_error = str(exc_info.value).split(" [type=value_error")[0]
    assert (
        actual_error
        == dedent(
            """
            1 validation error for App
              Value error, Invalid name `unknown` in `${unknown.something}`. The first must be one of:
             - variable in the current step's env: No env variables defined
             - name of a previous step: No previous steps defined
            """
        ).strip()
    ), str(exc_info.value)


def test_resolve_unknown_env_variable_no_previous_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        env:
          APP_VAR: app-var-value
        jobs:
          job1:
            - name: downloader
              transform: download
              env:
                VAR1: http://example.com/data
                VAR2: $unknown/foo/bar/baz
        """
    )
    with pytest.raises(ValueError) as exc_info:
        App.from_yaml(manifest)

    actual_error = str(exc_info.value).split(" [type=value_error")[0]
    assert (
        actual_error
        == dedent(
            """
            1 validation error for App
              Value error, Invalid name `unknown` in `$unknown`. The first must be one of:
             - variable in the current step's env: APP_VAR, VAR1, VAR2
             - name of a previous step: No previous steps defined
            """
        ).strip()
    ), str(exc_info.value)


def test_resolve_unknown_env_variable_no_current_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        jobs:
          job1:
            - name: first
              transform: first
              env:
                VAR1: http://example.com/data
            - name: second
              transform: $unknown
        """
    )
    with pytest.raises(ValueError) as exc_info:
        App.from_yaml(manifest)

    actual_error = str(exc_info.value).split(" [type=value_error")[0]
    assert (
        actual_error
        == dedent(
            """
            1 validation error for App
              Value error, Invalid name `unknown` in `$unknown`. The first must be one of:
             - variable in the current step's env: No env variables defined
             - name of a previous step: first, previous
            """
        ).strip()
    ), str(exc_info.value)


def test_resolve_unknown_env_variable_with_previous_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        env:
          APP_VAR: app-var-value
        jobs:
          job1:
            - name: first
              transform: first
              env:
                VAR1: http://example.com/data
            - name: second
              transform: second
              env:
                VAR1: http://example.com/data
                VAR2: $unknown/foo/bar/baz
        """
    )
    with pytest.raises(ValueError) as exc_info:
        App.from_yaml(manifest)

    actual_error = str(exc_info.value).split(" [type=value_error")[0]
    assert (
        actual_error
        == dedent(
            """
            1 validation error for App
              Value error, Invalid name `unknown` in `$unknown`. The first must be one of:
             - variable in the current step's env: APP_VAR, VAR1, VAR2
             - name of a previous step: first, previous
            """
        ).strip()
    ), str(exc_info.value)


def test_resolve_incomplete_variable_path_raises():
    manifest = dedent(
        f"""
        name: Single composed job manifest
        data: /data
        jobs:
          job1:
            - name: downloader1
              transform: download
              env:
                BASE_URL: http://example.com/data
                OUTPUT: $data/foo
            - name: downloader2
              transform: download
              env:
                BASE_URL: http://example.com/data
                OUTPUT: ${{previous}} # missing env key
        """
    )
    with pytest.raises(ValueError) as exc_info:
        App.from_yaml(manifest)
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
        jobs:
          job1:
            - name: downloader
              transform: download
              env:
                BASE_URL: http://example.com/data
                OUTPUT: ${{tmp.dir}}
            - name: splitter
              transform: split
              env:
                FOO: ${{previous.OUTPUT}}
                OUTPUT: ${{tmp.dir}}
        """
    )
    app = App.from_yaml(manifest)

    assert all(
        isinstance(step.env["OUTPUT"], str) and step.env["OUTPUT"].startswith(data_path + "/tmp/")
        for step in app.jobs["job1"]
    ), f"All steps should output to a tmp directory: {[s.env['output'] for s in app.jobs['job1']]}"
    assert all(os.path.isdir(step.env["OUTPUT"]) for step in app.jobs["job1"]), "Each output should be a directory"  # type: ignore
    assert (
        app.jobs["job1"][0].env["OUTPUT"] != app.jobs["job1"][1].env["OUTPUT"]
    ), "Every tmp value should be a different value"
    assert (
        app.jobs["job1"][1].env["FOO"] == app.jobs["job1"][0].env["OUTPUT"]
    ), "References to tmp dir should be the same value"


def test_resolve_tmp_file(tmpdir):
    data_path = str(tmpdir.mkdir("data"))
    manifest = dedent(
        f"""
        name: Single composed job manifest
        data: {data_path}
        jobs:
          job1:
            - name: downloader
              transform: download
              env:
                BASE_URL: http://example.com/data
                OUTPUT: ${{tmp.file}}
            - name: splitter
              transform: split
              env:
                FOO: ${{previous.OUTPUT}}
                OUTPUT: ${{tmp.file}}
        """
    )
    app = App.from_yaml(manifest)

    assert all(
        str(step.env["OUTPUT"]).startswith(data_path + "/tmp/") for step in app.jobs["job1"]
    ), "All steps should output to a tmp directory"
    assert all(
        os.path.isfile(str(step.env["OUTPUT"])) for step in app.jobs["job1"]
    ), "Each output should be a directory"
    assert (
        app.jobs["job1"][0].env["OUTPUT"] != app.jobs["job1"][1].env["OUTPUT"]
    ), "Every tmp value should be a different value"
    assert (
        app.jobs["job1"][1].env["FOO"] == app.jobs["job1"][0].env["OUTPUT"]
    ), "References to tmp file should be the same value"


def test_resolve_variable_previous_unknown_variable_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        jobs:
          job1:
            - name: downloader
              transform: download
              env:
                BASE_URL: http://example.com/data
                OUTPUT: /data/output1
            - name: splitter
              transform: split
              env:
                FOO: ${previous.unknown}
                OUTPUT: /data/output2
        """
    )

    with pytest.raises(ValueError) as exc_info:
        App.from_yaml(manifest)
    actual_error = str(exc_info.value).split(" [type=value_error")[0]
    assert actual_error.strip() == (
        "1 validation error for App\n  Value error, Invalid placeholder `unknown` in ${previous.unknown}. "
        "Valid keys are: `BASE_URL`, `OUTPUT`"
    ), str(exc_info.value)


def test_resolve_variable_previous_output_first_step_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        jobs:
          job1:
            - name: splitter
              transform: split
              env:
                FOO: ${previous.output}
                OUTPUT: /data/output
        """
    )

    with pytest.raises(Exception) as exc_info:
        App.from_yaml(manifest)
    assert "Cannot use $previous placeholder on the first step" in str(exc_info.value)


def test_resolve_variable_chained_placeholders():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        jobs:
          job1:
            - name: downloader1
              transform: download
              env:
                BASE_URL: http://example.com$data
                OUTPUT: /tmp/data/d1
            - name: downloader2
              transform: download
              env:
                BASE_URL: ${downloader1.base_url}
                OUTPUT: /tmp/data/d2
            - name: downloader3
              transform: download
              env:
                BASE_URL: ${downloader2.base_url}
                OUTPUT: /tmp/data/d3
    """
    )

    app = App.from_yaml(manifest)

    actual_base_urls = [step.env["BASE_URL"] for step in app.jobs["job1"]]
    assert actual_base_urls == ["http://example.com/data"] * 3


def test_resolve_variable_circular_placeholders_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        jobs:
          job1:
            - name: downloader1
              transform: download
              env:
                BASE_URL: http://example.com$data
                OUTPUT: ${downloader2.output}
            - name: downloader2
              transform: download
              env:
                BASE_URL: http://example.com$data
                OUTPUT: ${downloader1.output}
    """
    )

    with pytest.raises(Exception) as exc:
        App.from_yaml(manifest)
    actual_message = str(exc.value).split(" [type=value_error")[0]
    assert (
        actual_message
        == dedent(
            """
            1 validation error for App
              Value error, Invalid name `downloader2` in `${downloader2.output}`. The first must be one of:
             - variable in the current step's env: BASE_URL, OUTPUT
             - name of a previous step: No previous steps defined
            """
        ).strip()
    )


def test_resolve_variable_named_placeholders_reference_other_job_raises():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        jobs:
          job1:
            - name: downloader1
              transform: download
              env:
                BASE_URL: http://example.com$data
                OUTPUT: $data/job1
          job2:
            - name: downloader2
              transform: download
              env:
                BASE_URL: http://example.com$data
                OUTPUT: ${downloader1.output}/job2
        """
    )

    with pytest.raises(Exception) as exc:
        App.from_yaml(manifest)
    actual_message = str(exc.value).split(" [type=value_error")[0]
    assert (
        actual_message
        == dedent(
            """
            1 validation error for App
              Value error, Invalid name `downloader1` in `${downloader1.output}`. The first must be one of:
             - variable in the current step's env: BASE_URL, OUTPUT
             - name of a previous step: No previous steps defined
            """
        ).strip()
    )
