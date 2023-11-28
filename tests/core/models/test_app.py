import os
import re
from textwrap import dedent

import mock
from pydantic import ValidationError
import pytest

from metl.core.models.app import App


def fake_expanduser(path):
    return re.sub(r"^~", "/User/username", path)


def fake_abspath(path):
    return f"/absolute/path/to/{path}"


# TODO: add tests for steps?


@pytest.mark.parametrize(
    "placeholder, resolved",
    [
        ("${downloader.env.output}/mid/${downloader.name}", "/some/path/mid/downloader"),
        ("[${downloader.env.output}${downloader.name}]", "[/some/pathdownloader]"),
        ("${downloader.env.output}$downloader.name", "/some/path$downloader.name"),
        ("${downloader.env.output}/${previous.env.method}", "/some/path/GET"),
        ("${downloader.env.output}/$$${previous.env.method}", "/some/path/$GET"),
        ("$$$${downloader.output}", "$${downloader.output}"),
        ("$downloader.env.base_url", "$downloader.env.base_url"),
        ("[${downloader.env.base_url}]", "[http://example.com/data]"),
        ("[$data] *${downloader.transform}* $$${downloader.env.method}$", "[/data] *download* $GET$"),
        ("~/relative/path/${downloader.name}", "/User/username/relative/path/downloader"),
        ("${previous.env.output}", "/some/path"),
        ("${previous.env.OUTPUT}", "/some/path"),
    ],
)
@mock.patch("metl.core.models.app.os.path.expanduser", side_effect=fake_expanduser)
def test_resolve_placeholders(_, placeholder, resolved):
    manifest = dedent(
        f"""
        name: Single composed job manifest
        data: /data
        jobs:
          job1:
            - name: downloader
              transform: download
              env:
                METHOD: GET
                BASE_URL: http://example.com/data
                OUTPUT: /some/path
            - name: splitter
              transform: split
              env:
                OUTPUT: '{placeholder}'
        """
    )
    app = App.from_yaml(manifest)

    assert app.jobs["job1"][1].env["OUTPUT"] == resolved


@mock.patch("metl.core.models.app.os.path.abspath", side_effect=fake_abspath)
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


def test_resolve_unknown_app_variable():
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
                OUTPUT: $unknown/foo/bar/baz
        """
    )
    app = App.from_yaml(manifest)

    assert app.jobs["job1"][0].env["OUTPUT"] == "$unknown/foo/bar/baz", "The output should have stayed intact"


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
                FOO: bar
                OUTPUT: ${{tmp.dir}}
        """
    )
    app = App.from_yaml(manifest)

    assert all(
        isinstance(step.env["OUTPUT"], str) and step.env["OUTPUT"].startswith(data_path + "/tmp/")
        for step in app.jobs["job1"]
    ), f"All steps should output to a tmp directory: {[s.env['output'] for s in app.jobs['job1']]}"
    assert all(isinstance(step.env["OUTPUT"], str) for step in app.jobs["job1"])
    assert all(os.path.isdir(step.env["OUTPUT"]) for step in app.jobs["job1"]), "Each output should be a directory"  # type: ignore
    assert (
        app.jobs["job1"][0].env["OUTPUT"] != app.jobs["job1"][1].env["OUTPUT"]
    ), "Every tmp value should be a different value"


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
                FOO: bar
                OUTPUT: ${{tmp.file}}
        """
    )
    app = App.from_yaml(manifest)

    assert all(
        step.env["OUTPUT"].startswith(data_path + "/tmp/") for step in app.jobs["job1"]
    ), "All steps should output to a tmp directory"
    assert all(os.path.isfile(step.env["OUTPUT"]) for step in app.jobs["job1"]), "Each output should be a directory"
    assert (
        app.jobs["job1"][0].env["OUTPUT"] != app.jobs["job1"][1].env["OUTPUT"]
    ), "Every tmp value should be a different value"


def test_resolve_unknown_step():
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
                FOO: ${unknown.output}
                OUTPUT: /data/output2
        """
    )

    with pytest.raises(Exception) as exc_info:
        App.from_yaml(manifest)
    assert (
        str(exc_info.value)
        == "Invalid placeholder `unknown` in ${unknown.output}. Valid keys are: `downloader`, `previous`"
    )


def test_resolve_unknown_variable():
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
                FOO: ${downloader.unknown}
                OUTPUT: /data/output2
        """
    )

    with pytest.raises(Exception) as exc_info:
        App.from_yaml(manifest)
    assert (
        str(exc_info.value)
        == "Invalid placeholder `unknown` in ${downloader.unknown}. Valid keys are: `description`, `env`, `name`, `skip`, `transform`"
    )


def test_resolve_variable_previous_output():
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
                OUTPUT: /some/path
            - name: splitter
              transform: split
              env:
                FOO: ${previous.env.output}
                OUTPUT: /data/output
        """
    )
    app = App.from_yaml(manifest)

    assert app.jobs["job1"][1].env["FOO"] == app.jobs["job1"][0].env["OUTPUT"]


def test_resolve_variable_previous_output_no_previous_output():
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

    with pytest.raises(Exception) as exc_info:
        App.from_yaml(manifest)
    assert str(exc_info.value) == (
        "Invalid placeholder `unknown` in ${previous.unknown}. "
        "Valid keys are: `description`, `env`, `name`, `skip`, `transform`"
    )


def test_resolve_variable_previous_output_first_step():
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
    assert str(exc_info.value) == "Cannot use $previous placeholder on the first step"


def test_resolve_variable_previous_output_variable(tmpdir):
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
                OUTPUT: $data/output
            - name: splitter
              transform: split
              env:
                FOO: ${{previous.env.output}}
                OUTPUT: /data/output
        """
    )

    app = App.from_yaml(manifest)

    assert app.jobs["job1"][1].env["FOO"] == app.jobs["job1"][0].env["OUTPUT"]
    assert app.jobs["job1"][0].env["OUTPUT"] == f"{data_path}/output"


def test_resolve_variable_with_literal_dollar_sign():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /should/not/get/resolved/by/literal/dollar/signs
        jobs:
          job1:
            - name: downloader
              transform: download
              env:
                BASE_URL: http://example.com/$$data
                OUTPUT: path/$$data
            - name: splitter
              transform: split
              env:
                FOO: ${previous.env.base_url}
                OUTPUT: ${previous.env.output}
        """
    )

    app = App.from_yaml(manifest)

    steps = app.jobs["job1"]
    assert steps[0].env["OUTPUT"] == "path/$data"
    assert steps[0].env["BASE_URL"] == "http://example.com/$data"
    assert steps[1].env["OUTPUT"] == "path/$data"
    assert steps[1].env["FOO"] == "http://example.com/$data"


def test_run_app_chained_placeholders():
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
                BASE_URL: ${downloader1.env.base_url}
                OUTPUT: /tmp/data/d2
            - name: downloader3
              transform: download
              env:
                BASE_URL: ${downloader2.env.base_url}
                OUTPUT: /tmp/data/d3
    """
    )

    app = App.from_yaml(manifest)

    actual_base_urls = [step.env["BASE_URL"] for step in app.jobs["job1"]]
    assert actual_base_urls == ["http://example.com/data"] * 3


def test_run_app_circular_placeholders():
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
    assert (
        str(exc.value) == "Invalid placeholder `downloader2` in ${downloader2.output}. There are no steps to reference."
    )


def test_run_app_named_placeholders_reference_other_job():
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
    assert (
        str(exc.value) == "Invalid placeholder `downloader1` in ${downloader1.output}. There are no steps to reference."
    )
