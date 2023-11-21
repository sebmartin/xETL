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


@pytest.mark.parametrize(
    "placeholder, resolved",
    [
        ("${downloader.output}/mid/${downloader.name}", "/some/path/mid/downloader"),
        ("[${downloader.output}${downloader.name}]", "[/some/pathdownloader]"),
        ("${downloader.output}$downloader.name", "/some/path$downloader.name"),
        ("${downloader.output}/${previous.args.method}", "/some/path/GET"),
        ("${downloader.output}/$$${previous.args.method}", "/some/path/$GET"),
        ("$$$${downloader.output}", "$${downloader.output}"),
        ("$downloader.args.base_url", "$downloader.args.base_url"),
        ("[${downloader.args.base_url}]", "[http://example.com/data]"),
        ("[$data] *${downloader.transform}* $$${downloader.args.method}$", "[/data] *download* $GET$"),
        ("~/relative/path/${downloader.name}", "/User/username/relative/path/downloader"),
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
              output: /some/path
              args:
                method: GET
                base_url: http://example.com/data
            - name: splitter
              transform: split
              output: '{placeholder}'
        """
    )
    app = App.from_yaml(manifest)

    assert app.jobs["job1"][1].output == resolved


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
              output: $data/downloader/output
              args:
                base_url: http://example.com/data
        """
    )
    app = App.from_yaml(manifest)

    assert app.data == "/absolute/path/to/relative/data/path"
    assert app.jobs["job1"][0].output == f"{app.data}/downloader/output"


def test_resolve_doesnt_expand_absolute_data_dir():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data/path
        jobs:
          job1:
            - name: downloader
              transform: download
              output: $data/downloader/output
              args:
                base_url: http://example.com/data
        """
    )
    app = App.from_yaml(manifest)

    assert app.data == "/data/path"
    assert app.jobs["job1"][0].output == f"{app.data}/downloader/output"


def test_resolve_unknown_app_variable():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        jobs:
          job1:
            - name: downloader
              transform: download
              output: $unknown/foo/bar/baz
              args:
                base_url: http://example.com/data
        """
    )
    app = App.from_yaml(manifest)

    assert app.jobs["job1"][0].output == "$unknown/foo/bar/baz", "The output should have stayed intact"


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
              output: ${{tmp.dir}}
              args:
                base_url: http://example.com/data
            - name: splitter
              transform: split
              output: ${{tmp.dir}}
              args:
                foo: bar
        """
    )
    app = App.from_yaml(manifest)

    assert all(
        step.output.startswith(data_path + "/tmp/") for step in app.jobs["job1"]
    ), f"All steps should output to a tmp directory: {[s.output for s in app.jobs['job1']]}"
    assert all(os.path.isdir(step.output) for step in app.jobs["job1"]), "Each output should be a directory"
    app.jobs["job1"][0].output != app.jobs["job1"][1].output, "Every tmp value should be a different value"


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
              output: ${{tmp.file}}
              args:
                  base_url: http://example.com/data
            - name: splitter
              transform: split
              output: ${{tmp.file}}
              args:
                  foo: bar
        """
    )
    app = App.from_yaml(manifest)

    assert all(
        step.output.startswith(data_path + "/tmp/") for step in app.jobs["job1"]
    ), "All steps should output to a tmp directory"
    assert all(os.path.isfile(step.output) for step in app.jobs["job1"]), "Each output should be a directory"
    app.jobs["job1"][0].output != app.jobs["job1"][1].output, "Every tmp value should be a different value"


def test_resolve_unknown_step():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        jobs:
          job1:
            - name: downloader
              transform: download
              output: /data/output1
              args:
                base_url: http://example.com/data
            - name: splitter
              transform: split
              output: /data/output2
              args:
                foo: ${unknown.output}
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
              output: /data/output1
              args:
                base_url: http://example.com/data
            - name: splitter
              transform: split
              output: /data/output2
              args:
                foo: ${downloader.unknown}
        """
    )

    with pytest.raises(Exception) as exc_info:
        App.from_yaml(manifest)
    assert (
        str(exc_info.value)
        == "Invalid placeholder `unknown` in ${downloader.unknown}. Valid keys are: `args`, `description`, `name`, `output`, `skip`, `transform`"
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
              output: /some/path
              args:
                base_url: http://example.com/data
            - name: splitter
              transform: split
              output: /data/output
              args:
                foo: ${previous.output}
        """
    )
    app = App.from_yaml(manifest)

    assert app.jobs["job1"][1].args["foo"] == app.jobs["job1"][0].output


def test_resolve_variable_previous_output_no_previous_output():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        jobs:
          job1:
            - name: downloader
              transform: download
              output: /data/output1
              args:
                base_url: http://example.com/data
            - name: splitter
              transform: split
              output: /data/output2
              args:
                foo: ${previous.unknown}
        """
    )

    with pytest.raises(Exception) as exc_info:
        App.from_yaml(manifest)
    assert str(exc_info.value) == (
        "Invalid placeholder `unknown` in ${previous.unknown}. "
        "Valid keys are: `args`, `description`, `name`, `output`, `skip`, `transform`"
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
              output: /data/output
              args:
                foo: ${previous.output}
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
              output: $data/output
              args:
                base_url: http://example.com/data
            - name: splitter
              transform: split
              output: /data/output
              args:
                foo: ${{previous.output}}
        """
    )

    app = App.from_yaml(manifest)

    assert app.jobs["job1"][1].args["foo"] == app.jobs["job1"][0].output
    assert app.jobs["job1"][0].output == f"{data_path}/output"


def test_resolve_variable_with_literal_dollar_sign():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /should/not/get/resolved/by/literal/dollar/signs
        jobs:
          job1:
            - name: downloader
              transform: download
              output: path/$$data
              args:
                base_url: http://example.com/$$data
            - name: splitter
              transform: split
              output: ${previous.output}
              args:
                foo: ${previous.args.base_url}
        """
    )

    app = App.from_yaml(manifest)

    steps = app.jobs["job1"]
    assert steps[0].output == "path/$data"
    assert steps[0].args["base_url"] == "http://example.com/$data"
    assert steps[1].output == "path/$data"
    assert steps[1].args["foo"] == "http://example.com/$data"


def test_run_app_chained_placeholders():
    manifest = dedent(
        """
        name: Single composed job manifest
        data: /data
        jobs:
          job1:
            - name: downloader1
              transform: download
              output: /tmp/data/d1
              args:
                base_url: http://example.com$data
            - name: downloader2
              transform: download
              output: /tmp/data/d2
              args:
                base_url: ${downloader1.args.base_url}
            - name: downloader3
              transform: download
              output: /tmp/data/d3
              args:
                base_url: ${downloader2.args.base_url}
    """
    )

    app = App.from_yaml(manifest)

    actual_base_urls = [step.args["base_url"] for step in app.jobs["job1"]]
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
              output: ${downloader2.output}
              args:
                base_url: http://example.com$data
            - name: downloader2
              transform: download
              output: ${downloader1.output}
              args:
                base_url: http://example.com$data
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
              output: $data/job1
              args:
                base_url: http://example.com$data
          job2:
            - name: downloader2
              transform: download
              output: ${downloader1.output}/job2
              args:
                base_url: http://example.com$data
        """
    )

    with pytest.raises(Exception) as exc:
        App.from_yaml(manifest)
    assert (
        str(exc.value) == "Invalid placeholder `downloader1` in ${downloader1.output}. There are no steps to reference."
    )
