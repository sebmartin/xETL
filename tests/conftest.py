import logging
import os
import sys
from textwrap import dedent

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


@pytest.fixture
def caplog(caplog):
    # Default log level capture to INFO
    caplog.set_level(logging.INFO)
    return caplog


@pytest.fixture
def tasks_fixtures_path():
    return os.path.abspath(os.path.dirname(__file__) + "/../tests/fixtures")


def job_file(job_yaml: str, tmpdir):
    path = os.path.join(tmpdir, "job.yml")
    with open(path, "w") as fd:
        fd.write(job_yaml)
    return path


@pytest.fixture
def job_manifest_simple(tasks_fixtures_path):
    return dedent(
        f"""
        name: Simple job manifest
        data: /data
        tasks: {tasks_fixtures_path}
        env:
          JOB_VAR: job-var-value
        commands:
          - name: Download
            task: download
            env:
              BASE_URL: http://example.com/data
              THROTTLE: 1000
              OUTPUT: /tmp/data
        """
    )


@pytest.fixture
def job_manifest_simple_path(job_manifest_simple, tmpdir):
    return job_file(job_manifest_simple, tmpdir)


@pytest.fixture
def job_manifest_multiple_commands(tasks_fixtures_path):
    return dedent(
        f"""
        name: Multiple job manifest
        data: /data
        tasks: {tasks_fixtures_path}
        commands:
          - name: Download-File
            task: download
            env:
              BASE_URL: http://example.com/data
              THROTTLE: 1000
              OUTPUT: /tmp/data
          - name: Split_File
            task: splitter
            env:
              FILES: /tmp/data
              OUTPUT: /tmp/data/splits
        """
    )


@pytest.fixture
def job_manifest_multiple_commands_path(job_manifest_multiple_commands, tmpdir):
    return job_file(job_manifest_multiple_commands, tmpdir)
