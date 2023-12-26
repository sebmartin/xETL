import os
import sys
from textwrap import dedent

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


@pytest.fixture
def commands_fixtures_path():
    return os.path.abspath(os.path.dirname(__file__) + "/../tests/fixtures")


def job_file(job_yaml: str, tmpdir):
    path = os.path.join(tmpdir, "job.yml")
    with open(path, "w") as fd:
        fd.write(job_yaml)
    return path


@pytest.fixture
def job_manifest_simple(commands_fixtures_path):
    return dedent(
        f"""
        name: Simple job manifest
        data: /data
        commands: {commands_fixtures_path}
        env:
          JOB_VAR: job-var-value
        tasks:
          - name: Download
            command: download
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
def job_manifest_multiple_tasks(commands_fixtures_path):
    return dedent(
        f"""
        name: Multiple job manifest
        data: /data
        commands: {commands_fixtures_path}
        tasks:
          - name: Download
            command: download
            env:
              BASE_URL: http://example.com/data
              THROTTLE: 1000
              OUTPUT: /tmp/data
          - name: Splitter
            command: splitter
            env:
              FILES: /tmp/data
              OUTPUT: /tmp/data/splits
        """
    )


@pytest.fixture
def job_manifest_multiple_tasks_path(job_manifest_multiple_tasks, tmpdir):
    return job_file(job_manifest_multiple_tasks, tmpdir)
