import os
from textwrap import dedent
import pytest


@pytest.fixture
def transforms_fixtures_path():
    return os.path.abspath(os.path.dirname(__file__) + "/../../tests/fixtures")


def app_file(app_yaml: str, tmpdir):
    path = os.path.join(tmpdir, "app.yml")
    with open(path, "w") as fd:
        fd.write(app_yaml)
    return path


@pytest.fixture
def app_manifest_simple():
    return dedent(
        """
        name: Simple app manifest
        data: /data
        jobs:
          my-job:
            - transform: download
              output: /tmp/data/morgues
              args:
                base_url: http://example.com/data
                throttle: 1000
        """
    )


@pytest.fixture
def app_manifest_simple_path(app_manifest_simple, tmpdir):
    return app_file(app_manifest_simple, tmpdir)


@pytest.fixture
def app_manifest_multiple_single_step_jobs():
    return dedent(
        """
        name: Multiple job manifest
        data: /data
        jobs:
          download:
            - transform: download
              output: /tmp/data/morgues
              args:
                base_url: http://example.com/data
                throttle: 1000
          split:
            - transform: splitter
              output: /tmp/data/splits
              args:
                morgues: /tmp/data/morgues
        """
    )


@pytest.fixture
def app_manifest_multiple_single_step_jobs_path(app_manifest_multiple_single_step_jobs, tmpdir):
    return app_file(app_manifest_multiple_single_step_jobs, tmpdir)


@pytest.fixture
def app_manifest_single_multiple_step_job():
    return dedent(
        """
        name: Multiple job manifest
        data: /data
        jobs:
          download:
            - transform: download
              output: /tmp/data/morgues
              args:
                base_url: http://example.com/data
                throttle: 1000
            - transform: splitter
              output: /tmp/data/splits
              args:
                morgues: /tmp/data/morgues
        """
    )


@pytest.fixture
def app_manifest_single_multiple_step_job_path(app_manifest_single_multiple_step_job, tmpdir):
    return app_file(app_manifest_single_multiple_step_job, tmpdir)


@pytest.fixture
def app_manifest_multiple_jobs_with_multiples():
    return dedent(
        """
        name: Multiple job manifest
        data: /data
        jobs:
          download-1:
            - name: download-1
              transform: download
              output: /tmp/data1/source
              args:
                base_url: http://example.com/data1
                throttle: 1000
            - name: splitter-1
              transform: splitter
              output: /tmp/data1/splits
              args:
                source: /tmp/data1/source
          download-2:
            - name: download-2
              transform: download
              output: /tmp/data2/source
              args:
                base_url: http://example.com/data2
                throttle: 1000
            - name: splitter-2
              transform: splitter
              output: /tmp/data2/splits
              args:
                source: /tmp/data2/source
        """
    )


@pytest.fixture
def app_manifest_multiple_jobs_with_multiples_path(app_manifest_multiple_jobs_with_multiples, tmpdir):
    return app_file(app_manifest_multiple_jobs_with_multiples, tmpdir)
