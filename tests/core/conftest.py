import os
from textwrap import dedent
import pytest


@pytest.fixture
def transforms_fixtures_path():
    return os.path.abspath(os.path.dirname(__file__) + "/../tests/fixtures")


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
              env:
                BASE_URL: http://example.com/data
                THROTTLE: 1000
                OUTPUT: /tmp/data
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
              env:
                BASE_URL: http://example.com/data
                THROTTLE: 1000
                OUTPUT: /tmp/data
          split:
            - transform: splitter
              env:
                FILES: /tmp/data
                OUTPUT: /tmp/data/splits
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
              env:
                BASE_URL: http://example.com/data
                THROTTLE: 1000
                OUTPUT: /tmp/data
            - transform: splitter
              env:
                FILES: /tmp/data
                OUTPUT: /tmp/data/splits
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
              env:
                BASE_URL: http://example.com/data1
                THROTTLE: 1000
                OUTPUT: /tmp/data1/source
            - name: splitter-1
              transform: splitter
              env:
                source: /tmp/data1/source
                OUTPUT: /tmp/data1/splits
          download-2:
            - name: download-2
              transform: download
              env:
                BASE_URL: http://example.com/data2
                THROTTLE: 1000
                OUTPUT: /tmp/data2/source
            - name: splitter-2
              transform: splitter
              env:
                source: /tmp/data2/source
                OUTPUT: /tmp/data2/splits
        """
    )


@pytest.fixture
def app_manifest_multiple_jobs_with_multiples_path(app_manifest_multiple_jobs_with_multiples, tmpdir):
    return app_file(app_manifest_multiple_jobs_with_multiples, tmpdir)
