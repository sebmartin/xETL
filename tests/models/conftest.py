import mock
import pytest


@pytest.fixture(autouse=True)
def mock_subprocess_run(request):
    """
    Make sure we don't accidentally run a subprocess during tests. This can be disabled by marking
    the test with `@pytest.mark.real_subprocess_run`.
    """
    if "real_subprocess_run" in request.keywords:
        yield
        return
    with mock.patch("subprocess.run", mock.Mock()) as mock_run:
        yield mock_run


@pytest.fixture(autouse=True)
def mock_verify_data_dir(request):
    """
    Do not verify that the data directory exists during tests. This can be disabled by marking
    the test with `@pytest.mark.real_verify_data_dir`.
    """
    if "real_verify_data_dir" in request.keywords:
        yield
        return
    with mock.patch("xetl.models.job.Job._verify_data_dir", mock.Mock()) as mock_verify:
        yield mock_verify
