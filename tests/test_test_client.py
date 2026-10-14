import subprocess
import sys


def test_the_api_test_client_imports_without_a_deprecation_warning():
    # A fresh process, so an earlier import cannot hide the warning. `-W error` makes it fail.
    result = subprocess.run(
        [sys.executable, "-W", "error", "-c", "import fastapi.testclient"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr[-400:]
