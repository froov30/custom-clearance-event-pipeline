import os

import pytest


def pytest_collection_modifyitems(config, items):
    # Integration tests need the Compose stack; plain `pytest -q` stays infra-free.
    if os.environ.get("CCP_INTEGRATION") == "1":
        return
    skip = pytest.mark.skip(reason="needs the Compose stack; set CCP_INTEGRATION=1")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)
