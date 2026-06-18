import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--smoke",
        action="store_true",
        default=False,
        help="run smoke tests against live external services",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--smoke"):
        return

    skip_smoke = pytest.mark.skip(reason="need --smoke option to run")
    for item in items:
        if "smoke" in item.keywords:
            item.add_marker(skip_smoke)
