def pytest_ignore_collect(path, config):
    return "orchestration/assets" in str(path)
