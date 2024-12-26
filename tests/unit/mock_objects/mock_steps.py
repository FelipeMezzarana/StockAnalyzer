class MockValidator:
    """Mock class for Validator."""

    def __init__(self, *args, **kwargs):
        pass

    def run(self):
        """Mock run method."""
        return True, {"table": "output"}


class MockSQLLoader:
    """Mock class for SQLLoader."""

    def __init__(self, *args, **kwargs):
        pass

    def run(self):
        """Mock run method."""
        return True, {"table": "output"}
