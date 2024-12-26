# Standard library
import json


class TestCaseLoader(object):
    """Used to load tests cases from json."""

    __test__ = False

    def __init__(self, source_path: str):
        with open(f"tests/test_cases/{source_path}.json", "r") as f:
            self.source = json.load(f)

    def load(self):
        """loads tests for a specific method."""
        test_params = self.source["params"]
        test_cases = self.source["test_cases"]

        scenarios = []
        params = test_params.split(", ")
        for scenario_name in test_cases:
            test_case = test_cases[scenario_name]
            scenario = [test_case.get(param) for param in params]
            scenarios.append(tuple(scenario))

        return [test_params, scenarios]
