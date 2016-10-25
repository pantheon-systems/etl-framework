"""tests results"""
import unittest
from mock import MagicMock

from etl_framework.testing.configs.result import ResultConfig
from etl_framework.testing.results import ResultInterface, \
    Results

class ResultsTestCases(unittest.TestCase):
    """class for results tests"""

    def setUp(self):

        # Mocks
        self.result1 = MagicMock()
        self.result2 = MagicMock()
        self.results = Results([
            self.result1,
            self.result2,
        ])

    def test_set_up_calls_each_result_set_up(self):
        """stuff"""

        self.results.set_up()

        self.assertTrue(self.result1.set_up.called)
        self.assertTrue(self.result2.set_up.called)

    def test_tear_down_calls_each_result_tear_down(self):
        """stuff"""

        self.results.tear_down()

        self.assertTrue(self.result1.tear_down.called)
        self.assertTrue(self.result2.tear_down.called)

    def test_actual_and_expected_result_calls_each_actual_and_expected_result(self):
        """stuff"""

        for _ in self.results.actual_and_expected_results():
            pass

        self.assertTrue(self.result1.actual_and_expected_result.called)
        self.assertTrue(self.result2.actual_and_expected_result.called)

class ResultInterfaceTestCases(unittest.TestCase):
    """class for result tests"""

    def setUp(self):

	# Static values
        self.expected_result = [{"field1": "value1"}]
        self.raw_result = [{"field1": "value2", "field2": 1}]
        # Subset result is subset of raw_result with keys in expected result
        self.subset_result = [{"field1": "value2"}]

        # Mocks
        self.mock_schema = MagicMock()

        ResultInterface.raw_result = MagicMock()
        ResultInterface.raw_result.return_value = self.raw_result

        self.config = ResultConfig()
        self.config.config = {
            "schema": self.mock_schema,
            "expected_result": self.expected_result,
            "match_type": "exact"
        }


        self.result = ResultInterface(
            config=self.config
        )

    def test_actual_result_with_match_type_exact(self):
        """stuff"""

        self.config.config["match_type"] = 'exact'

        actual_result = self.result.actual_result()
        self.assertEqual(actual_result, self.raw_result)

    def test_actual_result_with_match_type_subset(self):
        """stuff"""

        self.config.config["match_type"] = 'subset'

        actual_result = self.result.actual_result()
        self.assertEqual(actual_result, self.subset_result)

    def test_actual_result_with_unsupported_match_type_raises_exception(self):
        """stuff"""

        self.config.config["match_type"] = 'NO TYPE'

        self.assertRaises(Exception, self.result.actual_result)

    def test_set_up_calls_schema_create_if_not_exists(self):
        """test result interface"""

        self.result.set_up()

        self.assertTrue(self.mock_schema.create_if_not_exists.called)

    def test_actual_and_expected_result(self):
        """stuff"""

        self.result.actual_result = MagicMock()
        stub_actual_result = 'ACTUAL RESULT'
        self.result.actual_result.return_value = stub_actual_result

        actual_and_expected_result = self.result.actual_and_expected_result()

        self.assertEqual(
            actual_and_expected_result,
            (stub_actual_result, self.expected_result)
        )

    def test_tear_down_calls_delete_if_exists(self):
        """makes sure teardown removes schema from datastore on teardown"""

        self.result.tear_down()
        self.assertTrue(self.mock_schema.delete_if_exists.called)
