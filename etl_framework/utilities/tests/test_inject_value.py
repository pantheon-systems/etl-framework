""" test the environment config """
import unittest

from etl_framework.utilities.transform_helpers import inject_value

class InjectValueTestCases(unittest.TestCase):
    """ class for test cases """

    def test_inject_value_with_list_of_dicts(self):
        field_mapping = {
            "from_path": ["c", "e"],
            "to_path": ["field1", "field2"],
            "filter_function": lambda x: x
        }

        input_row = {
            "a": None,
            "b": None,
            "c": [{"e": "f"}, {"e": "g"}, {"e": "h"}]
        }
        output_row = {}
        expected_output = {
            "field1": [
            {"field2": "f"},
            {"field2": "g"},
            {"field2": "h"}
            ],
        }

        inject_value(
            input_row,
            output_row,
            field_mapping["from_path"],
            field_mapping["to_path"],
            field_mapping["filter_function"]
        )

        self.assertEqual(output_row, expected_output)

    def test_inject_value_with_list_of_dicts_of_list(self):
        field_mapping = {
            "from_path": ["c", "e", "f"],
            "to_path": ["field1", "field2", "field3"],
            "filter_function": lambda x: x
        }


        input_row = {
            "a": None,
            "b": None,
            "c": [{"e": [{"f": "1"}]}, {"e": [{"f": "2"}]}, {"e": [{"f": "3"}]}]
        }

        output_row = {}
        expected_output = {
            "field1": [
            {"field2": [{"field3": "1"}]},
            {"field2": [{"field3": "2"}]},
            {"field2": [{"field3": "3"}]}
            ],
        }

        inject_value(
            input_row,
            output_row,
            field_mapping["from_path"],
            field_mapping["to_path"],
            field_mapping["filter_function"]
        )

        self.assertEqual(output_row, expected_output)

    def test_inject_value_with_list_of_dicts_of_list_of_values(self):
        field_mapping = {
            "from_path": ["c", "e"],
            "to_path": ["field1", "field2"],
            "filter_function": lambda x: x
        }

        input_row = {
            "a": None,
            "b": None,
            "c": [{"e": [1, 2]}, {"e": [3, 4]}, {"e": [5, 6]}]
        }
        output_row = {}
        expected_output = {
            "field1": [
            {"field2": [1, 2]},
            {"field2": [3, 4]},
            {"field2": [5, 6]}
            ],
        }

        inject_value(
            input_row,
            output_row,
            field_mapping["from_path"],
            field_mapping["to_path"],
            field_mapping["filter_function"]
        )

        self.assertEqual(output_row, expected_output)

    def test_inject_value_with_list(self):
        field_mapping = {
            "from_path": ["c"],
            "to_path": ["field1"],
            "filter_function": lambda x: x
        }

        input_row = {
            "a": None,
            "b": None,
            "c": ["something"]
        }
        output_row = {}
        expected_output = {
            "field1": ["something"],
        }

        inject_value(
            input_row,
            output_row,
            field_mapping["from_path"],
            field_mapping["to_path"],
            field_mapping["filter_function"]
        )

        self.assertEqual(output_row, expected_output)
