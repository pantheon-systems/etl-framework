"""tests DataTraverser class"""
#pylint: disable=too-many-locals

import unittest

from etl_framework.utilities.DataTraverser import DataTraverser

class DataTraverserTestCases(unittest.TestCase):
    """stuff"""

    def setUp(self):
        """stuff"""

        pass

    def tearDown(self):
        """stuff"""

        pass

    def test_normalize(self):
        """tests normalize method"""

        source_data = {'id': 'id_value',
                        'id2': 'id_value2',
                        'names': ['name1', 'name2', 'name3'],
                        'values': ['value1', 'value2', 'value3'],
                        'name_values': [
                                    {'name': 'name1', 'value': 'value1'},
                                    {'name': 'name2', 'value': 'value2'},
                                    {'name': 'name3', 'value': 'value3'}
                                        ]
                        }

        field_paths1 = [['output_id', ['id']],
                        ['output_id2', ['id2']]
                        ]
        expected_output1 = [
                            {'output_id': 'id_value', 'output_id2': 'id_value2'}
                            ]

        field_paths2 = [['output_id', ['id']]
                        ]

        expected_output2 = [
                            {'output_id': 'id_value'}
                            ]

        field_paths3 = [['output_id', ['id']],
                        ['output_name', ['names', None]]
                        ]
        expected_output3 = [
                                {'output_id': 'id_value', 'output_name': 'name1'},
                                {'output_id': 'id_value', 'output_name': 'name2'},
                                {'output_id': 'id_value', 'output_name': 'name3'}
                            ]

        field_paths4 = [['output_id', ['id']],
                        ['output_name', ['names', None]],
                        ['output_value', ['values', None]]
                        ]
        expected_output4 = [
                                {'output_id': 'id_value', 'output_name': 'name1', 'output_value': 'value1'},
                                {'output_id': 'id_value', 'output_name': 'name2', 'output_value': 'value2'},
                                {'output_id': 'id_value', 'output_name': 'name3', 'output_value': 'value3'}
                            ]

        field_paths5 = [['output_id', ['id']],
                        ['output_name', ['name_values', 'name']],
                        ['output_value', ['name_values', 'value']]
                        ]
        expected_output5 = [
                                {'output_id': 'id_value', 'output_name': 'name1', 'output_value': 'value1'},
                                {'output_id': 'id_value', 'output_name': 'name2', 'output_value': 'value2'},
                                {'output_id': 'id_value', 'output_name': 'name3', 'output_value': 'value3'}
                            ]

        inputs = [field_paths1, field_paths2, field_paths3, field_paths4, field_paths5]
        expected_outputs = [expected_output1, expected_output2, expected_output3, expected_output4, expected_output5]

        unformatted_fail_string = '\nFailed with\ninput: {0}\nexpected output:{1}\noutput:{2}\n'

        for input_val, expected_output in zip(inputs, expected_outputs):

            output = [row for row in DataTraverser.normalize(source_data, input_val)]

            self.assertTrue(output == expected_output,
                            unformatted_fail_string.format(input_val, expected_output, output))
