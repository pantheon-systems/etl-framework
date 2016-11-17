""" test the environment config """

import os
import json

import unittest
import mock
from mock import patch, Mock

from etl_framework.Exceptions import EnvironmentSettingNotFoundException
from etl_framework.configs.environment import EnvironmentConfig


class EnvironmentConfigTestCases(unittest.TestCase):
    """ class for test cases """

    @mock.patch.dict(os.environ, {})
    def test_create_environment_variable_attribute_use_default(self):
        """ stuff """

        setting = {
            "type": "environment_variable",
             "from_attribute": "test_from",
             "to_attribute": "test_to",
             "default": "another_value"
        }

        if "test_from" in os.environ:
            os.environ.pop("test_from")

        output = EnvironmentConfig.create_environment_variable_attribute(setting)
        self.assertEqual(output, "another_value")

    @mock.patch.dict(os.environ, {'test_from':'some_value'})
    def test_create_environment_variable_attribute_dont_use_default(self):
        """ stuff """

        setting = {
            "type": "environment_variable",
             "from_attribute": "test_from",
             "to_attribute": "test_to",
             "default": "another_value"
        }

        output = EnvironmentConfig.create_environment_variable_attribute(setting)
        self.assertEqual(output, "some_value")

    @mock.patch.dict(os.environ, {})
    def test_create_environment_variable_attribute_no_default(self):

        setting = {
            "type": "environment_variable",
             "from_attribute": "test_from",
             "to_attribute": "test_to"
        }

        with self.assertRaises(EnvironmentSettingNotFoundException):
            output = EnvironmentConfig.create_environment_variable_attribute(setting)

    @patch('__builtin__.open')
    def test_create_file_attribute_use_default(self, open_mock):

        open_mock.side_effect = IOError()

        setting = {
            "type": "file",
             "from_filepath": "test.txt",
             "to_attribute": "other_value",
             "default": "another_value",
        }

        output = EnvironmentConfig.create_file_attribute(setting)
        self.assertEqual(output, "another_value")

    @patch('__builtin__.open')
    def test_create_file_attribute_dont_use_default(self, open_mock):

        mock_context = Mock()
        mock_enter = Mock()
        mock_exit = Mock()
        mock_file_obj = Mock()
        mock_file_obj.read.return_value = "some_value"
        mock_enter.return_value = mock_file_obj
        setattr(mock_context, '__enter__', mock_enter)
        setattr(mock_context, '__exit__', mock_exit)
        open_mock.return_value = mock_context

        setting = {
            "type": "file",
             "from_filepath": "test.txt",
             "to_attribute": "other_value",
             "default": "another_value",
        }

        output = EnvironmentConfig.create_file_attribute(setting)
        self.assertEqual(output, "some_value")

    @patch('__builtin__.open')
    def test_create_file_attribute_no_default(self, open_mock):

        open_mock.side_effect = IOError()

        setting = {
            "type": "file",
             "from_filepath": "test.txt",
             "to_attribute": "other_value",
        }

        with self.assertRaises(EnvironmentSettingNotFoundException):
            output = EnvironmentConfig.create_file_attribute(setting)

    @patch('__builtin__.open')
    def test_create_json_file_attribute_use_default(self, open_mock):

        open_mock.side_effect = IOError()

        setting = {
            "type": "json_file",
             "from_filepath": "test.txt",
             "to_attribute": "other_value",
             "default": {"another_value": None},
        }

        output = EnvironmentConfig.create_json_file_attribute(setting)
        self.assertEqual(output, {"another_value": None})

    @patch('__builtin__.open')
    def test_create_json_file_attribute_dont_use_default(self, open_mock):

        mock_context = Mock()
        mock_enter = Mock()
        mock_exit = Mock()
        mock_file_obj = Mock()
        mock_file_obj.read.return_value = json.dumps({"some_value": None})
        mock_enter.return_value = mock_file_obj
        setattr(mock_context, '__enter__', mock_enter)
        setattr(mock_context, '__exit__', mock_exit)
        open_mock.return_value = mock_context

        setting = {
            "type": "json_file",
             "from_filepath": "test.txt",
             "to_attribute": "other_value",
             "default": {"another_value": None},
        }

        output = EnvironmentConfig.create_json_file_attribute(setting)
        self.assertEqual(output, {"some_value": None})

    @patch('__builtin__.open')
    def test_create_json_file_attribute_no_default(self, open_mock):

        open_mock.side_effect = IOError()

        setting = {
            "type": "json_file",
             "from_filepath": "test.txt",
             "to_attribute": "other_value",
        }

        with self.assertRaises(EnvironmentSettingNotFoundException):
            output = EnvironmentConfig.create_json_file_attribute(setting)

    def test_set_environment(self):

        # NOTE expected environment depends on what is set in the fixture config
        # At the moment, the expected environment is just the default values of
        # each environment setting.  A more comprehensive test would actually
        # set environment variables and files to test
        expected_environment = {"A": 0, "B": 1, "C": 2}

        config_filepath = os.path.join(
            os.path.dirname(__file__),
            "fixtures",
            "environment.json"
        )

        config = EnvironmentConfig.create_from_filepath(config_filepath)

        config.set_environment()

        self.assertEqual(config.environment, expected_environment)

    def test_get_environment_attribute(self):
        """ tests that environment variable can be accessed as attribute """

        config = EnvironmentConfig()
        config.environment = {"a_field": "a_value"}

        self.assertEqual("a_value", config.a_field)

    def test_get_non_existent_environment_attribute_raises_error(self):

        config = EnvironmentConfig()
        config.environment = {}

        def test_function():

            return config.a_field

        self.assertRaises(KeyError, test_function)


