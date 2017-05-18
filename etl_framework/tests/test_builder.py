import unittest
from etl_framework.builders import Builder
from etl_framework.BaseConfig import BaseConfig
from etl_framework.Exceptions import DuplicateConfigException

class BuildTestCases(unittest.TestCase):

    def setUp(self):

        self.mock_config = BaseConfig()
        self.mock_config.identifier = "Test"
        self.builder = Builder()

    def test_add_config_twice_raises_exception(self):

        self.builder.add_config(self.mock_config)
        self.assertRaises(
            DuplicateConfigException,
            self.builder.add_config,
            self.mock_config
        )

    def test_add_config(self):

        self.builder.add_config(self.mock_config)

        expected_configs = {
            "Test": self.mock_config,
        }

        self.assertTrue(self.builder.configs, expected_configs)
