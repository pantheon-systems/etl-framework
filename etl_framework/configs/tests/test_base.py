""" test the environment config """

import unittest

from etl_framework.BaseConfig import BaseConfig
from etl_framework.configs.tests.fixtures import etl_module
from etl_framework.builders import Builder

class BaseConfigTestCases(unittest.TestCase):
    """ class for test cases """

    def setUp(self):


        self.builder = Builder(etl_module=etl_module)

        self.config = BaseConfig()

    def test_create_subclasses_with_identifiers(self):

        self.builder.etl_classes = {
            "etl_class.1": etl_module.TestClass1(None),
            "etl_class.2": etl_module.TestClass2(None)
        }

        self.builder.configs = {
            "etl_class.1": etl_module.TestConfig1(
                config_dict={
                    "config_class": "TestConfig1",
                    "etl_class": "TestClass1"
                }
            ),
            "etl_class.2": etl_module.TestConfig2(
                config_dict={
                    "config_class": "TestConfig2",
                    "etl_class": "TestClass2"
                }
            ),
        }

        self.builder.etl_classes = {
            "etl_class.1": etl_module.TestClass1(config=self.builder.configs["etl_class.1"]),
            "etl_class.2": etl_module.TestClass2(config=self.builder.configs["etl_class.2"])
        }

        config_dict = {
            "a_config__by_identifier":  "etl_class.1",
            "field_a": {
                "b_config__by_identifier": "etl_class.2"
            },
            "field_b": "value_b",
            "a_configs__by_identifiers": ["etl_class.1", "etl_class.2"]
        }

        self.config.create_subclasses(config_dict, self.builder)

        self.assertEqual(config_dict["field_b"], "value_b")

        self.assertTrue(isinstance(config_dict["a_config"], etl_module.TestClass1))
        self.assertTrue("a_config__config" not in config_dict)

        self.assertTrue(isinstance(config_dict["field_a"]["b_config"], etl_module.TestClass2))
        self.assertTrue("b_config__config" not in config_dict["field_a"])

        self.assertTrue(isinstance(config_dict["a_configs"][0], etl_module.TestClass1))
        self.assertTrue(isinstance(config_dict["a_configs"][1], etl_module.TestClass2))
        self.assertTrue("a_configs__configs" not in config_dict)

    def test_create_subclasses_with_nested_configs(self):
        """ stuff """

        Config1 = {
            "identifier": "config1",
            "etl_class": "TestClass1",
            "config_class": "TestConfig1"
        }

        Config2 = {
            "identifier": "config2",
            "etl_class": "TestClass2",
            "config_class": "TestConfig2"
        }

        config_dict = {
            "a_config__config":  Config1,
            "field_a": {
                "b_config__config": Config2
            },
            "field_b": "value_b",
            "a_configs__configs": [Config1, Config2]
        }

        self.config.create_subclasses(config_dict, self.builder)

        self.assertEqual(config_dict["field_b"], "value_b")

        self.assertTrue(isinstance(config_dict["a_config"], etl_module.TestClass1))
        self.assertTrue("a_config__config" not in config_dict)

        self.assertTrue(isinstance(config_dict["field_a"]["b_config"], etl_module.TestClass2))
        self.assertTrue("b_config__config" not in config_dict["field_a"])

        self.assertTrue(isinstance(config_dict["a_configs"][0], etl_module.TestClass1))
        self.assertTrue(isinstance(config_dict["a_configs"][1], etl_module.TestClass2))
        self.assertTrue("a_configs__configs" not in config_dict)

