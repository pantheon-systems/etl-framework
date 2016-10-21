""" test the environment config """

import os

import unittest

from etl_framework.etl_class import EtlClass

class EtlClassTestCases(unittest.TestCase):
    """ class for test cases """

    JOB_CONFIG_FILEPATH = os.path.join(
        os.path.dirname(__file__),
        'fixtures/job.json'
    )

    class Config(object):
        """Config stub for tests"""

        def __init__(self, config):
            """stuff"""

            self.config_dict = config

        @property
        def field1(self):
            """stuff"""

            return self.config_dict["field1"]

        @field1.setter
        def field1(self, value):
            """stuff"""

            self.config_dict["field1"] = value

        @property
        def field2(self):
            """stuff"""

            return self.config_dict["field2"]

        @field2.setter
        def field2(self, value):
            """stuff"""

            self.config_dict["field2"] = value

    def setUp(self):
        """stuff"""

        self.config = EtlClassTestCases.Config({"field1": "value1", "field2": "value2"})

        self.etl_class = EtlClass(config=self.config)
        setattr(self.etl_class, "field3", "value3")

    def test_set_attribute_on_class(self):
        """Tests set an attribute on the etl_class"""

        self.etl_class.field3 = "test"
        self.assertEqual(self.etl_class.field3, "test")

    def test_get_atribute_on_class(self):
        """Tests getting attribute on etl_class"""

        self.assertEqual(self.etl_class.field3, "value3")

    def test_set_attribute_on_config(self):
        """Tests set attribute on config"""

        self.etl_class.field1 = "test"
        self.assertEqual(self.etl_class.config.field1, "test")

    def test_get_attribute_on_config(self):
        """Tests get attribute on config"""

        self.assertEqual(self.etl_class.field1, "value1")

    def test_set_attribute_raises_error(self):
        """Tests set attribute raises error when attribute doesn't exist"""

        #NOTE this doesnt actually raise an error since you can set whatever attribute you
        # want in python. Not sure yet if that's what we want...

        def test_set():
            self.etl_class.field4 = "value4"

    def test_get_attribute_raises_error(self):
        """Tests get attribute raises error when attribute doesn't exist"""

        def test_get():

            return self.etl_class.field4

        self.assertRaises(AttributeError, test_get)
