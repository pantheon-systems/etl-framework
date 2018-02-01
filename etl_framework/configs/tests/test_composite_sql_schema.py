""" test the environment config """
#pylint: disable=protected-access

import os

import unittest

from etl_framework.SqlSchemaConfig import SqlSchemaConfig
from etl_framework.configs.composite_sql_schema import CompositeSqlSchemaConfig
from etl_framework.configs.tests.fixtures import etl_module
from etl_framework.builders import Builder

class CompositeSqlSchemaConfigTestCases(unittest.TestCase):
    """ class for test cases """

    SCHEMA_CONFIG_FILEPATH = os.path.join(
        os.path.dirname(__file__),
        'fixtures/schema.sql.json'
    )

    COMPOSITE_SCHEMA_CONFIG_FILEPATH = os.path.join(
        os.path.dirname(__file__),
        'fixtures/schema.composite_sql.json'
    )

    def setUp(self):

        self.builder = Builder(etl_module=etl_module)

        self.schema_config = SqlSchemaConfig.create_from_filepath(
            self.SCHEMA_CONFIG_FILEPATH
        )

        self.config = CompositeSqlSchemaConfig.create_from_filepath(
            self.COMPOSITE_SCHEMA_CONFIG_FILEPATH
        )

        self.builder.configs[self.schema_config.identifier] = self.schema_config

    def test__compose_schemas(self):

        self.config._compose_schemas(self.builder)

        # Test that a config attribute is set with the Component Config
        component_schema = list(self.config.component_schemas.values())[0]
        self.assertEqual(component_schema["config"].config, self.schema_config.config)

    def test__compose_unique_keys(self):

        expected_output = [
            ["", ["fieldd"]],
            ["", ["field6"]]
        ]

        self.config._compose_schemas(self.builder)
        self.config._compose_unique_keys()

        self.assertEqual(sorted(expected_output), sorted(self.config.unique_keys))

    def test__compose_indexes(self):

        expected_output = [
            ["", ["fieldb"]],
            ["", ["field5"]]
        ]

        self.config._compose_schemas(self.builder)
        self.config._compose_indexes()

        self.assertEqual(sorted(expected_output), sorted(self.config.indexes))

    def test__compose_fields(self):

        expected_output = {
            "id": "INT(8) AUTO_INCREMENT",
            "record_updated": "timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
            "field5": "varchar(36)",
            "field6": "varchar(36)",
            "fielda": "varchar(36)",
            "fieldc": "varchar(36)",
            "fieldd": "varchar(36)"
        }

        self.config._compose_schemas(self.builder)
        self.config._compose_fields()
        self.assertEqual(expected_output, self.config.fields)
