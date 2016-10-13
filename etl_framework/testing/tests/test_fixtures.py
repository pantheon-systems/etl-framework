"""tests fixtures"""
import unittest
from mock import MagicMock

from etl_framework.testing.fixtures import FixtureInterface, \
    Fixtures

class FixturesTestCases(unittest.TestCase):
    """class for fixtures tests"""

    def setUp(self):

        # Mocks
        self.fixture1 = MagicMock()
        self.fixture2 = MagicMock()
        self.fixtures = Fixtures([
            self.fixture1,
            self.fixture2,
        ])

    def test_set_up_calls_each_fixture_set_up(self):
        """stuff"""

        self.fixtures.set_up()

        self.assertTrue(self.fixture1.set_up.called)
        self.assertTrue(self.fixture2.set_up.called)

    def test_tear_down_calls_each_fixture_tear_down(self):
        """stuff"""

        self.fixtures.tear_down()

        self.assertTrue(self.fixture1.tear_down.called)
        self.assertTrue(self.fixture2.tear_down.called)

class FixtureInterfaceTestCases(unittest.TestCase):
    """class for fixture tests"""

    def setUp(self):

        # Mocks
        self.mock_schema = MagicMock()
        self.data = [{}]
        FixtureInterface.load = MagicMock()

        self.fixture = FixtureInterface(
            schema=self.mock_schema,
            data=self.data
        )

    def test_set_up_calls_schema_create_if_not_exists(self):
        """test fixture interface"""

        self.fixture.set_up()

        self.assertTrue(self.mock_schema.create_if_not_exists.called)

    def test_tear_down_calls_delete_if_exists(self):
        """makes sure teardown removes schema from datastore on teardown"""

        self.fixture.tear_down()
        self.assertTrue(self.mock_schema.delete_if_exists.called)
