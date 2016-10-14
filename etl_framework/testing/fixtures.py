""" fixture base classes for testing """

class FixtureInterface(object):
    """One fixture should have exactly 1 schema"""

    def __init__(self, schema, data):
        """
        data : list of dictionaries
        schema : schema object created from SchemaConfig
        """

        self.schema = schema
        self.data = data

    def load(self):
        """loads fixtures"""

        raise NotImplementedError

    def set_up(self):
        """set_up should create table from schema"""

        self.schema.create_if_not_exists()
        self.load()

    def tear_down(self):
        """tears down table"""

        self.schema.delete_if_exists()

class Fixtures(object):
    """list of fixtures for a specific test or type or test"""

    def __init__(self, fixtures):
        """fixtures is list of fixture objects"""

        self.fixtures = fixtures

    def set_up(self):
        """imports fixtures to datastore"""

        for fixture in self.fixtures:
            fixture.set_up()

    def tear_down(self):
        """removes fixtures from datastore"""

        for fixture in self.fixtures:
            fixture.tear_down()
