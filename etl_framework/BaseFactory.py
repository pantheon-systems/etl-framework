"""Base Factory class"""
#pylint: disable=relative-import

class BaseFactory(object):
    """class to return filter functions"""

    factories = {}

    def __init__(self):
        """"initializes object"""
        pass

    @classmethod
    def add_factory(cls, identifier, factory):
        """adds a factory to Transformer factory"""

        cls.factories[identifier] = factory

    @classmethod
    def create(identifier, params=None):
        """returns object from factory"""
        if identifier not in Factory.factories:
            Factory.factories[identifier] = \
              eval(identifier + '.Factory()')

        return Factory.factories[identifier].create(params)
