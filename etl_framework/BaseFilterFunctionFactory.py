"""returns filter functions for Transformer object"""

import abc

class BaseFilterFunctionFactory(object):
    """class to return filter functions"""

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, *args, **kwargs):
        """"initializes object"""

        super(BaseFilterFunctionFactory, self).__init__(*args, **kwargs)

    @classmethod
    def get_filter_function(cls, filter_function_name):
        """returns filter function"""
        raise NotImplementedError
