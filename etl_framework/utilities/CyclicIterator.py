"""Cyclic Iterator class"""

from etl_framework.Exceptions import BadIteratorException

class CyclicIterator(object):
    """class that wraps iterator to make it cycle and flags if its cycled once"""

    def __init__(self, creator, *args, **kwargs):
        """creator is method to create iterator"""

        self._cycled = False
        self._component_iterator = None

        #singleton
        self._iterator = None

        self._args = args or []
        self._kwargs = kwargs or {}
        self._creator = creator

        self.check_valid_iterator()

        self._set_component_iterator()


    def check_valid_iterator(self):
        """checks that iterator iterates something"""

        test_iterator = self._creator(*self._args, **self._kwargs)

        try:
            next(test_iterator)
        except StopIteration:
            raise BadIteratorException('Iterator doesnt yield anything')

    def get_iterator(self):
        """return iterator"""

        def iterate():
            """yields elements from iterator"""

            while True:
                for value in self._component_iterator:
                    yield value

                self._cycled = True
                self._set_component_iterator()

        if self._iterator is None:
            self._iterator = iterate()

        return self._iterator

    def is_cycled(self):

        return self._cycled

    def _create_component_iterator(self):
        """recreates iterator"""

        return self._creator(*self._args, **self._kwargs)

    def _set_component_iterator(self):
        """recreates iterator"""

        self._component_iterator = self._create_component_iterator()
