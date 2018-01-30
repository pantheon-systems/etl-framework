"""AlternatingIterator class"""

class AlternatingIterator(object):
    """
    this class creates an iterator that alternates
    yielding from list of iterators
    """

    def __init__(self, component_iterators):
        """stuff"""

        #singleton
        self._iterator = None

        self._component_iterators = list(component_iterators)

    def get_iterator(self):
        """stuff"""

        def alternate_iterate():
            """stuff"""

            while True:

                #shit might raise StopIterationException
                for iterator in self._component_iterators:
                    yield next(iterator)

        if self._iterator is None:
            self._iterator = alternate_iterate()

        return self._iterator

