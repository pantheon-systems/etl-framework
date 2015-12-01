"""utility to store data and call function en masse"""

import functools

class MultiCaller(object):
    """class to store data and call function en masse"""

    def __init__(self, threshold, function, func_args=None, func_kwargs=None):
        """
        creates instance of Segmenter
        """

        if not func_args:
            func_args = []

        if not func_kwargs:
            func_kwargs = {}

        self._threshold = threshold
        self._function = functools.partial(function, *func_args, **func_kwargs)
        self._data = None

        self._reset_data()

    def set_threshold(self, threshold):
        """sets threshold"""

        self._threshold = threshold

    def set_function(self, function):
        """sets function"""

        self._function = function

    def call_function(self):
        """helper method to run statement with set chunk values"""

        #run statement if there are chunk_values
        if self._chunk_values:

            output = self._function(self._data)
            self._reset_data()

            return output

    def append_values(self, next_values):
        """appends to chunk values and writes to db when _chunk_size is reached"""

        self._data.append(next_values)
        if len(self._data) >= self._threshold:
            self.call_function()

    def _reset_data(self):
        """resets values to empty tuple"""

        self._data = list()
