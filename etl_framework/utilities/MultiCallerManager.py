"""stores many MultiCallers"""

import functools

from etl_framework.utilities.MultiCaller import MultiCaller

class MultiCallerManager(object):
    """handles many MultiCallers"""

    def __init__(self, multicaller_creator=None, multicaller_creator_args=None, multicaller_creator_kwargs=None):
        """
        creates instance of MultiCallerManager
        """

        self._multicallers = dict()
        self._multicaller_creator = functools.partial(multicaller_creator,
                                    *multicaller_creator_args,
                                    **multicaller_creator_kwargs)

    def create_multicaller(self, multicaller_id):
        """creates another multi caller using creator"""

        self._multicallers[multicaller_id] = self._multicaller_creator()

    def create_custom_multicaller(self, multicaller_id, args, kwargs):
        """creates another multi caller"""

        self._multicallers[multicaller_id] = MultiCaller(*args, **kwargs)

    def set_threshold(self, multicaller_id, args, kwargs):
        """sets threshold for multicaller"""

        self._multicallers[multicaller_id].set_threshold(*args, **kwargs)

    def set_function(self, multicaller_id, args, kwargs):
        """sets function for multicaller"""

        self._multicallers[multicaller_id].set_function(*args, **kwargs)

    def call_function(self, multicaller_id):
        """calls function for specified multicaller"""

        self._multicallers[multicaller_id].run_function()

    def append_values(self, multicaller_id, next_values):
        """returns next values for multicaller"""

        self._multicallers[multicaller_id].append_values(next_values)

    def iter_multicallers(self):
        """iterates through each multicaller"""

        for caller_id, caller in self._multi_callers:
            yield caller_id, caller

    def multicaller_ids(self):
        """returns multicaller ids"""

        return self._multicallers.keys()

    def multicallers(self):
        """returns multicaller objects"""

        return self._multicallers.values()


