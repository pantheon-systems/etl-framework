"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated

from Loader import Loader
from loader_mixins.BufferMixin import BufferMixin

class BufferedLoader(Loader, BufferMixin):
    """loads data into database"""

    def load(self, values):
        """stuff"""

        self.write_to_buffer(values)
