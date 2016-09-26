"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated

from etl_framework.loaders.PostgreSqlLoader import PostgreSqlLoader
from etl_framework.loader_mixins.BufferMixin import BufferMixin


class PostgreSqlBufferedLoader(PostgreSqlLoader, BufferMixin):
    """loads data into database"""

    def load_buffered(self):

        # NOTE this is specific to SQL loaders,
        # so this class could be renamed SqlBufferedLoader
        self.run_statement(
            self.config.get_sql_statement(),
            multiple_values=self._buffered_values,
            commit=True
        )

    def load(self, values):
        """stuff"""

        self.write_to_buffer(values)
