"""adds Buffer functionality to Loader"""

class BufferMixin(object):
    """stuff"""

    def __init__(self, *args, **kwargs):
        """initializes base data loader"""

        super(BufferMixin, self).__init__(*args, **kwargs)

        self._buffered_values = None
        #set _buffered_values
        self._reset_buffered_values()

    def flush_buffer(self):
        """helper method to run statement with set buffered values"""

        #run statement if there are buffered_values
        if self._buffered_values:
            self.run_statement(self.config.get_sql_statement(), multiple_values=self._buffered_values, commit=True)
            self._reset_buffered_values()

    def write_to_buffer(self, next_values):
        """appends to buffered values and writes to db when _buffer_size is reached"""

        #assume next_values is DataRow object
        self._buffered_values.append(next_values.row_values(self.config.get_sql_fields()))

        if len(self._buffered_values) >= self.config.get_buffer_size():
            self.flush_buffer()

    def _reset_buffered_values(self):
        """resets values to empty tuple"""

        self._buffered_values = list()
