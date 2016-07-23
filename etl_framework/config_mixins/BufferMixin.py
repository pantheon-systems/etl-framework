"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class BufferMixin(object):
    """parses configuration files"""

    BUFFER_SIZE_ATTR = 'buffer_size'

    @check_config_attr_default_none
    def get_buffer_size(self):
        """stuff"""

        return self.config[self.BUFFER_SIZE_ATTR]

    @check_config_attr_default_none
    def set_buffer_size(self, buffer_size):
        """stuff"""

        self.config[self.BUFFER_SIZE_ATTR] = buffer_size

