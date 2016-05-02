"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class BatchMixin(object):
    """parses configuration files"""

    BATCH_MAX_COUNT_ATTR = 'batch_max_count'
    BATCH_SIZE_ATTR = 'batch_size'

    @check_config_attr_default_none
    def get_batch_max_count(self):
        """stuff"""

        return self.config[self.BATCH_MAX_COUNT_ATTR]

    @check_config_attr_default_none
    def get_batch_size(self):
        """stuff"""

        return self.config[self.BATCH_SIZE_ATTR]

