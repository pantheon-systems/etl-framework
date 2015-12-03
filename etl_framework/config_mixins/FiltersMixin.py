"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.method_wrappers.check_config_attr import check_config_attr

class FiltersMixin(object):
    """parses configuration files"""

    PRE_FILTER_ATTR = 'pre_filter'
    FILTER_ATTR = 'filter'
    POST_FILTER_ATTR = 'post_filter'

    @check_config_attr
    def get_pre_filter(self):
        """stuff"""

        return self.config[self.PRE_FILTER_ATTR]

    @check_config_attr
    def get_filter(self):
        """stuff"""

        return self.config[self.FILTER_ATTR]

    @check_config_attr
    def get_post_filter(self):
        """stuff"""

        return self.config[self.POST_FILTER_ATTR]
