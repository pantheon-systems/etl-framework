"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from etl_framework.config_mixins.AddFiltersMixin import AddFiltersMixin
from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class FiltersMixin(AddFiltersMixin):
    """parses configuration files"""

    PRE_FILTER_ATTR = 'pre_filter'
    FILTER_ATTR = 'filter'
    POST_FILTER_ATTR = 'post_filter'

    def add_filters(self, filter_mappings):
        """override add_filters method of config object"""

        super(FiltersMixin, self).add_filters(filter_mappings)

        self.set_pre_filter(filter_mappings.get(self.get_pre_filter()))
        self.set_filter(filter_mappings.get(self.get_filter()))
        self.set_post_filter(filter_mappings.get(self.get_post_filter()))

    @check_config_attr_default_none
    def get_pre_filter(self):
        """stuff"""

        return self.config[self.PRE_FILTER_ATTR]

    @check_config_attr_default_none
    def get_filter(self):
        """stuff"""

        return self.config[self.FILTER_ATTR]

    @check_config_attr_default_none
    def get_post_filter(self):
        """stuff"""

        return self.config[self.POST_FILTER_ATTR]

    @check_config_attr_default_none
    def set_pre_filter(self, pre_filter):
        """stuff"""

        self.config[self.PRE_FILTER_ATTR] = pre_filter

    @check_config_attr_default_none
    def set_filter(self, filter_function):
        """stuff"""

        self.config[self.FILTER_ATTR] = filter_function

    @check_config_attr_default_none
    def set_post_filter(self, post_filter):
        """stuff"""

        self.config[self.POST_FILTER_ATTR] = post_filter
