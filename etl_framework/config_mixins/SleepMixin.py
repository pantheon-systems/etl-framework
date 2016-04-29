"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class SleepMixin(object):
    """parses configuration files"""

    SLEEP_MIN_TIME_ATTR = 'sleep_min_time'
    SLEEP_MAX_TIME_ATTR = 'sleep_max_time'

    @check_config_attr_default_none
    def get_sleep_min_time(self):
        """stuff"""

        return self.config[self.SLEEP_MIN_TIME_ATTR]

    @check_config_attr_default_none
    def get_sleep_max_time(self):
        """stuff"""

        return self.config[self.SLEEP_MAX_TIME_ATTR]

