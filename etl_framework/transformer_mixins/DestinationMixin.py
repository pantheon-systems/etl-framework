"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.method_wrappers.check_config_attr import check_config_attr

class DestinationMixin(object):
    """parses configuration files"""

    def get_destination(self):
        """stuff"""

        return self.config.get_destination()

    @check_config_attr
    def choose_destination(self, criteria):
        """stuff"""

        return self.config.get_destination_chooser()(criteria)

