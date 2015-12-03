"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.method_wrappers.check_config_attr import check_config_attr

class DestinationMixin(object):
    """parses configuration files"""

    DESTINATION_ATTR = 'destination'
    DESTINATION_CHOOSER_ATTR = 'destination_chooser'

    @check_config_attr
    def get_destination(self):
        """gets destination for current configuration"""

        return self.config[self.DESTINATION_ATTR]

    @check_config_attr
    def get_destination_chooser(self):
        """gets destination chooser function"""

        return self.config[self.DESTINATION_CHOOSER_ATTR]

