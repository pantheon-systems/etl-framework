"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.config_mixins.AddDestinationMixin import AddDestinationMixin
from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class DestinationMixin(AddDestinationMixin):
    """parses configuration files"""

    DESTINATION_ATTR = 'destination'
    DESTINATION_CHOOSER_ATTR = 'destination_chooser'

    def add_destination_chooser(self, destination_chooser_mappings):
        """chooses destination based on identifier of ETL entity"""

        self.set_destination_chooser(destination_chooser_mappings[self.get_identifier()])

    @check_config_attr_default_none
    def get_destination(self):
        """gets destination for current configuration"""

        return self.config[self.DESTINATION_ATTR]

    @check_config_attr_default_none
    def get_destination_chooser(self):
        """gets destination chooser function"""

        return self.config[self.DESTINATION_CHOOSER_ATTR]

    @check_config_attr_default_none
    def set_destination_chooser(self, destination_chooser):
        """gets destination chooser function"""

        self.config[self.DESTINATION_CHOOSER_ATTR] = destination_chooser
