"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class FieldSplitsMixin(object):
    """parses configuration files"""

    FIELD_SPLITS_ATTR = 'field_splits'

    @check_config_attr_default_none
    def get_field_splits(self):
        """yup"""

        return self.config[self.FIELD_SPLITS_ATTR]

    def get_field_split_destinations(self):
        """yup"""

        return self.get_field_splits().keys()

    def get_field_split_field_names(self):
        """yup"""

        return {key: tuple(field[0]for field in fields)
                for key, fields in self.get_field_splits().iteritems()}

