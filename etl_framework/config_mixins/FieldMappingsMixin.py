"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class FieldMappingsMixin(object):
    """parses configuration files"""

    FIELD_MAPPINGS = 'field_mappings'

    @check_config_attr_default_none
    def get_field_mappings(self):
        """yup"""

        return self.config[self.FIELD_MAPPINGS]

    def get_field_mapping_fields(self):
        """returns only fields (without filters)"""

        return {key: value[1] for key, value in self.get_field_mappings().iteritems()}

    def get_field_mappings_without_target_fields(self):
        """returns only fields (without filters)"""

        return {key: value[0] for key, value in self.get_field_mappings().iteritems()}

    def get_field_mapping_target_fields(self):
        """returns target fields"""

        return tuple(value[1] for value in self.get_field_mappings().values())

    def get_field_mapping_source_fields(self):
        """returns source fields"""

        return tuple(key for key in self.get_field_mappings().keys())
