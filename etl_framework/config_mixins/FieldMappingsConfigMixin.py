"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from method_wrappers.check_config_attr import check_config_attr

class FieldMappingsConfigMixin(object):
    """parses configuration files"""

    FIELDS_ATTR = 'fields'
    FIELD_MAPPINGS = 'field_mappings'

    @check_config_attr
    def get_fields(self):
        """gets identifier for current configuration"""

        return self.config[self.FIELDS_ATTR]

    @check_config_attr
    def get_field_mappings(self):
        """yup"""

        return self.config[self.FIELD_MAPPINGS]

    def get_field_mappings_without_filters(self):
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
