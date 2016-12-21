"""parses configuration and returns useful things"""
#pylint: disable=relative-import
#pylint cant tell AddFiltersMixin is new class
#pylint: disable=super-on-old-class

from etl_framework.Exceptions import ConfigAttrNotSetException
from etl_framework.config_mixins.AddFiltersMixin import AddFiltersMixin
from etl_framework.method_wrappers.check_config_attr import check_config_attr_default_none

class FieldMappingsMixin(AddFiltersMixin):
    """parses configuration files"""

    FIELD_MAPPINGS = 'field_mappings'

    def add_filters(self, filter_mappings):
        """override add_filters method of config object"""

        super(FieldMappingsMixin, self).add_filters(filter_mappings)

        if self.get_field_mappings():
            self.set_field_mappings({key: [filter_mappings.get(value[0]), value[1]]
                                        for key, value in self.get_field_mappings().iteritems()})
        else:
            raise ConfigAttrNotSetException

    def add_filters_from_module(self, filters_module):
        """override add_filters_from_module method of config object"""

        super(FieldMappingsMixin, self).add_filters_from_module(filters_module)
        if self.get_field_mappings():
            self.set_field_mappings({key: [getattr(filters_module, value[0]), value[1]]
                                        for key, value in self.get_field_mappings().iteritems()})
        else:
            raise ConfigAttrNotSetException

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

    @check_config_attr_default_none
    def set_field_mappings(self, field_mappings):
        """yup"""

        self.config[self.FIELD_MAPPINGS] = field_mappings
