"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.utilities.DataTable import DataRow

class FieldMappingsMixin(object):
    """stuff"""

    def map_fields(self, row):
        """stuff"""

        mapped_row = DataRow()

        for traverse_path, mapped_field_name in self.config.get_field_mapping_fields().iteritems():
            mapped_row[mapped_field_name] = reduce(lambda key, value: key[value], traverse_path.split('|'), row)

        return mapped_row

    def rename_fields(self, row):
        """stuff"""

        mappings = self.config.get_field_mapping_fields()

        return {value[0]: row[key] for key, value in mappings.iteritems()}

    def filter_fields(self, row):
        """stuff"""

        mappings = self.config.get_field_mapping_fields()

        return {key: value[0](row[key]) for key, value in mappings.iteritems()}

    def map_and_filter_fields(self, row):
        """stuff"""

        mapped_row = DataRow()

        for traverse_path, mapping in self.config.get_field_mappings().iteritems():
            mapped_row[mapping[1]] = mapping[0](reduce(lambda key, value: key[value], traverse_path.split('|'), row))

        return mapped_row

    def rename_and_filter_fields(self, row):
        """stuff"""

        mappings = self.config.get_field_mappings()

        return {value[0]: value[0](row[key]) for key, value in mappings.iteritems()}

