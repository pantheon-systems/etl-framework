"""parses configuration and returns useful things"""
#pylint: disable=relative-import
from etl_framework.utilities.DataTable import DataRow
from functools import reduce

class FieldMappingsMixin(object):
    """stuff"""

    def map_fields(self, row):
        """stuff"""

        mapped_row = DataRow()

        for traverse_path, mapped_field_name in list(self.config.get_field_mapping_fields().items()):
            mapped_row[mapped_field_name] = reduce(lambda key, value: key.get(value) if key else None,
                                                    traverse_path.split('|'), row)

        return mapped_row

    def rename_fields(self, row):
        """stuff"""

        mappings = self.config.get_field_mapping_fields()

        return {value[0]: row.get(key) for key, value in list(mappings.items())}

    def filter_fields(self, row):
        """stuff"""

        mappings = self.config.get_field_mapping_fields()

        return {key: value[0](row.get(key)) for key, value in list(mappings.items())}

    def filter_and_map_fields(self, row):
        """stuff"""

        mapped_row = DataRow()

        for traverse_path, mapping in list(self.config.get_field_mappings().items()):
            mapped_row[mapping[1]] = mapping[0](reduce(lambda key, value: key.get(value) if key else None,
                                                        traverse_path.split('|'), row))

        return mapped_row

    def filter_and_rename_fields(self, row):
        """stuff"""

        mappings = self.config.get_field_mappings()

        return {value[0]: value[0](row.get(key)) for key, value in list(mappings.items())}

