"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from etl_framework.utilities.DataTraverser import DataTraverser

class FieldSplitsMixin(object):
    """stuff"""

    def iter_split_fields(self, row):
        """gets destination for current configuration"""

        for destination, fields in self.config.get_field_splits().items():
            for output_row in DataTraverser.normalize(row, fields):
                yield destination, output_row

