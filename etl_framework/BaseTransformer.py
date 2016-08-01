"""Base class to transform extracted data before loading"""
#pylint: disable=relative-import
# NOTE This will be deprecated. Dont use.
import abc

from method_wrappers.check_attr_set import _check_attr_set

class BaseTransformer(object):
    """transforms extracted data and filters"""

    __metaclass__ = abc.ABCMeta

    def __init__(self, field_mappings=None, filter_function=None, identifier=None, *args, **kwargs):
        """initialized BaseTransformer"""

        super(BaseTransformer, self).__init__(*args, **kwargs)

        self.field_mappings = None
        self.filter_function = None
        self.identifier = None

        self.set_attributes(field_mappings, filter_function, identifier)

    def set_attributes(self, field_mappings, filter_function, identifier):
        """sets all attributes of Transformer object"""

        self.field_mappings = field_mappings
        self.filter_function = filter_function
        self.identifier = identifier

    @abc.abstractmethod
    def _rename_columns(self, filtered_data):
        """rename columns of filtered data"""

    @abc.abstractmethod
    def _transform_data(self, data):
        """returns transformed data"""

    @staticmethod
    def _set_subtract(list1, list2):
        """returns list1 set-subtract list2 as list object"""

        return list(set(list1) - set(list2))

    @staticmethod
    def _split_data(data_row, field_splits):
        """splits data into different parts (to write to multiple tables etc.)"""

        for field_names in field_splits:

            row = data_row.row_values(field_names)

            yield row, field_names

    def _filter_data(self, transformed_data):
        """filters transformed data"""

        return self.filter_function(transformed_data)


    @_check_attr_set('field_mappings')
    @_check_attr_set('filter_function')
    @_check_attr_set('identifier')
    def run_transformation(self, data, field_splits=None):
        """transforms, filters, and renames data"""

        #print 'Transforming data for {0}'.format(self.identifier)

        data = self._transform_data(data)
        data = self._filter_data(data)
        data = self._rename_columns(data)

        #if field splits isn't given, assume no splitting of data
        if field_splits is None:
            field_splits = [None]
        else:
            pass

        #return generator object
        for values, fieldnames in self._split_data(data, field_splits):
            yield values, fieldnames
