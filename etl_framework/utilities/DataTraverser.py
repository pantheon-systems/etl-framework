"""utility to traverse JSON-like data"""

import json
import itertools

from etl_framework.Exceptions import BadIteratorException
from etl_framework.utilities.DataTable import DataRow
from etl_framework.utilities.CyclicIterator import CyclicIterator
from etl_framework.utilities.AlternatingIterator import AlternatingIterator

class DataTraverser(object):
    """class to traverse JSON-like data"""

    def __init__(self):
        """creates instance of Segmenter"""

        pass

    @staticmethod
    def get_value(data, key, default_value=None):
        """returns value from JSON-like data"""

        if data is None:
            #print 'WARNING: Traversed data is None with key: {0}'.format(key)
            return None

        elif isinstance(data, str):
            data = json.loads(data)

        return data.get(key, default_value)

    @staticmethod
    def traverse_path(source_data, field_path):
        """helper method to traverse data for given field path"""

        if isinstance(source_data, str):
            source_data = json.loads(source_data)

        if len(field_path) > 1:
            if isinstance(source_data, list):
                if field_path[0] is None:
                    for element in source_data:
                        for value in DataTraverser.traverse_path(element, field_path[1:]):
                            yield value
                else:
                    for element in source_data:
                        for value in DataTraverser.traverse_path(element[field_path[0]], field_path[1:]):
                            yield value
            else:
                if field_path[0] is None:
                    for value in DataTraverser.traverse_path(source_data[field_path[0]], field_path[1:]):
                        yield value
                else:
                    for value in DataTraverser.traverse_path(source_data[field_path[0]], field_path[1:]):
                        yield value
        else:
            if isinstance(source_data, list):
                if field_path[0] is None:
                    for element in source_data:
                        yield element
                else:
                    for element in source_data:
                        yield element[field_path[0]]
            else:
                if field_path[0] is None:
                    yield source_data
                else:
                    yield source_data[field_path[0]]

    @staticmethod
    def normalize(source_data, fields):
        """
        yields normalized data
        field_paths : {[fieldname, [path_element1, path_element2, ...], ...}
        """

        field_names, field_paths = list(zip(*iter(fields.items())))

        #if there are iterators that yield nothing, BadIterationException will be raised
        try:
            field_iterators = [CyclicIterator(DataTraverser.traverse_path, source_data, path)
                            for path in field_paths]
        except BadIteratorException:
            return

        alternating_iterator = AlternatingIterator([iterator.get_iterator()
                                                    for iterator in field_iterators]).get_iterator()

        while True:
            row = DataRow({name: value for name, value in zip(field_names, alternating_iterator)})

            #only yield row if not all iterators have cycled through
            if not all(field_iterator.is_cycled() for field_iterator in field_iterators):
                yield row
            else:
                break
