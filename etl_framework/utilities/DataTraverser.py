"""utility to traverse JSON-like data"""

import json

from etl_framework.utilities.DataTable import DataRow

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

        elif isinstance(data, basestring):
            data = json.loads(data)

        return data.get(key, default_value)

    @staticmethod
    def traverse_path(source_data, field_path):
        """helper method to traverse data for given field path"""

        if isinstance(source_data, basestring):
            source_data = json.loads(source_data)

        if source_data is None:
            yield None

        elif len(field_path) > 1:
            if isinstance(source_data, list):
                if field_path[0] is None:
                    for element in source_data:
                        for value in DataTraverser.traverse_path(element, field_path[1:]):
                            yield value
                else:
                    for element in source_data:
                        for value in DataTraverser.traverse_path(element.get(field_path[0]), field_path[1:]):
                            yield value
            else:
                if field_path[0] is None:
                    for value in DataTraverser.traverse_path(source_data.get(field_path[0]), field_path[1:]):
                        yield value
                else:
                    for value in DataTraverser.traverse_path(source_data.get(field_path[0]), field_path[1:]):
                        yield value
        else:
            if isinstance(source_data, list):
                if field_path[0] is None:
                    for element in source_data:
                        yield element
                else:
                    for element in source_data:
                        yield element.get(field_path[0])
            else:
                if field_path[0] is None:
                    yield source_data
                else:
                    yield source_data.get(field_path[0])

    @staticmethod
    def normalize(source_data, field_paths):
        """yields normalized data"""

        if len(field_paths) > 1:
            for value in DataTraverser.traverse_path(source_data, field_paths[0][1]):
                data = {field_paths[0][0]: value}
                for extra_data in DataTraverser.normalize(source_data, field_paths[1:]):
                    yield DataRow(data, **extra_data)
        else:
            for value in DataTraverser.traverse_path(source_data, field_paths[0][1]):
                yield DataRow({field_paths[0][0]: value})

