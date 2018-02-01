class DataRow(dict):
    """object for holding row of data"""

    def __init__(self, *args, **kwargs):
        """creates instance of DataRow"""

        super(DataRow, self).__init__(*args, **kwargs)

        self.target_table = None

    def row_values(self, field_names, default_value=None):
        """returns row value of specified field_names"""

        return tuple(self.get(field_name, default_value) for field_name in field_names)

    def set_target_table(self, target_table):
        """sets target table attribute"""

        self.target_table = target_table

    def get_target_table(self):
        """returns target table attribute"""

        return self.target_table

class DataTable(object):
    """object for holding data"""

    def __init__(self, data, keys=None):
        """instantiates Table object with rows(which should be a list of dictionaries)"""

        self.rows = list(data)

        #set keys as _keys of first row by default
        if keys:
            self._keys = keys
        else:
            self._keys = list(self.rows[0].keys())

    def keys(self):
        """returns keys of Table"""

        return self._keys

    def append_row(self, row):
        """adds another row to table"""

        self.rows.append(row)

    def iterrows(self, field_names, default_value=None):
        """generator that yields specified fields for each row"""

        for row in self.rows:
            yield tuple(row.get(field_name, default_value) for field_name in field_names)
