"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated

import abc
from contextlib import closing

from method_wrappers.check_attr_set import _check_attr_set

class BaseDataLoader(object):
    """loads data into database"""

    __metaclass__ = abc.ABCMeta

    def __init__(self, db_credentials=None, *args, **kwargs):
        """initializes base data loader"""

        super(BaseDataLoader, self).__init__(*args, **kwargs)

        self.con = None
        self.db_credentials = None

        #set db_credentials only if argument was set
        if db_credentials:
            self.set_db_credentials(db_credentials)

    @classmethod
    def create_from_dsn(cls, dsn):
        """creates instance of data_loader from dsn (i.e. a sql url)"""

        raise NotImplementedError

    def set_db_credentials_from_dsn(self, dsn):
        """sets credentials from dsn"""

        self.set_db_credentials(self.parse_dsn(dsn))

    @staticmethod
    def parse_dsn(dsn):
        """
        parse dsn and outputs credential parameters
        """
        raise NotImplementedError

    @abc.abstractmethod
    def set_db_credentials(self, db_credentials):
        """sets the database credentials"""

    def _get_connection(self, new_con=True):
        """returns connection object to data warehouse"""

        #clear saved connnection if you want a new connection
        if new_con:
            self._clear_connection()
            print 'Creating new connection'
            return self._create_connection()

        #check if self.con is set
        elif not self.con:
            raise Exception('Cant return old connection object. self.con isnt set!')

        #return old connection
        else:
            print 'Reusing old connection'
            return self.con

    @abc.abstractmethod
    def _create_connection(self):
        """returns a new connection object"""


    @abc.abstractmethod
    def _clear_connection(self):
        """
        sets self.con value to None and closes existing connection
        """

    def _save_connection(self, con):
        """
        saves connection to self.con
        """

        #first clear old connection
        print '\nSaving sql connection\n'
        self._clear_connection()

        self.con = con

    @staticmethod
    def yield_chunks(data, chunk_size):
        """Yield successive n-sized chunks from data"""

        for offset in xrange(0, len(data), chunk_size):
            print 'Yielding data rows: %d to %d'%(offset, min(len(data), offset+chunk_size) -1)
            yield data[offset:offset+chunk_size]

    @_check_attr_set('db_credentials')
    def run_statement(self, sql_statement, new_con=True, save_con=False,
                    fetch_data=False, commit=False, params=None, multiple_values=None):
        """method to run an sql statement"""

        print '\nsql_statement is :\n%s\n'%(sql_statement,)

        con = self._get_connection(new_con)

        #make sure to close con
        try:
            with closing(con.cursor()) as cursor:
                if multiple_values:
                    cursor.executemany(sql_statement, multiple_values)
                elif params:
                    cursor.execute(sql_statement, params)
                else:
                    cursor.execute(sql_statement)

                print '\nNumber of rows affected: %s\n'%(cursor.rowcount, )

                if fetch_data:
                    print '\nFetching data\n'
                    values = cursor.fetchall()
                    try:
                        columns = [desc[0] for desc in cursor.description]
                    except TypeError:
                        columns = None

                    output = values, columns

                else:
                    output = None

            if commit:
                print '\nCommitting sql transaction\n'
                con.commit()

        finally:
            if save_con:
                self._save_connection(con)
            else:
                con.close()

        return output
