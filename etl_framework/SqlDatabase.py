"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated

import abc
from contextlib import closing

class SqlDatabase(object):
    """loads data into database"""

    __metaclass__ = abc.ABCMeta

    def __init__(self, db_credentials=None, *args, **kwargs):
        """initializes base data loader"""

        super(SqlDatabase, self).__init__(*args, **kwargs)

        self.con = None
        self.db_credentials = None

        if db_credentials:
            self.set_db_credentials(db_credentials)

    @classmethod
    def create_from_dsn(cls, dsn):
        """creates instance of data_loader from dsn (i.e. a sql url)"""

        return cls(cls.parse_dsn(dsn))

    def set_db_credentials_from_dsn(self, dsn):
        """sets credentials from dsn"""

        self.set_db_credentials(self.parse_dsn(dsn))

    @staticmethod
    def parse_dsn(dsn):
        """
        parse dsn and outputs credential parameters
        """

        url_segments = dsn.split(':')
        user = url_segments[1].replace('/', '')
        password, host = url_segments[2].split('@')
        port, database = url_segments[3].split('/')

        port = int(port)

        return (host, port, user, password, database)

    def set_db_credentials(self, db_credentials):
        """sets the database credentials"""

        if len(db_credentials) != 5:
            raise Exception('Db credentials must be array of 5 items. %d given'%len(db_credentials))

        self.db_credentials = db_credentials

    def _get_connection(self, new_con=True, verbose=True):
        """returns connection object to data warehouse"""

        #clear saved connnection if you want a new connection
        if new_con:
            self._clear_connection()

            if verbose:
                print 'Creating new connection'

            return self._create_connection()

        #check if self.con is set
        elif not self.con:
            raise Exception('Cant return old connection object. self.con isnt set!')

        #return old connection
        else:
            if verbose:
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

    def _save_connection(self, con, verbose=True):
        """
        saves connection to self.con
        """

        if con is self.con:
            if verbose:
                print '\nResaving sql connection\n'

        else:
            #first clear old connection
            if verbose:
                print '\nSaving sql connection\n'

            self._clear_connection()

            self.con = con

    def run_statement(self, sql_statement=None, new_con=True, save_con=False,
                    fetch_data=False, commit=False, params=None, multiple_values=None,
                    verbose=True):
        """method to run an sql statement"""

        if verbose:
            print '\nsql_statement is :\n%s\n'%(sql_statement,)

        con = self._get_connection(new_con, verbose=verbose)

        #make sure to close con
        try:
            with closing(con.cursor()) as cursor:
                if multiple_values:
                    cursor.executemany(sql_statement, multiple_values)
                elif params:
                    cursor.execute(sql_statement, params)
                else:
                    cursor.execute(sql_statement)

                if verbose:
                    print '\nNumber of rows affected: %s\n'%(cursor.rowcount, )

                if fetch_data:
                    if verbose:
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
                self._save_connection(con, verbose=verbose)
            else:
                con.close()

        return output
