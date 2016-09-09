"""Base class to load data into data warehouse"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated
#pylint: disable=attribute-defined-outside-init

import abc
from contextlib import closing

from etl_framework.datastore_interfaces.datastore_interface import DatastoreInterface

class SqlDatabaseInterface(DatastoreInterface):
    """loads data into database"""

    __metaclass__ = abc.ABCMeta

    #CONNECTION_CLOSED_EXCEPTION = 'THIS NEEDS TO BE SET'
    #CONNECTION_GONE_EXCEPTION = 'THIS NEEDS TO BE SET'

    def __new__(cls, *args, **kargs):
        """check that ETL_JOB_ID and ETL_JOB_NAME are set"""

        if hasattr(cls, 'CONNECTION_CLOSED_EXCEPTION') and hasattr(cls, 'CONNECTION_GONE_EXCEPTION'):
            pass
        else:
            raise NotImplementedError('EtlSetUp should have CONNECTION_CLOSED_EXCEPTION and \
                                        CONNECTION_GONE_EXCEPTION attributes set!')

        return object.__new__(cls, *args, **kargs)

    @classmethod
    def create_from_dsn(cls, dsn):
        """creates instance of data_loader from dsn (i.e. a sql url)"""

        return cls(credentials=cls.parse_dsn(dsn))

    def set_credentials_from_dsn(self, dsn):
        """sets credentials from dsn"""

        self.set_credentials(self.parse_dsn(dsn))

    @staticmethod
    def parse_dsn(dsn):
        """
        parse dsn and outputs credential parameters
        """

        # Unix socket connection
        if '(' in dsn:
            segment_start, segment_end = dsn.split('@')
            user, password = segment_start.replace('//', '').split(':')[1:3]
            unix_socket = segment_end[
                segment_end.find("(") + 1: segment_end.find(")")
            ]

            database = segment_end.split('/')[-1]

            return (user, password, unix_socket, database)

        # Tcp connection
        else:

            url_segments = dsn.split(':')
            user = url_segments[1].replace('/', '')
            password, host = url_segments[2].split('@')
            port, database = url_segments[3].split('/')

            port = int(port)

            return (host, port, user, password, database)

    def set_credentials(self, credentials):
        """sets the database credentials"""

        if len(credentials) not in [4, 5]:
            raise Exception('Db credentials must be array of 4(for unix socket) or ' +\
                '5(for tcp connection) items. %d given'%len(credentials)
            )

        self.credentials = credentials

    def get_connection(self):
        """Satisfies Datastore Interface"""

        return self._get_connection()

    def _get_connection(self, new_con=True, verbose=True):
        """returns connection object to data warehouse"""

        #clear saved connnection if you want a new connection
        if new_con:
            self.clear_connection()

            if verbose:
                print 'Creating new connection'

            return self._create_connection()

        #check if self.con is set
        elif not self.con:
            print 'Cant return old connection because self.con isnt set. Creating new connection'
            return self._create_connection()

        #return old connection
        else:
            if verbose:
                print 'Reusing old connection'

            return self.con

    def commit_connection(self):
        """
        commits the saved connection
        """

        if self.con is not None:
            print '\nCommitting sql transaction\n'
            self.con.commit()

    def clear_connection(self):
        """
        sets self.con value to None and closes existing connection
        """

        #if no connection saved, do nothing
        if not self.con:
            return

        try:
            self.con.close()
        except self.CONNECTION_CLOSED_EXCEPTION:
            print 'Saved connection was already closed'

        self.con = None

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

            self.clear_connection()

            self.con = con

    @staticmethod
    def _execute_statement(cursor, sql_statement, multiple_values=None, params=None):
        """helper method to execute statement"""

        if multiple_values:
            cursor.executemany(sql_statement, multiple_values)
        elif params:
            cursor.execute(sql_statement, params)
        else:
            cursor.execute(sql_statement)

    def run_statement(self, sql_statement=None, new_con=False, save_con=True,
                    fetch_data=False, commit=False, params=None, multiple_values=None,
                    verbose=True):
        """method to run an sql statement"""

        if verbose:
            print '\nsql_statement is :\n%s\n'%(sql_statement,)

        con = self._get_connection(new_con, verbose=verbose)

        #make sure to close con
        try:
            with closing(con.cursor()) as cursor:
                try:
                    self._execute_statement(cursor=cursor,
                                        sql_statement=sql_statement,
                                        multiple_values=multiple_values,
                                        params=params)

                except self.CONNECTION_GONE_EXCEPTION:
                    print 'WARNING: Attempting reconnect to MySQL db. Uncommitted transactions will rollback!'
                    con.ping(True)
                    self._execute_statement(cursor=cursor,
                                            sql_statement=sql_statement,
                                            multiple_values=multiple_values,
                                            params=params)

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
