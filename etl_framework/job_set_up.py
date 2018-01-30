"""Base class for Etl SetUp"""
#pylint: disable=unsubscriptable-object
#pylint: disable=relative-import

import datetime

from method_wrappers.check_attr_set import _check_attr_set
from utilities.DatetimeConverter import DatetimeConverter

from etl_framework.datastores.mysql_database import MySqlDatabase

class JobSetUp(object):
    """class to setup etl process, get etl settings/credentials, and teardown"""

    CONFIG_DIR = ''

    DERIVED_CONFIG_DIR = ''
    EXTRACTOR_CONFIG_DIR = ''
    TRANSFORMER_CONFIG_DIR = ''
    LOADER_CONFIG_DIR = ''
    SCHEMA_CONFIG_DIR = ''

    ETL_JOB_EARLIEST_TIME = datetime.datetime(1000, 01, 01, 0, 0, 0, 0)
    ETL_JOBS_TABLE = '_etl_jobs_'
    ETL_JOBS_STARTED_AT_FIELD = 'most_recent_started_at'
    ETL_JOBS_CUTOFF_AT_FIELD = 'cutoff_at'
    ETL_JOBS_NAME_FIELD = 'job_name'

    ETL_JOB_CREATE_TABLE_IF_NOT_EXISTS_STATEMENT =\
    """
    CREATE TABLE IF NOT EXISTS `{0}` (
      `id` int(8) NOT NULL AUTO_INCREMENT,
      `{1}` varchar(127) DEFAULT NULL,
      `{2}` datetime DEFAULT NULL,
      `{3}` datetime DEFAULT NULL,
      estimated_completion_at datetime,
      record_updated timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (`id`),
      KEY `job_name` (`{1}`)
    ) ENGINE=InnoDB AUTO_INCREMENT=55 DEFAULT CHARSET=utf8
    """.format(
        ETL_JOBS_TABLE,
        ETL_JOBS_NAME_FIELD,
        ETL_JOBS_CUTOFF_AT_FIELD,
        ETL_JOBS_STARTED_AT_FIELD
    )

    ETL_JOB_WHERE_PHRASE = '\n'.join([
                                    'WHERE',
                                    '\t{0} = \'%s\''.format(ETL_JOBS_NAME_FIELD),
                                    ';'
                                      ])

    ETL_JOB_UPDATE_SET_PHRASE = '\n'.join([
                                    'UPDATE `%s`'%(ETL_JOBS_TABLE, ),
                                    'SET'
                                      ])

    ETL_JOB_SET_START_STATEMENT = '\n'.join([
                                    ETL_JOB_UPDATE_SET_PHRASE,
                                    '\t`%s` = NOW()'%(ETL_JOBS_STARTED_AT_FIELD, ),
                                    ETL_JOB_WHERE_PHRASE,
                                      ])

    ETL_JOB_SELECT_CUTOFF_STATEMENT = '\n'.join([
                                    'SELECT',
                                    ETL_JOBS_CUTOFF_AT_FIELD,
                                    'FROM %s'%(ETL_JOBS_TABLE, ),
                                    ETL_JOB_WHERE_PHRASE,
                                    ])

    ETL_JOB_INSERT_JOB_STATEMENT = '\n'.join([
                                    'INSERT IGNORE INTO {0} (`job_name`)'.format(ETL_JOBS_TABLE),
                                    'VALUES (\'{0}\')'
                                    ])

    ETL_JOB_SET_CUTOFF_STATEMENT = '\n'.join([
                                    ETL_JOB_UPDATE_SET_PHRASE,
                                    '\t`{0}` = {1}'.format(ETL_JOBS_CUTOFF_AT_FIELD, ETL_JOBS_STARTED_AT_FIELD),
                                    ETL_JOB_WHERE_PHRASE,
                                    ])

    ETL_JOB_CLEAR_CUTOFF_STATEMENT = '\n'.join([
                                    ETL_JOB_UPDATE_SET_PHRASE,
                                    '\t`{0}` = \'{1}\''.format(ETL_JOBS_CUTOFF_AT_FIELD, ETL_JOB_EARLIEST_TIME),
                                    ETL_JOB_WHERE_PHRASE,
                                    ])

    ETL_JOB_CREATE_JOB_STATEMENT = '\n'.join([
                                    'INSERT INTO {0} ({1})'.format(
                                        ETL_JOBS_TABLE, ETL_JOBS_NAME_FIELD),
                                    'VALUES (%d, "%s")'
                                    ])

    ETL_JOB_CUTOFF_OFFSET = datetime.timedelta(days=5)

    DERIVED_TABLE_CUTOFF_OFFSET = datetime.timedelta(days=1)

    def __init__(self, job_name, bi_dsn, *args, **kwargs):
        """"initialize object"""

        super(JobSetUp, self).__init__(*args, **kwargs)

        self.job_name = job_name

        self.sql_database = MySqlDatabase()
        self.bi_dsn = bi_dsn
        self.etl_job_cutoff_at = None

    @classmethod
    def get_config_dir(cls):
        """returns path to configuration dir"""

        return cls.CONFIG_DIR

    @classmethod
    def get_extractor_config_dir(cls):
        """returns path to configuration dir"""

        return cls.EXTRACTOR_CONFIG_DIR

    @classmethod
    def get_transformer_config_dir(cls):
        """returns path to configuration dir"""

        return cls.TRANSFORMER_CONFIG_DIR

    @classmethod
    def get_loader_config_dir(cls):
        """returns path to configuration dir"""

        return cls.LOADER_CONFIG_DIR

    @classmethod
    def get_schema_config_dir(cls):
        """returns path to configuration dir"""

        return cls.SCHEMA_CONFIG_DIR

    @classmethod
    def get_derived_config_dir(cls):
        """returns path to configuration dir"""

        return cls.DERIVED_CONFIG_DIR

    def set_up(self, datetime_cutoff=None):
        """do set up for etl"""

        # This also sets the db_credentials
        self.create_database_if_not_exists()

        self.create_etl_job_table_if_not_exists()
        self.create_etl_job_row_if_not_exists()

        #set start time of etl job and get cutoff value from SQL db
        self.run_etl_job_start_statement()
        self.set_etl_job_cutoff_value(datetime_cutoff=datetime_cutoff)

    def tear_down(self):
        """do teardown for etl"""

        self.run_etl_job_cutoff_statement()

    def create_database_if_not_exists(self):
        """creates database if it doesn't exist"""

        #This assumes dsn has a database
        dsn = self.bi_dsn
        db_start = dsn.rfind('/')
        dsn_without_db = dsn[: db_start + 1]
        db = dsn[db_start + 1:]

        # Set credentials without the database in order to create db
        self.sql_database.set_credentials_from_dsn(dsn_without_db)

        self.sql_database.run_statement(
            "CREATE DATABASE IF NOT EXISTS {}".format(db)
        )

        # Reset credentials to dsn
        self.sql_database.set_credentials_from_dsn(dsn)

    def create_etl_job_table_if_not_exists(self):
        """ Creates _etl_job_ table if it doesnt already exist"""

        sql_statement = self.ETL_JOB_CREATE_TABLE_IF_NOT_EXISTS_STATEMENT
        self.sql_database.run_statement(sql_statement)

    def create_etl_job_row_if_not_exists(self):
        """ Inserts the requisite row into the ETL job table if it does not yet exist """

        sql_statement = self.ETL_JOB_INSERT_JOB_STATEMENT.format(
            self.job_name
        )
        self.sql_database.run_statement(sql_statement)

    def set_etl_job_cutoff_value(self, datetime_cutoff=None):
        """sets etl cutoff datetime value"""

        if datetime_cutoff:
            pass

        else:
            sql_statement = self.ETL_JOB_SELECT_CUTOFF_STATEMENT%(self.job_name, )

            datetime_cutoff = self.sql_database.run_statement(sql_statement, fetch_data=True)[0][0][0]

            if datetime_cutoff is None:
                datetime_cutoff = self.ETL_JOB_EARLIEST_TIME

        self.etl_job_cutoff_at = datetime_cutoff

    @_check_attr_set('etl_job_cutoff_at')
    def get_etl_job_cutoff_value(self):
        """gets etl cutoff value - offset"""

        return self.etl_job_cutoff_at - self.ETL_JOB_CUTOFF_OFFSET

    def get_etl_cutoff_timestamp(self):
        """returns etl_cutoff in unix timestamp format"""

        return DatetimeConverter.get_timestamp(self.get_etl_job_cutoff_value())

    @_check_attr_set('etl_job_cutoff_at')
    def get_derived_table_cutoff_value(self):
        """gets etl cutoff value - derived table offset"""

        return self.etl_job_cutoff_at - self.DERIVED_TABLE_CUTOFF_OFFSET

    def run_etl_job_start_statement(self):
        """sets the start datetime for etl job in SQL db"""

        sql_statement = self.ETL_JOB_SET_START_STATEMENT%(self.job_name, )
        self.sql_database.run_statement(sql_statement, commit=True)

    def run_etl_job_cutoff_statement(self):
        """sets the new cutoff  datetime for etl job in SQL db"""

        sql_statement = self.ETL_JOB_SET_CUTOFF_STATEMENT%(self.job_name)
        self.sql_database.run_statement(sql_statement, commit=True)

    def run_etl_job_clear_cutoff_date(self):
        """sets the cutoff datetime to earliest value in SQL db (to reset db during tests)"""

        sql_statement = self.ETL_JOB_CLEAR_CUTOFF_STATEMENT%(self.job_name)
        self.sql_database.run_statement(sql_statement, commit=True)
