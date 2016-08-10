"""Base class for Etl SetUp"""
#pylint: disable=relative-import

import os
import abc
import datetime

from Exceptions import EnvironmentVariableNotSetException
from method_wrappers.check_attr_set import _check_attr_set
from utilities.DatetimeConverter import DatetimeConverter

class BaseEtlSetUp(object):
    """class to setup etl process, get etl settings/credentials, and teardown"""

    __metaclass__ = abc.ABCMeta

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
    ETL_JOBS_ID_FIELD = 'job_id'
    ETL_JOBS_NAME_FIELD = 'job_name'

    #THESE VALUES NEED TO BE SET!!
    #ETL_JOB_ID = None
    #ETL_JOB_NAME = None

    ETL_JOB_WHERE_PHRASE = '\n'.join([
                                    'WHERE',
                                    '\t{0} = %d'.format(ETL_JOBS_ID_FIELD),
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

    ETL_JOB_CUTOFF_OFFSET = datetime.timedelta(days=5)

    DERIVED_TABLE_CUTOFF_OFFSET = datetime.timedelta(days=1)

    def __new__(cls, *args, **kargs):
        """check that ETL_JOB_ID and ETL_JOB_NAME are set"""

        if hasattr(cls, 'ETL_JOB_ID') and hasattr(cls, 'ETL_JOB_NAME'):
            pass
        else:
            raise NotImplementedError('EtlSetUp should have ETL_JOB_ID and ETL_JOB_NAME attributes set!')

        return object.__new__(cls, *args, **kargs)

    def __init__(self, sql_database, *args, **kwargs):
        """"initialize object"""

        super(BaseEtlSetUp, self).__init__(*args, **kwargs)

        self.sql_database = sql_database
        self.bi_dsn = None
        self.etl_job_cutoff_at = None

    def _set_from_env_variable(self, attribute_name, env_variable, display=False):
        """sets attribute from environment variable"""

        try:
            setattr(self, attribute_name, os.environ[env_variable])
        except KeyError:
            raise EnvironmentVariableNotSetException('%s must be set as ENV'%(env_variable, ))
        else:
            if display:
                print 'self.%s is : %s'%(attribute_name, getattr(self, attribute_name))

    def _get_attr(self, attribute_name):
        """gets instance attribute given attribute name"""

        attribute = getattr(self, attribute_name)

        if attribute is None:
            raise Exception('%s attribute isnt set yet!'%(attribute, ))
        else:
            return attribute

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

    def get_bi_dsn(self):
        """returns dns to connect to data warehouse"""

        return self._get_attr('bi_dsn')

    @abc.abstractmethod
    def _set_bi_dsn(self):
        """
        set the sql url to bi database
        during Etl setUp
        """

    def set_up(self, datetime_cutoff=None):
        """do set up for etl"""

        self._set_bi_dsn()
        self.sql_database.set_db_credentials_from_dsn(self.get_bi_dsn())

        #set start time of etl job and get cutoff value from SQL db
        self.run_etl_job_start_statement()
        self.set_etl_job_cutoff_value(datetime_cutoff=datetime_cutoff)

    def tear_down(self):
        """do teardown for etl"""

        self.run_etl_job_cutoff_statement()

    def set_etl_job_cutoff_value(self, datetime_cutoff=None):
        """sets etl cutoff datetime value"""

        if datetime_cutoff:
            pass

        else:
                sql_statement = self.ETL_JOB_SELECT_CUTOFF_STATEMENT%(self.ETL_JOB_ID, )

            try:
                datetime_cutoff = self.sql_database.run_statement(sql_statement, fetch_data=True)[0][0][0]
            except IndexError as e:
                datetime_cutoff = None

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

        sql_statement = self.ETL_JOB_SET_START_STATEMENT%(self.ETL_JOB_ID, )
        self.sql_database.run_statement(sql_statement, commit=True)

    def run_etl_job_cutoff_statement(self):
        """sets the new cutoff  datetime for etl job in SQL db"""

        sql_statement = self.ETL_JOB_SET_CUTOFF_STATEMENT%(self.ETL_JOB_ID)
        self.sql_database.run_statement(sql_statement, commit=True)

    def run_etl_job_clear_cutoff_date(self):
        """sets the cutoff datetime to earliest value in SQL db (to reset db during tests)"""

        sql_statement = self.ETL_JOB_CLEAR_CUTOFF_STATEMENT%(self.ETL_JOB_ID)
        self.sql_database.run_statement(sql_statement, commit=True)
