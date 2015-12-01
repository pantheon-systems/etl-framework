"""parses configuration and returns useful things"""
#pylint: disable=relative-import

import os
import json

from method_wrappers.check_attr_set import _check_attr_set

class BaseConfigParser(object):
    """parses configuration files"""

    COLUMN_MAPPINGS_ATTR = 'field_mappings'
    IDENTIFIER_ATTR = 'identifier'
    FILTER_FUNC_ATTR = 'filter_function'
    PRE_FILTER_FUNC_ATTR = 'pre_filter_function'
    REFERENCE_IDS_ATTR = 'reference_ids'

    def __init__(self, config_dir=None, config_filename=None):
        """intialize Parser"""

        self.config_dir = None
        self.config_filename = None
        self.config = None

        if config_dir:

            #set config only if config_filename and dir given.
            if config_filename:
                self.set_config(config_dir=config_dir, config_filename=config_filename)

            #set config dir only if config dir given
            else:
                self.set_config_dir(config_dir=config_dir)

    @staticmethod
    def _replace_reference_ids(string, reference_ids):
        """replaces table ids with their names"""
        for table_name, table_id in reference_ids.iteritems():
            string = string.replace('{' + str(table_id) + '}', table_name)

        return string

    @_check_attr_set('config')
    def get_identifier(self):
        """gets identifier for current configuration"""

        return self.config[self.IDENTIFIER_ATTR]

    @_check_attr_set('config')
    def get_field_mappings(self):
        """returns field_mappings attribute"""

        return self.config[self.COLUMN_MAPPINGS_ATTR]

    @_check_attr_set('config')
    def get_filter_function_name(self):
        """returns filter_function name attribute"""

        return self.config[self.FILTER_FUNC_ATTR]

    @_check_attr_set('config')
    def get_pre_filter_function_name(self):
        """returns pre_filter_function name attribute"""

        return self.config[self.PRE_FILTER_FUNC_ATTR]

    def config_is_set(self):
        """checks if config attribute is set"""

        return bool(self.config)

    def set_config_dir(self, config_dir):
        """ sets config_dir attribute"""

        self.config_dir = config_dir

    @classmethod
    def _get_config_from_string(cls, config_string):
        """helper method for set_config"""

        #use raw config to replace reference_ids with table_names in config_string
        raw_config = json.loads(config_string)
        config_string = cls._replace_reference_ids(config_string, raw_config[cls.REFERENCE_IDS_ATTR])

        return json.loads(config_string)

    def _set_config(self, config):
        """helper function to set config attribute"""

        self.config = config

    def set_config(self, config_dir=None, config_filename=None):
        """sets configuration and config_filename"""

        #if config_dir is not set, use already existing one
        config_dir = config_dir or self.config_dir

        #do same for config_filename
        config_filename = config_filename or self.config_filename

        #set all ConfiguationParser attributes
        self._set_config(self.get_config(config_dir, config_filename))
        self.config_dir = config_dir
        self.config_filename = config_filename

    @classmethod
    def get_config(cls, config_dir, config_filename):
        """helper function to return config"""

        #check that both config_dir and config_filename are non-null
        if not config_filename:
            raise Exception('Config filename not specified')
        elif not config_dir:
            raise Exception('Config dir not specified')
        else:
            filepath = os.path.join(config_dir, config_filename)

        #check that file exists
        if not os.path.exists(filepath):
            raise Exception('Configuration filepath %s doesnt exist'%(filepath, ))

        else:
            with open(filepath, 'r') as config_file:
                config_string = config_file.read()

            #return config
            return cls._get_config_from_string(config_string)
