"""parses configuration and returns useful things"""
#pylint: disable=relative-import

import os
import json

from method_wrappers.check_config_attr import check_config_attr

class ConfigurationParser(object):
    """parses configuration files"""

    IDENTIFIER_ATTR = 'identifier'

    def __init__(self, config_dir=None, config_filename=None):
        """intialize Parser"""

        self.config_dir = None
        self.config_filename = None
        #self.config will be set later

        if config_dir:

            #set config only if config_filename and dir given.
            if config_filename:
                self.set_config(config_dir=config_dir, config_filename=config_filename)

            #set config dir only if config dir given
            else:
                self.set_config_dir(config_dir=config_dir)

    @check_config_attr
    def get_identifier(self):
        """gets identifier for current configuration"""

        return self.config[self.IDENTIFIER_ATTR]

    def set_config_dir(self, config_dir):
        """ sets config_dir attribute"""

        self.config_dir = config_dir

    @staticmethod
    def _get_config_from_string(config_string):
        """helper method for set_config"""

        return json.loads(config_string)

    def set_config(self, config_dir=None, config_filename=None):
        """sets configuration and config_filename"""

        #if config_dir is not set, use already existing one
        config_dir = config_dir or self.config_dir

        #do same for config_filename
        config_filename = config_filename or self.config_filename

        #set all ConfiguationParser attributes
        if config_filename and config_dir:
            self.config = self.get_config(config_dir, config_filename)
            self.config_dir = config_dir
            self.config_filename = config_filename
        else:
            raise Exception('Must specify valid config dir: {0}\nand\nvalid config_filename: {1}'
                            .format(config_dir, config_filename))

    @staticmethod
    def get_config(config_dir, config_filename):
        """helper function to return config"""

        #check that both config_dir and config_filename are non-null
        filepath = os.path.join(config_dir, config_filename)

        try:
            with open(filepath, 'r') as config_file:
                ConfigParser._get_config_from_string(config_file.read())
        except IOError:
            raise Exception('Configuration filepath %s doesnt exist'%(filepath, ))
