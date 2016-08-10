"""parses configuration and returns useful things"""
#pylint: disable=relative-import

import os
import json

from method_wrappers.check_config_attr import check_config_attr_default_none

class BaseConfig(object):
    """parses configuration files"""

    IDENTIFIER_ATTR = 'identifier'
    ETL_CLASS_ATTR = 'etl_class'
    CONFIG_CLASS_ATTR = 'config_class'

    def __init__(self, config_dir=None, config_filename=None, config_dict=None):
        """intialize Parser"""

        self.config_dir = None
        self.config_filename = None
        #self.config will be set later

        # If you create config with config_dict, filename and directory attributes
        # will be left as None
        if config_dict:
            self.config = config_dict

        elif config_dir:

            #set config only if config_filename and dir given.
            if config_filename:
                self.set_config(config_dir=config_dir, config_filename=config_filename)

            # NOTE this logic should just be removed
            #set config dir only if config dir given
            else:
                self.set_config_dir(config_dir=config_dir)

    @classmethod
    def create_from_filepath(cls, filepath, *args, **kwargs):
        """creates instance of Config from filepath"""

        config_dir, config_filename = os.path.split(filepath)
        return cls(config_dir=config_dir,
            config_filename=config_filename,
            *args,
            **kwargs
        )

    @check_config_attr_default_none
    def get_identifier(self):
        """gets identifier for current configuration"""

        return self.config[self.IDENTIFIER_ATTR]

    def get_etl_class(self):
        """gets etl class name for current configuration"""

        return self.config[self.ETL_CLASS_ATTR]

    def get_config_class(self):
        """gets config class name for current configuration"""

        return self.config[self.CONFIG_CLASS_ATTR]

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
                return BaseConfig._get_config_from_string(config_file.read())
        except IOError:
            raise Exception('Configuration filepath %s doesnt exist'%(filepath, ))

    def morph(self, configs):
        """
        returns a config of different class
        configs should be a module with config classes
        """

        config_class_name = self.get_config_class()
        ConfigClass = getattr(configs, config_class_name)

        return ConfigClass(config_dict=self.config)

    def create(self, etl_classes):
        """
        returns an EtlClass object with this config
        etl_classes should be a module with Etl Classes
        """

        etl_class_name = self.get_etl_class()
        EtlClass = getattr(etl_classes, etl_class_name)

        return EtlClass(config=self)

    @classmethod
    def show_example_config(cls):
        """prints out an example config"""

        example = {getattr(cls, attribute): "" for attribute in dir(cls)
            if attribute.endswith("_ATTR") or attribute.endswith("_ATTRIBUTE")
        }

        print json.dumps(example, indent=4, sort_keys=True)
