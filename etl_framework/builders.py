"""base class for builder"""

import os

from etl_framework.BaseConfig import BaseConfig
from etl_framework.Exceptions import DuplicateConfigException

class Builder(object):

    def __init__(
        self,
        configs=None,
        etl_module=None
    ):

        if not configs:
            self.configs = {}

        self.etl_module = etl_module

        self.etl_classes = None
        self.environment = None

        self.clear_etl_classes()

    def clear_etl_classes(self):

        self.etl_classes = {}

    def add_etl_class(self, etl_class):
        """config is an instance of BaseConfig"""

        if etl_class.identifier in self.etl_classes:
            raise DuplicateConfigException(
                "Builder already has etl_class with identifier : {}".format(
                    etl_class.identifier
                )
            )

        self.etl_classes[etl_class.identifier] = etl_class

    def set_etl_classes(self):
        """
        Sets etl_classes attribute by processing configs
        """

        self.clear_etl_classes()

        for config in self.configs.itervalues():

            etl_class = config.create(
                etl_classes=self.etl_module,
            )

            self.add_etl_class(etl_class)

    def get_etl_class(self, identifier):

        return self.etl_classes[identifier]

    def get_config(self, identifier):

        return self.configs[identifier]

    def add_configs_from_directories(self, directories):
        """
        Adds all configs found in a directories
        @directories is a list of directory paths
        """

        for directory in directories:
            for filename in next(os.walk(directory))[2]:

                filepath = os.path.join(directory, filename)
                self.add_config_from_filepath(filepath)

    def add_config_from_filepath(self, config_filepath):
        """Yeah"""

        config = BaseConfig.create_from_filepath(config_filepath)
        config = config.morph(
            configs=self.etl_module
        )

        self.add_config(config)

    def add_config(self, config):
        """config is an instance of BaseConfig"""

        if config.identifier in self.configs:
            raise DuplicateConfigException(
                "Builder already has config with identifier : {}".format(
                    config.identifier
                )
            )

        self.configs[config.identifier] = config
