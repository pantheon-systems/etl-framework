"""base class for builder"""

import os

from etl_framework.BaseConfig import BaseConfig
from etl_framework.Exceptions import DuplicateConfigException, ConfigNotFoundException

class Builder(object):

    def __init__(
        self,
        configs=None,
        etl_module=None,
        environment=None,
    ):

        if not configs:
            self.configs = {}

        self.etl_module = etl_module
        self.environment = environment

        self.etl_classes = None

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

    def build_etl_classes(self):
        """
        Sets etl_classes attribute by processing configs
        """

        self.clear_etl_classes()

        for config in self.configs.itervalues():

            etl_class = self.build(config)

            self.add_etl_class(etl_class)

    def build(self, config):
        """builds a config"""

        return config.create(
            etl_classes=self.etl_module,
            builder=self,
        )

    def build_from_filepath(self, filepath):

        config = BaseConfig.create_from_filepath(filepath)

        return self.build(config)

    def get_etl_class(self, identifier):

        return self.etl_classes[identifier]

    def get_config(self, identifier):

        try:
            return self.configs[identifier]
        except KeyError:
            raise ConfigNotFoundException()

    def add_configs_from_directories(self, directories):
        """
        Adds all configs found in a directories
        @directories is a list of directory paths
        """

        for directory in directories:
            try:
                filenames = next(os.walk(directory))[2]
            except StopIteration:
                print "WARNING {} directory has no files".format(directory)
                continue
            for filename in filenames:

                filepath = os.path.join(directory, filename)
                self.add_config_from_filepath(filepath)

    def add_config_from_filepath(self, config_filepath):
        """Yeah"""

        config = BaseConfig.create_from_filepath(config_filepath)

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
