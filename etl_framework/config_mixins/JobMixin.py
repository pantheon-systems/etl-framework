"""parses configuration and returns useful things"""
#pylint: disable=relative-import
import os

from etl_framework.method_wrappers.check_config_attr import check_config_attr

class JobMixin(object):
    """parses configuration files"""

    JOB_ID_ATTR = 'job_id'
    JOB_NAME_ATTR = 'job_name'
    CONFIGURATION_GROUPS_ATTR = 'configuration_groups'
    TRANSFORMER_CONFIG_ATTR = 'transformers'
    SCHEMA_CONFIG_ATTR = 'schemas'
    EXTRACTOR_CONFIG_ATTR = 'extractors'
    LOADER_CONFIG_ATTR = 'loaders'
    ENVIRONMENT_CONFIG_ATTR = 'environment'
    CONFIG_DIR_ATTR = 'config_dir'
    CONFIG_FILENAMES_ATTR = 'config_filenames'
    CONFIG_FILENAME_ATTR = 'config_filename'
    TESTS_ATTR = 'tests'

    def get_test(self, name):
        """gets test attribute by filepath"""

        return self.config[self.TESTS_ATTR][name]

    @check_config_attr
    def get_job_id(self):
        """gets configurations """

        return self.config[self.JOB_ID_ATTR]

    @check_config_attr
    def get_job_name(self):
        """gets configurations """

        return self.config[self.JOB_NAME_ATTR]

    @check_config_attr
    def get_configuration_groups(self):
        """gets configurations """

        return self.config[self.CONFIGURATION_GROUPS_ATTR]

    @classmethod
    @check_config_attr
    def get_configuration_directory(cls, configurations):
        """gets config directory of configurations"""

        return configurations[cls.CONFIG_DIR_ATTR]

    @classmethod
    @check_config_attr
    def get_configuration_filename(cls, configuration):
        """gets config filename of singular configuration"""

        return configuration[cls.CONFIG_FILENAME_ATTR]

    @classmethod
    @check_config_attr
    def get_configuration_filepath(cls, configuration):
        """ gets the filepath of a singular configuration """

        config_dir = cls.get_configuration_directory(configuration)
        filename = cls.get_configuration_filename(configuration)

        return os.path.join(config_dir, filename)

    @classmethod
    @check_config_attr
    def iter_configuration_filenames(cls, configurations):
        """yields filenames of configurations"""

        for filename in configurations[cls.CONFIG_FILENAMES_ATTR]:
            yield filename

    @classmethod
    @check_config_attr
    def iter_configuration_filepaths(cls, configurations):
        """yields filenames of configurations"""

        config_dir = cls.get_configuration_directory(configurations)

        for filename in cls.iter_configuration_filenames(configurations):
            yield os.path.join(config_dir, filename)

    @check_config_attr
    def get_transformer_configurations(self):
        """gets transformer configurations"""

        return self.get_configuration_groups()[self.TRANSFORMER_CONFIG_ATTR]

    @check_config_attr
    def get_extractor_configurations(self):
        """gets extractor configurations"""

        return self.get_configuration_groups()[self.EXTRACTOR_CONFIG_ATTR]

    @check_config_attr
    def get_loader_configurations(self):
        """gets loader configurations"""

        return self.get_configuration_groups()[self.LOADER_CONFIG_ATTR]

    @check_config_attr
    def get_schema_configurations(self):
        """gets schema configurations"""

        return self.get_configuration_groups()[self.SCHEMA_CONFIG_ATTR]

    @check_config_attr
    def get_environment_configuration(self):
        """gets environment configuration"""

        return self.config[self.ENVIRONMENT_CONFIG_ATTR]

    @check_config_attr
    def get_environment_configuration_filepath(self):
        """ returns environment configuration filepath """

        return self.get_configuration_filepath(self.get_environment_configuration())

    @check_config_attr
    def iter_transformer_configuration_filepaths(self):
        """iterates over transformer filepaths"""

        for path in self.iter_configuration_filepaths(self.get_transformer_configurations()):
            yield path

    @check_config_attr
    def iter_schema_configuration_filepaths(self):
        """iterates over schema filepaths"""

        for path in self.iter_configuration_filepaths(self.get_schema_configurations()):
            yield path

    @check_config_attr
    def iter_extractor_configuration_filepaths(self):
        """iterates over extractor filepaths"""

        for path in self.iter_configuration_filepaths(self.get_extractor_configurations()):
            yield path

    @check_config_attr
    def iter_loader_configuration_filepaths(self):
        """iterates over loader filepaths"""

        for path in self.iter_configuration_filepaths(self.get_loader_configurations()):
            yield path
