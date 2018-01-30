"""parses configuration and returns useful things"""
#pylint: disable=relative-import
#pylint: disable=super-on-old-class

import os
import json

from etl_framework.Exceptions import EnvironmentSettingNotFoundException
from etl_framework.BaseConfig import BaseConfig

class EnvironmentConfig(BaseConfig):
    """parses configuration files"""

    ENVIRONMENT_SETTINGS_ATTRIBUTE = 'environment_settings'

    def __init__(self, *args, **kwargs):

        super(EnvironmentConfig, self).__init__(*args, **kwargs)

        self.environment = None
        # NOTE config_dict should be a mandatory argument to instantiate config
        # In meantime, only set environment if self.config is set
        if self.config:
            self.set_environment()

    def __getattr__(self, key):
        """Get attribute on config if not in EtlClass object"""

        # Get attribute if Config doesnt exist
        # we don't need a special call to super here because getattr is only
        # called when an attribute is NOT found in the instance's dictionary
        environment = self.__dict__["environment"]

        return environment[key]

    def get_environment_settings(self):
        """ stuff """

        return self.config[self.ENVIRONMENT_SETTINGS_ATTRIBUTE]

    def set_environment(self):

        # Clear old environment
        self.environment = {}

        for setting in self.get_environment_settings():

            # NOTE all environment settings must have type and to_attribute values
            # TODO json schema??
            env_type = setting["type"]
            attribute_name = setting["to_attribute"]

            try:
                method = getattr(self, 'create_' + env_type + '_attribute')
            except KeyError:
                raise KeyError(
                    "No method for setting {} environment attribute"\
                    .format(env_type)
                )

            self.environment[attribute_name] = method(setting)

    @staticmethod
    def create_environment_variable_attribute(setting):

        # NOTE from_attribute and must be set
        from_attribute = setting["from_attribute"]

        if "default" in setting:
            default = setting["default"]
            return os.environ.get(from_attribute, default)
        else:
            try:
                return os.environ[from_attribute]
            except KeyError:
                raise EnvironmentSettingNotFoundException(
                    "Cant find environment variable {}".format(from_attribute)
                )

    @staticmethod
    def create_file_attribute(setting):

        # NOTE from_filepath must be set
        from_filepath = setting["from_filepath"]

        try:
            with open(from_filepath, 'r') as file_obj:
                value = file_obj.read()
        except IOError:
            if "default" in setting:
                value = setting["default"]
            else:
                raise EnvironmentSettingNotFoundException(
                    "Cant find file {}".format(from_filepath)
                )

        return value

    @staticmethod
    def create_json_file_attribute(setting):

        output = EnvironmentConfig.create_file_attribute(setting)

        # Output can be a dictionary if its the default value
        # Otherwise, its a json string
        if isinstance(output, str):
            return json.loads(output)
        else:
            return output


