"""parses configuration and returns useful things"""
#pylint: disable=relative-import
#pylint: disable=super-on-old-class

from etl_framework.BaseConfig import BaseConfig

class ResultConfig(BaseConfig):
    """parses configuration files"""

    @property
    def error_message(self):

        return self.config["error_message"]

    @property
    def schema_from_filepath(self):

        return self.config["schema"]["from_filepath"]

    @property
    def data(self):

        return self.config["data"]
