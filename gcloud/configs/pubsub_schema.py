from etl_framework.BaseConfig import BaseConfig

class PubsubSchemaConfig(
    BaseConfig
):
    """parses configuration files"""

    @property
    def project(self):

        return self.config["project"]

    @property
    def subscription(self):

        return self.config["subscription"]

    @property
    def topic(self):

        return self.config["topic"]
