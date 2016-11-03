"""stuff"""
from etl_framework.BaseConfig import BaseConfig
from etl_framework.config_mixins.CredentialsMixin import CredentialsMixin

class SchemaConfig(
    BaseConfig,
    CredentialsMixin
):
    """stuff"""
