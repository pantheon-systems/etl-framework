"""Base class to load data into data warehouse"""

from simple_salesforce import Salesforce

from etl_framework.Exceptions import InvalidCredentialsException
from etl_framework.datastore_interface.datastore_interface import DatastoreInterface

class SalesforceApi(DatastoreInteface):
    """loads data into database"""

    def set_credentials(self, credentials):
        """credentials should be a named tuple"""


        for attribute in ("username", "password", "security_token"):
            if not hasattr(credentials, attribute):
                raise InvalidCredentialsException(
                    "credentials should be namedtuple with {} attribute"\
                        .format(attribute)
                )

        self.credentials = credentials

    def get_connection(self):
        """returns a connection"""

        if not self.con:
            self.con = self._create_connection()

        return self.con

    def _create_connection(self):
        """creates a new connection and returns it"""

        return Salesforce(
            username=credentials.username,
            password=credentials.password,
            security_token=credentials.security_token,
        )

    def insert(self, object_name, row):

    def delete(self, object_name, 
