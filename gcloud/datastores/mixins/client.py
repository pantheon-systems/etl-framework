"""Mixin for getting client to a glcoud service"""

import httplib2

from apiclient import discovery
from oauth2client import client as oauth2client

class ClientMixin(object):

    # These attributes should be overriden
    CLIENT_SERVICE = None
    CLIENT_SERVICE_VERSION = None
    CLIENT_SCOPES = []

    def __init__(self, http=None, *args, **kwargs):

        self._http = http
        self._client = None

        super(ClientMixin, self).__init__(*args, **kwargs)

    def get_client(self):
        """returns gcloud client"""

        if self._client:
            pass
        else:
            self._client = self._create_client()

        return self._client

    def _create_client(self):
        """creates a gcloud client"""

        credentials = oauth2client.GoogleCredentials.get_application_default()
        if credentials.create_scoped_required():
            credentials = credentials.create_scoped(self.CLIENT_SCOPES)

        if not self._http:
            http = httplib2.Http()
        else:
            http = self._http

        credentials.authorize(http)

        return discovery.build(
            self.CLIENT_SERVICE,
            self.CLIENT_SERVICE_VERSION,
            http=http
        )
