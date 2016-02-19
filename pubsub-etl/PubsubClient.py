"""
client for google pubsub
this code was more or less copied from https://cloud.google.com/pubsub/configure
"""

import httplib2

from apiclient import discovery
from oauth2client import client as oauth2client

PUBSUB_SCOPES = ['https://www.googleapis.com/auth/pubsub']

class PubsubClient(object):
    """instance of pubsub client"""

    def __init__(self, project_name, http=None, num_retries=3):
        """instantiate pubsub client"""

        self.project = 'projects/' + project_name
        self.http = http
        self.num_retries = num_retries
        self.client = None

    def get_client(self):
        """returns pubsub client"""

        if self.client:
            pass
        else:
            self.client = self._create_client()

        return self.client

    def _create_client(self):
        """creates a pubsub client"""

        credentials = oauth2client.GoogleCredentials.get_application_default()
        if credentials.create_scoped_required():
            credentials = credentials.create_scoped(PUBSUB_SCOPES)

        if not self.http:
            http = httplib2.Http()
        else:
            http = self.http

        credentials.authorize(http)

        return discovery.build('pubsub', 'v1', http=http)

    def create_topic(self, topic_name):
        """
        creates a topic
        name is name of a topic
        """

        topic = self.get_client().projects().topics().create(
                name=self.project + '/topics/' + topic_name, body={}).execute(
                        num_retries=self.num_retries)

        return topic

    def delete_topic(self, topic_name):
        """deletes topic"""

        return self.get_client().projects().topics().delete(
                topic=self.project + '/topics/' + topic_name).execute(
                        num_retries=self.num_retries)

    def iter_topics(self):
        """yields topic objects one at a time"""

        next_page_token = None

        while True:
            resp = self.get_client().projects().topics().list(
                project=self.project,
                pageToken=next_page_token).execute(num_retries=self.num_retries)

            for topic in resp['topics']:
                yield topic

            next_page_token = resp.get('nextPageToken')

            if not next_page_token:
                break

    def create_subscription(self, topic_name, subscription_name, push_endpoint=None):
        """
        creates a subscription to a topic
        if push_endpoint is not set, then it'll be a pull endpoint
        """

        body = {'topic': self.project + '/topics/' + topic_name}

        if push_endpoint:
            body['pushConfig'] = {'pushEndpoint': push_endpoint}


        subscription = self.get_client().projects().subscriptions().create(
                    name=self.project + '/subscriptions/' + subscription_name,
                    body=body).execute(num_retries=self.num_retries)

        return subscription

    def delete_subscription(self, subscription_name):
        """
        deletes a subscription to a topic
        """

        return self.get_client().projects().subscriptions().delete(
                    subscription=self.project + '/subscriptions/' + subscription_name).execute(
                            num_retries=self.num_retries)

    def iter_subscriptions(self):
        """
        lists subscription objects
        """

        next_page_token = None

        while True:
            resp = self.get_client().projects().subscriptions().list(
                project=self.project,
                pageToken=next_page_token).execute(num_retries=self.num_retries)

            for subscription in resp['subscriptions']:
                yield subscription

            next_page_token = resp.get('nextPageToken')

            if not next_page_token:
                break

