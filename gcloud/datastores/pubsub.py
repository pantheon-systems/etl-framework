"""
client for google pubsub
this code was more or less copied from https://cloud.google.com/pubsub/configure
"""
#pylint: disable=super-on-old-class
import os
import json

from googleapiclient.errors import HttpError

from gcloud.datastores.mixins.message_acker import MessageAckerMixin
from gcloud.datastores.utils.pubsub_messages import SubscriberMessage
from etl_framework.datastore_interfaces.datastore_inteface import DatastoreInterface
from gcloud.datastores.mixins.project import ProjectMixin
from gcloud.datastores.mixins.client import ClientMixin
from etl_framework.glcoud.datastores.mixins.topic import TopicMixin

PUBSUB_SCOPES = ['https://www.googleapis.com/auth/pubsub']

class PubsubClient(DatastoreInterface, ProjectMixin, ClientMixin):
    """instance of pubsub client"""

    CLIENT_SERVICE = 'pubsub'
    CLIENT_SERVICE_VERSION = 'v1'
    CLIENT_SCOPES = ['https://www.googleapis.com/auth/pubsub']

    def __init__(self, num_retries=3, *args, **kwargs):
        """instantiate pubsub client"""

        self.num_retries = num_retries

        super(PubsubClient, self).__init__(*args, **kwargs)

    def set_credentials(self, credentials):

        pass

    def get_connection(self):

        pass

    def _create_connection(self):

        pass

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

class PubsubPublisher(PubsubClient, TopicMixin):
    """publishes messages"""

    def __init__(self, *args, **kwargs):
        """initializes publisher_topic"""

        super(PubsubPublisher, self).__init__(*args, **kwargs)

    def publish(self, messages):
        """
        publish messages
        messages is a list of PublishMessage objects
        """

        body = {
                'messages': [message.get_publisher_message() for message in messages]
                }

        resp = self.get_client().projects().topics().publish(
                topic=self.topic, body=body).execute(
                        num_retries=self.num_retries)

        return resp

class PubsubSubscriber(PubsubClient, MessageAckerMixin, TopicMixin):
    """pulls messages"""

    def __init__(self, batch_size=100, *args, **kwargs):
        """initializes publisher_topic"""

        self.batch_size = None

        # Note that topic_name is used only to create a non-existent subscription
        # It is not used in request to pull messages

        self.set_batch_size(batch_size)
        self.pull_request_body = {'returnImmediately': True,
                                'maxMessages': self.batch_size
                                }

        super(PubsubSubscriber, self).__init__(*args, **kwargs)

    def set_batch_size(self, batch_size):
        """sets batch_size"""

        self.batch_size = batch_size

    def pull(self, auto_ack=False, create_subscription=False):
        """pulls messages"""

        if create_subscription and self.topic_name:
            try:
                resp = self.get_client().projects().subscriptions().pull(
                    subscription=self.subscription, body=self.pull_request_body).execute(
                        num_retries=self.num_retries)
            except HttpError:
                #create subscription if it doesn't exist
                self.create_subscription(topic_name=self.topic_name,
                    subscription_name=self.subscription_name
                )

                resp = self.get_client().projects().subscriptions().pull(
                    subscription=self.subscription, body=self.pull_request_body).execute(
                        num_retries=self.num_retries)
        else:
            resp = self.get_client().projects().subscriptions().pull(
                subscription=self.subscription, body=self.pull_request_body).execute(
                    num_retries=self.num_retries)

        received_messages = resp.get('receivedMessages')

        if auto_ack and received_messages is not None:
            self.ack_messages([message.get('ackId') for message in received_messages])

        return received_messages

    def iter_pull(self, auto_ack=False):
        """iteratively pulls pubsub request messages"""

        while True:

            received_messages = self.pull(auto_ack=auto_ack)

            #yield recieved pubsub messages one at a time
            if received_messages is not None:
                for received_message in received_messages:
                    if received_message is not None:
                        yield received_message

            #this checks if we should stop
            if self.stop_pulling(received_messages):
                break

    def iter_pull_messages(self, auto_ack=False):
        """iteratively pulls pubsub Subscribe message objects"""

        for received_message in self.iter_pull(auto_ack=auto_ack):
            yield SubscriberMessage(received_message)

    def stop_pulling(self, received_messages):
        """
        returns True or False for if Subscriber should stop pulling
        There ideally could be 5 reasons:
            1. number_received messages < batch_size
            2. reached some time_threshold
            3. a message with a "watermark" was obtained
            4. pulled more than threshold
            5. some user_input
        """

        return received_messages is None or len(received_messages) < self.batch_size

class TopicLogger(PubsubSubscriber):
    """writes pubsub request messages to disk"""

    #PUBLISH_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
    DEFAULT_FILENAME = 'UNKNOWN'

    def __init__(self, log_directory, ack_batch_size=100, *args, **kwargs):
        """creates instance"""

        self.log_directory = log_directory
        self.ack_batch_size = ack_batch_size

        #private attributes used to keep track of logging files
        self._current_file_obj = None

        super(TopicLogger, self).__init__(*args, **kwargs)

    def iter_pull_messages_from_log(self, filenames):
        """
        creates subscribe message objects from files in log
        filenames is list of base filenames to read from
        """

        for filename in filenames:
            filepath = os.path.join(self.log_directory, filename)

            with open(filepath, 'r') as file_obj:
                for line in file_obj:
                    yield SubscriberMessage(json.loads(line.strip()))

    def iter_log(self):
        """writes files to log"""

        ack_ids = []

        try:
            for received_message in self.iter_pull(auto_ack=False):

                #keep track of ack_ids since we aren't auto_acking
                ack_ids.append(received_message.get('ackId'))

                current_file_obj = self._get_file_obj(received_message)
                current_file_obj.write(json.dumps(received_message) + '\n')

                #we ack if there are more than ack_batch_size number of ack_ids
                if ack_ids and len(ack_ids) >= self.ack_batch_size:
                    self.ack_messages(ack_ids)
                    ack_ids = []

        finally:

            #make sure to ack remaining messages
            if ack_ids:
                self.ack_messages(ack_ids)

            #make sure to close file_obj if necessary
            if self._current_file_obj:
                self._current_file_obj.close()
                self._current_file_obj = None

    def _get_file_obj(self, received_message):
        """
        returns the file obj for given filename
        also handles closing current existing file_obj if necessary
        """

        new_filepath = self._get_filepath(received_message)

        if self._current_file_obj:

            if self._current_file_obj.name == new_filepath:
                pass
            else:
                self._current_file_obj.close()
                self._current_file_obj = open(new_filepath, 'a')
        else:
            self._current_file_obj = open(new_filepath, 'a')

        return self._current_file_obj

    def _get_filepath(self, received_message):
        """creates log filepath for received_message"""

        filename = self.DEFAULT_FILENAME

        pubsub_message = received_message.get('message')
        if pubsub_message:
            publish_time = pubsub_message.get('publishTime')
            if publish_time:
                #filename should be date of publishTime
                filename = publish_time.split('T')[0]

        return os.path.join(self.log_directory, filename)
