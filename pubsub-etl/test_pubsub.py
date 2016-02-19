"""tests pubsub-etl"""

import unittest

from googleapiclient.errors import HttpError

from PubsubClient import PubsubClient
from Publisher import Publisher
from Subscriber import Subscriber
from PublishMessage import PublishMessage
from SubscribeMessage import SubscribeMessage

class PubSubTestCases(unittest.TestCase):
    """tests pubsub classes"""

    PROJECT_NAME = 'pantheon-internal'
    TOPIC_NAME = 'test'
    SUBSCRIPTION = 'test'
    DATA_VALUES = ['message %d'%index for index in range(10)]
    TEST_MESSAGES = [PublishMessage({'test': message}, {'test': 'yes'})
                        for message in DATA_VALUES]

    @classmethod
    def setUpClass(cls):
        """does setup"""

        cls.client = PubsubClient(cls.PROJECT_NAME)

        #create topic
        try:
            cls.client.create_topic(cls.TOPIC_NAME)
        except HttpError:
            pass

        #create subscription
        try:
            cls.client.create_subscription(topic_name=cls.TOPIC_NAME, subscription_name=cls.SUBSCRIPTION)
        except HttpError:
            pass

        cls.publisher = Publisher(project_name=cls.PROJECT_NAME, publisher_topic_name=cls.TOPIC_NAME)
        cls.subscriber = Subscriber(project_name=cls.PROJECT_NAME, subscription_name=cls.SUBSCRIPTION)

        cls.publisher.publish(cls.TEST_MESSAGES)

        cls.output_messages = [message for message in cls.subscriber.iter_pull_messages()]

    def test_ack_ids(self):
        """tests ack ids exist for each output message"""

        self.assertTrue(all(message.ack_id for message in self.output_messages))

    def test_data(self):
        """tests data matches cls.DATA_VALUES for each output message"""

        data_values = set([message.data['test'] for message in self.output_messages])
        self.assertTrue(data_values == set(self.DATA_VALUES))

    def test_attributes(self):
        """test attribute values set correctly for each output message"""

        attributes_test = [message.attributes['test'] == 'yes' for message in self.output_messages]
        self.assertTrue(all(attributes_test))

    def test_message_ids(self):
        """test messages_ids exist for each output message"""

        self.assertTrue(all(message.message_id for message in self.output_messages))

    def test_publish_times(self):
        """tests publish times are correct for each output message"""

        self.assertTrue(all(message.publish_time for message in self.output_messages))

    @classmethod
    def tearDownClass(cls):
        """tears down"""

        #delete subscription
        cls.client.delete_subscription(subscription_name=cls.SUBSCRIPTION)

        #delete topic
        cls.client.delete_topic(topic_name=cls.TOPIC_NAME)

