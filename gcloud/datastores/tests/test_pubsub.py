"""tests pubsub-etl"""

import os
import shutil
import unittest

from googleapiclient.errors import HttpError

from etl_framework.gcloud.datastores.PubsubClient import PubsubClient
from etl_framework.gcloud.datastores.PubsubPublisher import PubsubPublisher
from etl_framework.gcloud.datastores.PubsubSubscriber import PubsubSubscriber
from etl_framework.gcloud.datastores.TopicLogger import TopicLogger
from etl_framework.gcloud.datastores.utils.PublisherMessage import PublisherMessage

class PubSubTestCases(unittest.TestCase):
    """tests pubsub classes"""

    PROJECT_NAME = 'test'
    TOPIC_NAME = 'pubusb-test'
    SUBSCRIPTION = 'pubsub-test'
    LOG_SUBSCRIPTION = 'logging'
    LOG_DIRECTORY = 'test_logs'
    DATA_VALUES = ['message %d'%index for index in range(10)]
    TEST_MESSAGES = [PublisherMessage({'test': message}, {'test': 'yes'})
                        for message in DATA_VALUES]

    @classmethod
    def setUpClass(cls):
        """does setup"""

        #recreate test_directory
        try:
            shutil.rmtree(cls.LOG_DIRECTORY)
        except OSError:
            pass

        os.mkdir(cls.LOG_DIRECTORY)

        cls.client = PubsubClient(project_name=cls.PROJECT_NAME)

        #create topic
        try:
            cls.client.create_topic(cls.TOPIC_NAME)
        except HttpError:
            pass

        #create subscriptions
        try:
            cls.client.create_subscription(topic_name=cls.TOPIC_NAME, subscription_name=cls.SUBSCRIPTION)
        except HttpError:
            pass

        try:
            cls.client.create_subscription(topic_name=cls.TOPIC_NAME, subscription_name=cls.LOG_SUBSCRIPTION)
        except HttpError:
            pass

        cls.publisher = PubsubPublisher(project_name=cls.PROJECT_NAME, topic_name=cls.TOPIC_NAME)
        cls.subscriber = PubsubSubscriber(
            project_name=cls.PROJECT_NAME,
            subscription_name=cls.SUBSCRIPTION,
            topic_name=cls.TOPIC_NAME
        )
        cls.logger = TopicLogger(project_name=cls.PROJECT_NAME,
                            subscription_name=cls.LOG_SUBSCRIPTION,
                            topic_name=cls.TOPIC_NAME,
                            log_directory=cls.LOG_DIRECTORY,
                            ack_batch_size=2)

        cls.publisher.publish(cls.TEST_MESSAGES)

        #get output messages
        cls.output_messages = [message for message in cls.subscriber.iter_pull_messages()]

        #get log output messages
        cls.logger.iter_log()
        log_files = os.listdir(cls.LOG_DIRECTORY)
        cls.output_log_messages = [message for message in cls.logger.iter_pull_messages_from_log(log_files)]

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

    def test_log_ack_ids(self):
        """tests ack ids exist for each output_log message"""

        self.assertTrue(all(message.ack_id for message in self.output_log_messages))

    def test_log_data(self):
        """tests data matches cls.DATA_VALUES for each output_log message"""

        data_values = set([message.data['test'] for message in self.output_log_messages])
        self.assertTrue(data_values == set(self.DATA_VALUES))

    def test_log_attributes(self):
        """test attribute values set correctly for each output_log message"""

        attributes_test = [message.attributes['test'] == 'yes' for message in self.output_log_messages]
        self.assertTrue(all(attributes_test))

    def test_log_message_ids(self):
        """test messages_ids exist for each output_log message"""

        self.assertTrue(all(message.message_id for message in self.output_log_messages))

    def test_log_publish_times(self):
        """tests publish times are correct for each output_log message"""

        self.assertTrue(all(message.publish_time for message in self.output_log_messages))

    @classmethod
    def tearDownClass(cls):
        """tears down"""

        #delete subscriptions
        cls.client.delete_subscription(subscription_name=cls.SUBSCRIPTION)
        cls.client.delete_subscription(subscription_name=cls.LOG_SUBSCRIPTION)

        #delete topic
        cls.client.delete_topic(topic_name=cls.TOPIC_NAME)

        #delete log directory
        shutil.rmtree(cls.LOG_DIRECTORY)


