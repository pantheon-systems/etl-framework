"""Subscriber class"""

from etl_framework.glcoud.datastores.mixins.TopicMixin import TopicMixin
from etl_framework.glcoud.datastores.PubsubClient import PubsubClient

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
