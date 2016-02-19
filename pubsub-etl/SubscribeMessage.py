"""
pubsub subscribe message object
"""

import base64
import json

from PubsubMessage import PubsubMessage

class SubscribeMessage(PubsubMessage):
    """Pubsub publish message object"""

    def __init__(self, request_message, *args, **kwargs):
        """
        data is a dictionary
        attributes is a dictionary
        """

        super(SubscribeMessage, self).__init__(*args, **kwargs)

        self.message_id = None
        self.publish_time = None
        self.ack_id = None

        self.set_subscribe_message(request_message)

    def set_subscribe_message(self, request_message):
        """sets a subscribe message from a pull request_message"""

        if request_message:
            pubsub_message = request_message.get('message')

            if pubsub_message:
                self.data = self.unformat_data(pubsub_message.get('data'))
                self.attributes = pubsub_message.get('attributes')
                self.message_id = pubsub_message.get('messageId')
                self.publish_time = pubsub_message.get('publishTime')
                self.ack_id = request_message.get('ackId')

