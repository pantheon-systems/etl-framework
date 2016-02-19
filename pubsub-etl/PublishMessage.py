"""
pubsub publish message object
"""

import base64
import json

from PubsubMessage import PubsubMessage

class PublishMessage(PubsubMessage):
    """Pubsub publish message object"""

    def __init__(self, data, attributes=None, *args, **kwargs):
        """
        data is a dictionary
        attributes is a dictionary
        """

        super(PublishMessage, self).__init__(*args, **kwargs)

        self.data = data
        self.attributes = attributes or {}

    def get_publish_message(self):
        """returns pubsub message"""

        return {
                'data': self.format_data(self.data),
                'attributes': self.attributes
                }

