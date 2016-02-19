"""pubsub message object"""

import base64
import json

class PubsubMessage(object):
    """Pubsub message object"""

    def __init__(data, attributes=None):
        """
        data is a dictionary
        attributes is a dictionary
        """

        self.data = data
        self.attributes = attributes or {}

    def set_data(self, data):
        """sets data attribute"""

        self.data = data

    def set_attributes(self, attributes):
        """sets attributes value"""

        self.attributes = attributes

    def get_message(self):
        """returns pubsub message"""

        return {
                'data': base64.b64encode(json.loads(self.data)),
                'attributes': self.attributes
                }
