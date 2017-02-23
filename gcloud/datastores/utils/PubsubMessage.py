"""pubsub message object"""

import base64
import json

class PubsubMessage(object):
    """Pubsub message object"""

    def __init__(self):
        """
        data is a dictionary
        attributes is a dictionary
        """

        self.data = None
        self.attributes = None

    def set_data(self, data):
        """sets data attribute"""

        self.data = data

    def set_attributes(self, attributes):
        """sets attributes value"""

        self.attributes = attributes

    @staticmethod
    def format_data(data):
        """turns data into format necessary to send to pubsub"""

        return base64.b64encode(json.dumps(data))

    @staticmethod
    def unformat_data(data):
        """turns formatted pubsub data into native python object"""

        return json.loads(base64.b64decode(str(data)))
