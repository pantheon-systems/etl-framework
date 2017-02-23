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

class PublisherMessage(PubsubMessage):
    """Pubsub publish message object"""

    def __init__(self, data, attributes=None, *args, **kwargs):
        """
        data is a dictionary
        attributes is a dictionary
        """

        super(PublisherMessage, self).__init__(*args, **kwargs)

        self.data = data
        self.attributes = attributes or {}

    def get_publisher_message(self):
        """returns pubsub message"""

        return {
                'data': self.format_data(self.data),
                'attributes': self.attributes
                }

class SubscriberMessage(PubsubMessage):
    """Pubsub publish message object"""

    def __init__(self, received_message, *args, **kwargs):
        """
        data is a dictionary
        attributes is a dictionary
        """

        super(SubscriberMessage, self).__init__(*args, **kwargs)

        self.message_id = None
        self.publish_time = None
        self.ack_id = None

        self.set_subscribe_message(received_message)

    @staticmethod
    def create_message_dict(received_message):
        """
        creates a received message dictionary with set fields
        or None if message is empty
        """
        if received_message:
            pubsub_message = received_message.get('message')

            if pubsub_message:
                message = dict()
                message['data'] = SubscriberMessage.unformat_data(pubsub_message.get('data'))
                message['attributes'] = pubsub_message.get('attributes')
                message['messageId'] = pubsub_message.get('messageId')
                message['publishTime'] = pubsub_message['publishTime']
                message['ackId'] = received_message.get('ackId')
                return message

        return None

    def set_subscribe_message(self, received_message):
        """
        sets a subscribe message from a pull received_message
        OR a dictionary created from log files
        """

        if received_message:
            pubsub_message = received_message.get('message')

            if pubsub_message:
                self.data = self.unformat_data(pubsub_message.get('data'))
                self.attributes = pubsub_message.get('attributes')
                self.message_id = pubsub_message.get('messageId')
                self.publish_time = pubsub_message.get('publishTime')
                self.ack_id = received_message.get('ackId')
