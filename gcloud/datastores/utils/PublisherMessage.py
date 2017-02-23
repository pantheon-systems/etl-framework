"""
pubsub publish message object
"""

from etl_framework.gcloud.datastores.utils.PubsubMessage import PubsubMessage

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

