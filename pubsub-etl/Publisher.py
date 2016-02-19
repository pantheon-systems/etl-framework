"""Subscriber class"""

from PubsubClient import PubsubClient

class Publisher(PubsubClient):
    """publishes messages"""

    def __init__(self, publisher_topic_name, *args, **kwargs):
        """initializes publisher_topic"""

        super(Publisher, self).__init__(*args, **kwargs)

        #set subscription after call to superclass init so project name is set
        self.publisher_topic = self.project + '/topics/' + publisher_topic_name

    def publish(self, messages):
        """
        publish messages
        messages is a list of PublishMessage objects
        """

        body = {
                'messages': [message.get_publish_message() for message in messages]
                }

        resp = self.get_client().projects().topics().publish(
                topic=self.publisher_topic, body=body).execute(
                        num_retries=self.num_retries)

        return resp
 
