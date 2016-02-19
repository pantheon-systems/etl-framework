"""Subscriber class"""

from PubsubClient import PubsubClient

class Publisher(PubsubClient):
    """publishes messages"""

    def __init__(self, publisher_topic_name, *args, **kwargs):
        """initializes publisher_topic"""

        super(Publisher, self).__init__(self, *args, **kwargs)

        #set subscription after call to superclass init so project name is set
        self.publisher_topic = 'projects/' + self.project + '/topics/' + publisher_topic_name

    def publish(self, messages):
        """
        publish messages
        messages is a list of PubsubMessage objects
        """

        body = {
                'messages': [message.get_message() for message in messages]
                }

        resp = self.get_client().projects().topics().publish(
                topic=self.publisher_topic, body=body).
                execute(num_entries=self.num_entries)

        return resp
 
