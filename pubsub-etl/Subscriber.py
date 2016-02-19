"""Subscriber class"""

import base64

from PubsubClient import PubsubClient

class Subscriber(PubsubClient):
    """pulls messages"""

    def __init__(self, subscription_name, batch_size=100, *args, **kwargs):
        """initializes publisher_topic"""

        self.batch_size = batch_size

        super(Subscriber, self).__init__(self, *args, **kwargs)

        #set subscription after call to superclass init so project name is set
        self.subscription = 'projects/' + self.project + '/subscriptions/' + subscription_name

    def pull_messages(self, body):
        """pulls messages"""

        resp = self.get_client().projects().topics().publish(
        topic = 'projects/' + self.project + '/topics/' + self.publisher_topic, body=body).
                execute(num_entries=self.num_entries)

        return resp
 
