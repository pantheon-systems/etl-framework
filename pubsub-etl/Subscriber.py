"""Subscriber class"""

from SubscribeMessage import SubscribeMessage
from PubsubClient import PubsubClient

class Subscriber(PubsubClient):
    """pulls messages"""

    def __init__(self, subscription_name, batch_size=100, *args, **kwargs):
        """initializes publisher_topic"""

        self.batch_size = batch_size
        self.pull_request_body = {'returnImmediately': True,
                                'maxMessages': self.batch_size
                                }

        super(Subscriber, self).__init__(*args, **kwargs)

        #set subscription after call to superclass init so project name is set
        self.subscription = self.project + '/subscriptions/' + subscription_name

    def pull(self, auto_ack=True):
        """pulls messages"""

        resp = self.get_client().projects().subscriptions().pull(
                subscription=self.subscription, body=self.pull_request_body).execute(
                    num_retries=self.num_retries)

        received_messages = resp.get('receivedMessages')

        if auto_ack and received_messages is not None:
            self.ack_messages([message.get('ackId') for message in received_messages])

        return received_messages

    def iter_pull_messages(self, auto_ack=True):
        """iteratively pulls pubsub messages and ackId's"""

        while True:

            received_messages = self.pull(auto_ack=auto_ack)

            #yield recieved pubsub messages one at a time
            if received_messages is not None:
                for received_message in received_messages:
                    yield SubscribeMessage(received_message)

            #this checks if we should stop
            if self.stop_pulling(received_messages):
                break

    def ack_messages(self, ack_ids):
        """acks the messages"""

        # Create a POST body for the acknowledge request
        ack_body = {'ackIds': ack_ids}

        # Acknowledge the messages
        self.get_client().projects().subscriptions().acknowledge(
            subscription=self.subscription, body=ack_body).execute(num_retries=self.num_retries)

    def stop_pulling(self, received_messages):
        """
        returns True or False for if Subscriber should stop pulling
        There ideally could be 5 reasons:
            1. number_received messages < batch_size
            2. reached some time_threshold
            3. a message with a "watermark" was obtained
            4. pulled more than threshold
            5. some user_input
        """

        if received_messages is None or len(received_messages) < self.batch_size:
            return True
        else:
            return False

