"""Subscriber class"""
#pylint: disable=super-on-old-class

from googleapiclient.errors import HttpError

from etl_framework.gcloud.datastores.mixins.TopicMixin import TopicMixin
from etl_framework.gcloud.datastores.mixins.MessageAckerMixin import MessageAckerMixin
from etl_framework.gcloud.datastores.utils.SubscriberMessage import SubscriberMessage
from etl_framework.gcloud.datastores.PubsubClient import PubsubClient

class PubsubSubscriber(PubsubClient, MessageAckerMixin, TopicMixin):
    """pulls messages"""

    def __init__(self, batch_size=100, *args, **kwargs):
        """initializes publisher_topic"""

        self.batch_size = None

        # Note that topic_name is used only to create a non-existent subscription
        # It is not used in request to pull messages

        self.set_batch_size(batch_size)
        self.pull_request_body = {'returnImmediately': True,
                                'maxMessages': self.batch_size
                                }

        super(PubsubSubscriber, self).__init__(*args, **kwargs)

    def set_batch_size(self, batch_size):
        """sets batch_size"""

        self.batch_size = batch_size

    def pull(self, auto_ack=False, create_subscription=False):
        """pulls messages"""

        if create_subscription and self.topic_name:
            try:
                resp = self.get_client().projects().subscriptions().pull(
                    subscription=self.subscription, body=self.pull_request_body).execute(
                        num_retries=self.num_retries)
            except HttpError:
                #create subscription if it doesn't exist
                self.create_subscription(topic_name=self.topic_name,
                    subscription_name=self.subscription_name
                )

                resp = self.get_client().projects().subscriptions().pull(
                    subscription=self.subscription, body=self.pull_request_body).execute(
                        num_retries=self.num_retries)
        else:
            resp = self.get_client().projects().subscriptions().pull(
                subscription=self.subscription, body=self.pull_request_body).execute(
                    num_retries=self.num_retries)

        received_messages = resp.get('receivedMessages')

        if auto_ack and received_messages is not None:
            self.ack_messages([message.get('ackId') for message in received_messages])

        return received_messages

    def iter_pull(self, auto_ack=False):
        """iteratively pulls pubsub request messages"""

        while True:

            received_messages = self.pull(auto_ack=auto_ack)

            #yield recieved pubsub messages one at a time
            if received_messages is not None:
                for received_message in received_messages:
                    if received_message is not None:
                        yield received_message

            #this checks if we should stop
            if self.stop_pulling(received_messages):
                break

    def iter_pull_messages(self, auto_ack=False):
        """iteratively pulls pubsub Subscribe message objects"""

        for received_message in self.iter_pull(auto_ack=auto_ack):
            yield SubscriberMessage(received_message)

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

        return received_messages is None or len(received_messages) < self.batch_size

