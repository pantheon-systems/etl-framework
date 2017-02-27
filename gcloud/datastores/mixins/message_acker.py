"""Mixin for PubsubClient to ack messages"""
#pylint: disable=super-on-old-class

from gcloud.datastores.mixins.subscription import SubscriptionMixin

class MessageAckerMixin(SubscriptionMixin):
    """mixin to ack Pubsub messages"""

    def __init__(self, *args, **kwargs):
        """creates instance"""

        self.ack_ids = None
        self._clear_ack_ids()

        super(MessageAckerMixin, self).__init__(*args, **kwargs)

    def ack(self):

        if self.ack_ids:
            self.ack_messages(self.ack_ids)
            self._clear_ack_ids()

    def append_ack_id(self, ack_id):
        """appends an ack_id to buffered ack_ids"""

        self.ack_ids.append(ack_id)

    def extend_ack_ids(self, ack_ids):
        """extends buffer of ack_ids"""

        self.ack_ids.extend(ack_ids)

    def _clear_ack_ids(self):
        """clears buffer of ack_ids"""

        self.ack_ids = []

    def ack_messages(self, ack_ids):
        """acks the messages"""

        # Create a POST body for the acknowledge request
        ack_body = {'ackIds': ack_ids}

        # Acknowledge the messages
        self.get_client().projects().subscriptions().acknowledge(
            subscription=self.subscription, body=ack_body).execute(num_retries=self.num_retries)

