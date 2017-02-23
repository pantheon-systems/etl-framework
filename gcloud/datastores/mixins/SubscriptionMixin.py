"""Mixin for PubsubClient to have subscription attribute"""
#pylint: disable=super-on-old-class

from etl_framework.gcloud.datastores.mixins.ProjectMixin import ProjectMixin

class SubscriptionMixin(ProjectMixin):
    """mixin to ack Pubsub messages"""

    def __init__(self, subscription_name, *args, **kwargs):
        """creates instance"""

        self.subscription_name = subscription_name

        super(SubscriptionMixin, self).__init__(*args, **kwargs)

        #set subscription after call to superclass init so project name is set
        self.subscription = self.project + '/subscriptions/' + subscription_name
