"""Mixin for PubsubClient to have Topic name attribute"""

from gcloud.datastores.mixins.project import ProjectMixin

class TopicMixin(ProjectMixin):
    """mixin to ack Pubsub messages"""

    def __init__(self, topic_name, *args, **kwargs):
        """creates instance"""

        self.topic_name = topic_name

        super(TopicMixin, self).__init__(*args, **kwargs)

        self.topic = self.project + '/topics/' + self.topic_name

