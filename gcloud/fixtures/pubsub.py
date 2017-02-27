from etl_framework.testing.fixtures import FixtureInterface
from gcloud.datastores.utils.pubsub_messages import PublisherMessage
from gcloud.datastores.pubsub import PubsubPublisher

class PubsubFixture(FixtureInterface):

    def load(self):

        messages = [PublisherMessage(**row) for row in self.data]
        publisher = PubsubPublisher(
            project_name=self.schema.config.project,
            topic_name=self.schema.config.topic
        )

        publisher.publish(messages)
