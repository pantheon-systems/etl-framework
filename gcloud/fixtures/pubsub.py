from etl_framework.testing.fixtures import FixtureInterface
from etl_framework.gcloud.datastores.utils.PublisherMessage import PublisherMessage
from etl_framework.gcloud.datastores.PubsubPublisher import PubsubPublisher

class PubsubFixture(FixtureInterface):

    def load(self):

        messages = [PublisherMessage(**row) for row in self.data]
        publisher = PubsubPublisher(
            project_name=self.schema.config.project,
            topic_name=self.schema.config.topic
        )

        publisher.publish(messages)
