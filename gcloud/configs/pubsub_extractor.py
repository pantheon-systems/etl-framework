"""parses configuration and returns useful things"""
#pylint: disable=relative-import

from etl_framework.ExtractorConfig import ExtractorConfig
from etl_framework.config_mixins.SleepMixin import SleepMixin
from etl_framework.config_mixins.BatchMixin import BatchMixin
from etl_framework.config_mixins.FiltersMixin import FiltersMixin

#from etl_framework.config_mixins.DestinationMixin import DestinationMixin
from gcloud.configs.mixins.gcloud import GcloudMixin

class PubsubExtractorConfig(
    ExtractorConfig,
    FiltersMixin,
    GcloudMixin,
    SleepMixin,
    BatchMixin
):
    """parses configuration files"""

    @property
    def message_flusher(self):
        """stuff"""

        return self.config.get('message_flusher')

    @message_flusher.setter
    def message_flusher(self, message_flusher):
        """stuff"""

        self.config['message_flusher'] = message_flusher

    @property
    def pubsub_topic_name(self):
        """stuff"""

        return self.config.get('pubsub_topic_name')

	@property
    def extractor_loaders(self):
        """stuff"""

        return self.config.get('extractor_loaders')
