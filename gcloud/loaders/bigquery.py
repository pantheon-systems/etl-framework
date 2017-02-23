"""Bigquery Loader"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated

from etl_framework.BaseLoader import BaseLoader
from etl_framework.loader_mixins.BufferMixin import BufferMixin

from gcloud.datastores.bigquery import BigqueryClient

class BigqueryLoader(
    BaseLoader,
    BufferMixin,
):
    """loads data into database"""

    def __init__(self, config, *args, **kwargs):

        super(BigqueryLoader, self).__init__(
            config=config,
            *args,
            **kwargs
        )

        self.datastore = BigqueryClient(
            project_name=config.gcloud_project_id,
            dataset_id=config.bigquery_dataset_id
        )

    def load_buffered(self):

        # NOTE this is specific to SQL loaders,
        # so this class could be renamed SqlBufferedLoader
        self.datastore.insert_data(
            table_id=self.config.get_loader_table(),
            rows=self._buffered_values,
        )

    def load(self, row):
        """stuff"""

        row = self.bigquery_insert_filter()(row)
        self.write_to_buffer(row)
