"""Bigquery Loader"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated

from etl_framework.BaseLoader import BaseLoader
from etl_framework.loader_mixins.BufferMixin import BufferMixin

from etl_framework.gcloud.datastores.bigquery import BigqueryClient

class BigqueryLoader(
    BigqueryClient,
    BaseLoader,
    BufferMixin,
):
    """loads data into database"""

    def __init__(self, config, *args, **kwargs):

        project_name = config.get_gcloud_project_id()
        dataset_id = config.get_bigquery_dataset_id()

        super(BigqueryLoader, self).__init__(
            config=config,
            project_name=project_name,
            dataset_id=dataset_id,
            *args,
            **kwargs
        )

    def load_buffered(self):

        # NOTE this is specific to SQL loaders,
        # so this class could be renamed SqlBufferedLoader
        self.insert_data(
            table_id=self.config.get_loader_table(),
            rows=self._buffered_values,
        )

    def load(self, row):
        """stuff"""

        row = self.config.get_bigquery_insert_filter()(row)
        self.write_to_buffer(row)

    def set_db_credentials_from_config(self):
        """stuff"""

        pass
