"""Bigquery Loader"""
#pylint: disable=relative-import

from etl_framework.gcloud.loaders.bigquery import BigqueryLoader

class BigqueryPartitionedLoader(
    BigqueryLoader,
):
    """loads data into database"""

    def load_buffered(self):

        table_id = self.config.get_loader_table()

        partition = self.config.get_bigquery_table_partition_chooser()()

        if partition is not None:
            table_id = table_id + partition

        self.insert_data(
            table_id=table_id,
            rows=self._buffered_values,
        )

