"""Bigquery Loader"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated

from etl_framework.schemas.schema_interface import SchemaInterface
from gcloud.datastores.bigquery import BigqueryClient

class BigquerySchema(
    SchemaInterface,
):
    """loads data into database"""

    def __init__(self, config, *args, **kwargs):

        super(BigquerySchema, self).__init__(
            config=config,
            *args,
            **kwargs
        )

        self.datastore = BigqueryClient(
            project_name=config.gcloud_project_id,
            dataset_id=config.bigquery_dataset_id
        )

    def create(self):

        self.datastore.insert_table(
            table_id=self.bigquery_table_id,
            schema=self.bigquery_schema,
            expiration_time=self.bigquery_table_expiration_time,
            time_partitioning=self.bigquery_table_time_partitioning,
            time_partitioning_expiration=\
                self.bigquery_table_time_partitioning_expiration,
        )

    def delete(self):
        """stuff"""

        self.datastore.delete_table(
            table_id=self.bigquery_table_id
        )

    def create_if_not_exists(self):

        raise NotImplementedError()

    def delete_if_exists(self):

        raise NotImplementedError()

    def recreate(self):
        """stuff"""

        self.delete()
        self.create()
