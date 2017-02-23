"""Bigquery Loader"""
#pylint: disable=relative-import
#pylint: disable=too-many-function-args
#pylint: disable=too-many-arguments
#pylint: disable=abstract-class-instantiated

from etl_framework.gcloud.datastores.bigquery import BigqueryClient
from etl_framework.loader_mixins.SetConfigMixin import SetConfigMixin

class BigquerySchema(
    BigqueryClient,
    SetConfigMixin,
):
    """loads data into database"""

    def __init__(self, config, *args, **kwargs):

        project_name = config.get_gcloud_project_id()
        dataset_id = config.get_bigquery_dataset_id()

        super(BigquerySchemaModifier, self).__init__(
            project_name=project_name,
            dataset_id=dataset_id,
            *args,
            **kwargs
        )

        self.set_config(config)

    def create_table(self):

        self.insert_table(
            table_id=self.config.get_bigquery_table_id(),
            schema=self.config.get_bigquery_schema(),
            expiration_time=self.config.get_bigquery_table_expiration_time(),
            time_partitioning=self.config.get_bigquery_table_time_partitioning(),
            time_partitioning_expiration=\
                self.config.get_bigquery_table_time_partitioning_expiration(),
        )

    def drop_table(self):
        """stuff"""

        self.delete_table(
            table_id=self.config.get_bigquery_table_id()
        )

    def recreate_table(self):
        """stuff"""

        self.drop_table()
        self.create_table()
