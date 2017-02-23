"""Big query client that wraps google's library"""
#pylint: disable=super-on-old-class
#pylint: disable=too-many-arguments

from etl_framework.gcloud.datastores.mixins.ProjectMixin import ProjectMixin
from etl_framework.gcloud.datastores.mixins.client import ClientMixin
from etl_framework.gcloud.datastores.exceptions import BigqueryInsertError

class BigqueryClient(ProjectMixin, ClientMixin):
    """client with useful methods"""

    # These attributes should be overriden
    CLIENT_SERVICE = 'bigquery'
    CLIENT_SERVICE_VERSION = 'v2'
    CLIENT_SCOPES = ["https://www.googleapis.com/auth/bigquery"]

    def __init__(self, dataset_id=None, *args, **kwargs):

        self.set_dataset_id(dataset_id)

        super(BigqueryClient, self).__init__(*args, **kwargs)

    def clear_connection(self):
        """method necesary to satisfy inteface for a datastore"""

        pass

    def set_dataset_id(self, dataset_id):

        self.dataset_id = dataset_id

        return self.dataset_id

    def query(self, query):
        """runs a query job"""

        body = {
            "query": (
                query
            )
        }

        query_response = self.get_client().jobs().query(
            projectId=self.project_id,
            body=body
        ).execute()

        return query_response

    def insert_dataset(self, dataset_id):

        body = {
            "datasetReference": {
                "datasetId": dataset_id,
            }
        }

        return self.get_client().datasets().insert(
            projectId=self.project_id,
            body=body
        ).execute()

    def delete_dataset(self, dataset_id):

        return self.get_client().datasets().delete(
            projectId=self.project_id,
            datasetId=dataset_id
        ).execute()

    def get_dataset(self, dataset_id):

        return self.get_client().datasets().get(
            projectId=self.project_id,
            datasetId=dataset_id
        ).execute()

    def list_datasets(self):

        return self.get_client().datasets().list(
            projectId=self.project_id,
        ).execute()

    def get_table(self, table_id, dataset_id=None):

        if dataset_id is None:
            dataset_id = self.dataset_id

        return self.get_client().tables().get(
            projectId=self.project_id,
            datasetId=dataset_id,
            tableId=table_id
        ).execute()

    def delete_table(self, table_id, dataset_id=None):

        if dataset_id is None:
            dataset_id = self.dataset_id

        return self.get_client().tables().delete(
            projectId=self.project_id,
            datasetId=dataset_id,
            tableId=table_id,
        ).execute()

    def list_tables(self, dataset_id=None):

        if dataset_id is None:
            dataset_id = self.dataset_id

        return self.get_client().tables().list(
            projectId=self.project_id,
            datasetId=dataset_id,
        ).execute()

    def patch_table(self, table_id,
        schema=None, dataset_id=None, expiration_time=None,
        time_partitioning=False, time_partitioning_expiration=None):
        """updates a table's defnition with the fields specified"""

        if dataset_id is None:
            dataset_id = self.dataset_id

        body = self._create_table_body(
            project_id=self.project_id,
            table_id=table_id,
            schema=schema,
            dataset_id=dataset_id,
            expiration_time=expiration_time,
            time_partitioning=time_partitioning,
            time_partitioning_expiration=time_partitioning_expiration
        )

        return self.get_client().tables().patch(
            projectId=self.project_id,
            datasetId=dataset_id,
            tableId=table_id,
            body=body
        ).execute()

    def update_table(self, table_id,
        schema=None, dataset_id=None, expiration_time=None,
        time_partitioning=False, time_partitioning_expiration=None):
        """replaces a table's defnition with the fields specified"""

        if dataset_id is None:
            dataset_id = self.dataset_id

        body = self._create_table_body(
            project_id=self.project_id,
            table_id=table_id,
            schema=schema,
            dataset_id=dataset_id,
            expiration_time=expiration_time,
            time_partitioning=time_partitioning,
            time_partitioning_expiration=time_partitioning_expiration
        )

        return self.get_client().tables().update(
            projectId=self.project_id,
            datasetId=dataset_id,
            tableId=table_id,
            body=body,
        ).execute()

    def insert_table(self, table_id,
        schema=None, dataset_id=None, expiration_time=None,
        time_partitioning=False, time_partitioning_expiration=None):
        """
        According to https://developers.google.com/resources/api-libraries/\
            documentation/bigquery/v2/python/latest/bigquery_v2.tables.html:
            expiration_time: "time when this table expires,\
                in milliseconds since the epoch"
            time_partitioning_expiration: "Number of milliseconds\
                for which to keep the storage for a partition."
        If time_partitioning is False, time_partitioning_expiration will be ignored
        """

        if dataset_id is None:
            dataset_id = self.dataset_id

        body = self._create_table_body(
            project_id=self.project_id,
            table_id=table_id,
            schema=schema,
            dataset_id=dataset_id,
            expiration_time=expiration_time,
            time_partitioning=time_partitioning,
            time_partitioning_expiration=time_partitioning_expiration
        )

        return self.get_client().tables().insert(
            projectId=self.project_id,
            datasetId=dataset_id,
            body=body
        ).execute()

    def list_data(self, table_id, dataset_id=None,
        page_token=None, max_results=None, start_index=None):

        if dataset_id is None:
            dataset_id = self.dataset_id

        return self.get_client().tabledata().list(
            projectId=self.project_id,
            datasetId=dataset_id,
            tableId=table_id,
            pageToken=page_token,
            maxResults=max_results,
            startIndex=start_index,
        ).execute()

    def insert_data(self, table_id, rows, dataset_id=None):
        """
        rows should have format:
            [
                {
                    "insertId": "",
                    "json": {
                        "a_key": "",
                    },
                },
            ]
        """

        if dataset_id is None:
            dataset_id = self.dataset_id

        body = {
            "rows": rows
        }

        response = self.get_client().tabledata().insertAll(
            projectId=self.project_id,
            datasetId=dataset_id,
            tableId=table_id,
            body=body
        ).execute()

        if "insertErrors" in response:
            raise BigqueryInsertError(response["insertErrors"])

        return response

    @staticmethod
    def _create_table_body(project_id, table_id,
        schema, dataset_id, expiration_time,
        time_partitioning, time_partitioning_expiration):

        body = {
            "sourceFormat": "NEWLINE_DELIMITED_JSON",
            "tableReference": {
                "projectId": project_id,
                "tableId": table_id,
                "datasetId": dataset_id
            }

        }

        if schema is not None:
            body["schema"] = schema

        if time_partitioning:
            time_partitioning_body = {
                "type": "DAY"
            }

            if time_partitioning_expiration:
                time_partitioning_body["expirationMs"] =\
                time_partitioning_expiration

            body["timePartitioning"] = time_partitioning_body

        return body
