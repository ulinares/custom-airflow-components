import csv
from typing import Sequence
from io import BytesIO, StringIO

from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.cosmos import AzureCosmosDBHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.utils.decorators import apply_defaults
from airflow.utils.context import Context
from azure.cosmos.exceptions import CosmosHttpResponseError


class CosmosToBlobStorageOperator(BaseOperator):
    template_fields: Sequence[str] = ("target_blob_name",)

    @apply_defaults
    def __init__(
        self,
        cosmos_conn_id: str,
        blob_storage_conn_id: str,
        source_cosmos_database_name: str,
        source_cosmos_collection_name: str,
        target_blob_container_name: str,
        target_blob_name: str,
        replace: bool = True,
        **kwargs,
    ):
        super(CosmosToBlobStorageOperator, self).__init__(**kwargs)
        self._cosmos_conn_id = cosmos_conn_id
        self._blob_storage_conn_id = blob_storage_conn_id
        self._source_cosmos_database_name = source_cosmos_database_name
        self._source_cosmos_collection_name = source_cosmos_collection_name
        self._target_blob_container_name = target_blob_container_name
        self._target_blob_name = target_blob_name
        self._replace = replace

    def execute(self, context: Context):
        cosmos_hook = AzureCosmosDBHook(
            azure_cosmos_conn_id=self._cosmos_conn_id
        )

        start_ts = int(context["execution_date"].timestamp())
        end_ts = int(context["next_execution_date"].timestamp())

        sql_string = (
            f"select * from c where c._ts >= {start_ts} and c._ts <= {end_ts}"
        )

        try:
            # I need to re-implement this because the get_documents method from CosmosHook doesn't support cross_partition_query
            cosmos_container = (
                cosmos_hook.get_conn()
                .get_database_client(self._source_cosmos_database_name)
                .get_container_client(self._source_cosmos_collection_name)
            )

            result_iterable = cosmos_container.query_items(
                sql_string, enable_cross_partition_query=True
            )
            records = list(result_iterable)
        except CosmosHttpResponseError:
            self.log.exception("An error occurred while querying Cosmos.")

        header = {col for record in records for col in record}
        string_buffer = StringIO()
        csv_writer = csv.DictWriter(string_buffer, fieldnames=header)
        csv_writer.writeheader()
        csv_writer.writerows(records)

        bytes_buffer = BytesIO(string_buffer.getvalue().encode())
        string_buffer.close()
        bytes_buffer.seek(0)

        wasb_hook = WasbHook(wasb_conn_id=self._blob_storage_conn_id)
        wasb_hook.upload(
            container_name=self._target_blob_container_name,
            blob_name=self._target_blob_name,
            data=bytes_buffer,
            overwrite=self._replace,
        )
        bytes_buffer.close()
