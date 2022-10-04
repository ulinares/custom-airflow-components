from typing import List, Optional, Sequence

from airflow.models.baseoperator import BaseOperator
from airflow.providers.microsoft.azure.hooks.azure_cosmos import \
    AzureCosmosDBHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults


class AzureCosmosToPostgresOperator(BaseOperator):
    template_fields: Sequence[str] = ("source_azure_cosmos_sql_query",)

    @apply_defaults
    def __init__(
        self,
        source_azure_cosmos_conn_id: str,
        source_azure_cosmos_db_name: str,
        source_azure_cosmos_collection_name: str,
        source_azure_cosmos_sql_query: Optional[str],
        target_postgres_conn_id: str,
        target_table_name: str,
        target_column_names: List[str],
        replace: bool,
        replace_index: str,
        **kwargs,
    ) -> None:
        super(AzureCosmosToPostgresOperator, self).__init__(**kwargs)

        self._source_cosmos_conn_id = source_azure_cosmos_conn_id
        self._source_cosmos_db_name = source_azure_cosmos_db_name
        self._source_cosmos_collection_name = (
            source_azure_cosmos_collection_name
        )
        self._source_cosmos_query = source_azure_cosmos_sql_query
        self._target_postgres_conn_id = target_postgres_conn_id
        self._target_table_name = target_table_name
        self._target_column_names = target_column_names
        self._replace = replace
        self._replace_index = replace_index

    def execute(self, context):
        try:
            cosmos_hook = AzureCosmosDBHook(self._source_cosmos_conn_id)
            sql_query = self._source_cosmos_query

            if sql_query is None:
                sql_query = "SELECT * FROM c"

            self.log.info("Downloading Cosmos records...")
            cosmos_records = cosmos_hook.get_documents(
                sql_string=sql_query,
                database_name=self._source_cosmos_db_name,
                collection_name=self._source_cosmos_collection_name,
            )
            n_records = len(cosmos_records)
            self.log.info(f"Downloaded {n_records} records from Cosmos.")

            rows = [
                (record[col] for col in self._target_column_names)
                for record in cosmos_records
            ]

            postgres_hook = PostgresHook(
                postgres_conn_id=self._target_postgres_conn_id
            )
            self.log.info("Inserting records into Postgres...")
            postgres_hook.insert_rows(
                self._target_table_name,
                rows,
                self._target_column_names,
                replace=self._replace,
                replace_index=self._replace_index,
            )
            self.log.info("Insertion into Postgres complete.")
        except:
            self.log.exception("An error ocurred.")
            raise Exception
