import csv
from enum import Enum
from io import BytesIO, StringIO
from typing import List, Sequence

import fastavro
from airflow.exceptions import AirflowConfigException
from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults


class BlobType(Enum):
    CSV: str = "csv"
    AVRO: str = "avro"


class BlobStorageToPostgresOperator(BaseOperator):
    template_fields: Sequence[str] = "source_blob_name"

    @apply_defaults
    def __init__(
        self,
        blob_storage_conn_id: str,
        postgres_conn_id: str,
        source_container_name: str,
        source_blob_name: str,
        target_table_name: str,
        target_column_names: List[str],
        blob_type: str,
        replace: bool,
        replace_index: str,
        **kwargs,
    ):
        super(BlobStorageToPostgresOperator, self).__init__(**kwargs)
        self._blob_storage_conn_id = blob_storage_conn_id
        self._postgres_conn_id = postgres_conn_id
        self._source_container_name = source_container_name
        self._source_blob_name = source_blob_name
        self._target_table_name = target_table_name
        self._target_column_names = target_column_names
        self._blob_type = BlobType(blob_type.lower())
        self._replace = replace
        self._replace_index = replace_index

    def execute(self, context):
        try:
            self.log.info("Reading records from blob storage...")
            wasb_hook = WasbHook(wasb_conn_id=self._blob_storage_conn_id)
            bytes_buffer = BytesIO()
            download_stream = wasb_hook.download(
                container_name=self._source_container_name,
                blob_name=self._source_blob_name,
            )
            bytes_buffer.write(download_stream.readall())
            bytes_buffer.seek(0)

            if self._blob_type == BlobType.AVRO:
                records = list(fastavro.reader(bytes_buffer))
            elif self._blob_type == BlobType.CSV:
                string_buffer = StringIO(bytes_buffer.getvalue().decode())
                csv_data = csv.reader(string_buffer)
                records = list(csv_data)
                string_buffer.close()

            bytes_buffer.close()
            n_records = len(records)
            self.log.info(f"{n_records} has been loaded.")

            rows = [
                (record[col] for col in self._target_column_names)
                for record in records
            ]
            self.log.info("Inserting records into Postgres.")
            postgres_hook = PostgresHook(
                postgres_conn_id=self._postgres_conn_id
            )
            postgres_hook.insert_rows(
                self._target_table_name,
                rows,
                self._target_column_names,
                replace=self._replace,
                replace_index=self._replace_index,
            )

            self.log.info("Postgres insertion sucessful.")
        except:
            self.log.exception("An error occured.")
            raise Exception
