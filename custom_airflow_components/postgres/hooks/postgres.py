import csv
from io import StringIO
from typing import List
from contextlib import closing

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import DatabaseError


class CustomPostgresHook(PostgresHook):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def copy_from_records(self, table, record_list: List[dict], header: List[str]) -> None:
        self.log.info("Running copy from: %s", table)

        if len(record_list) != 0:
            buffer = StringIO()
            csv_writer = csv.DictWriter(
                buffer, fieldnames=header,
            )
            csv_writer.writerows(record_list)
            buffer.seek(0)
            with closing(self.get_conn()) as conn:
                with closing(conn.cursor()) as cur:
                    cur.copy_from(buffer, table, sep=",")
                    conn.commit()
            buffer.close()
