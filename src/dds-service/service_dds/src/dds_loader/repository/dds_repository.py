from datetime import datetime
from typing import Dict, List

from lib.pg import PgConnect

class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
        
    def hub_table_insert(self,
                         table_name: str,
                         data: Dict,
                         uniq_fields: List = []) -> None:
        columns = data.keys()
        values = [data[column] for column in columns]
        query = f'''INSERT INTO {table_name} ({', '.join(columns)}) VALUES {tuple(values)}'''
        if uniq_fields:
            update_fields = self._without_keys(data, uniq_fields)
            query += f'''ON CONFLICT ({', '.join(uniq_fields)}) DO UPDATE \n
            SET
            '''
            for i, field in enumerate(update_fields):
                query += f' {field} = EXCLUDED.{field}'
                if i < (len(update_fields)-1):
                    query += ', \n'
                
                
        # print(query)
            
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                
    @staticmethod
    def _without_keys(dict_data, list_to_exclude):
        return {x: dict_data[x] for x in dict_data if x not in list_to_exclude}

    def order_events_insert(self,
                            object_id: int,
                            object_type: str,
                            sent_dttm: datetime,
                            payload: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stg.order_events (object_id, object_type, sent_dttm, payload) VALUES (
                        %(object_id)s, %(object_type)s, %(sent_dttm)s, %(payload)s
                    )
                    ON CONFLICT (object_id) DO UPDATE
                        SET object_type = EXCLUDED.object_type,
                            sent_dttm = EXCLUDED.sent_dttm,
                            payload = EXCLUDED.payload;
                    """,
                    {
                        'object_id': object_id,
                        'object_type': object_type,
                        'sent_dttm': sent_dttm,
                        'payload': payload
                    }
                )