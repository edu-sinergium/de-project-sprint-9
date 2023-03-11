from datetime import datetime
import uuid
from typing import Dict, List

from lib.pg import PgConnect

class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
        
        
    # def user_product_counters_select(self, order_id):
    #     query = f"""
    #     select 
    #         lou.h_user_pk as user_id, 
    #         lop.h_product_pk as product_id,
    #         spn."name" as pruduct_name,
    #         count(lop.h_product_pk) over (partition by lop.h_product_pk, lou.h_user_pk) as order_cnt
    #     from dds.h_order ho 
    #     left join dds.l_order_product lop on ho.h_order_pk  = lop.h_order_pk
    #     left join dds.l_order_user lou on ho.h_order_pk  = lou.h_order_pk
    #     left join dds.s_product_names spn on lop.h_product_pk = spn.h_product_pk
    #     left join dds.s_user_names sun on lou.h_user_pk = sun.h_user_pk 
    #     left join dds.s_order_status sos on ho.h_order_pk = sos.h_order_pk 
    #     where sos.status = 'CLOSED' and ho.h_order_pk::text = '{order_id}'
    #     """
    #     result = ''
    #     with self._db.connection() as conn:
    #         with conn.cursor() as cur:
    #             cur.execute(query)
    #             result = cur.fetchall()
        
    #     return result
    
    def user_product_counters_insert(self,
                            user_id: uuid.UUID,
                            product_id: uuid.UUID,
                            product_name: datetime,
                            order_cnt: int
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cdm.user_product_counters AS upc (user_id, product_id, product_name, order_cnt) VALUES (
                        %(user_id)s, %(product_id)s, %(product_name)s, %(order_cnt)s
                    )
                    ON CONFLICT (user_id, product_id) DO UPDATE
                        SET order_cnt = upc.order_cnt + EXCLUDED.order_cnt;
                    """,
                    {
                        'user_id': user_id,
                        'product_id': product_id,
                        'product_name': product_name,
                        'order_cnt': order_cnt
                    }
                )
                
    def user_category_counters_insert(self,
                            user_id: uuid.UUID,
                            category_id: uuid.UUID,
                            category_name: datetime,
                            order_cnt: int
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cdm.user_category_counters AS upc (user_id, category_id, category_name, order_cnt) VALUES (
                        %(user_id)s, %(category_id)s, %(category_name)s, %(order_cnt)s
                    )
                    ON CONFLICT (user_id, category_id) DO UPDATE
                        SET order_cnt = upc.order_cnt + EXCLUDED.order_cnt;
                    """,
                    {
                        'user_id': user_id,
                        'category_id': category_id,
                        'category_name': category_name,
                        'order_cnt': order_cnt
                    }
                )