import time, uuid
from datetime import datetime
from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository
from logging import Logger


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: 100,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size
        self._logger = logger

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        # Имитация работы. Здесь будет реализована обработка сообщений.
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            
            if not msg:
                self._logger.info(f"{datetime.utcnow()}: There is no message")
                break
            
            payload = msg.get('payload')
            order_id = payload.get('order_id')
            
            self.insert_data_to_users_product_counter(payload=payload)
            self.insert_data_to_users_category_counter(payload=payload)
            
            # user_product_counters_result = self._cdm_repository.user_product_counters_select(order_id=order_id)
            
            # if user_product_counters_result:
            #     for item in user_product_counters_result:
            #         self._cdm_repository.user_product_counters_insert(
            #             user_id=item[0],
            #             product_id=item[1],
            #             product_name=item[2],
            #             order_cnt=item[3],
            #         )            

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def insert_data_to_users_product_counter(self, payload):
        products = payload.get('products_data')
        user_id = payload.get('user_id')
        
        if products:
            for product in products:
                self._cdm_repository.user_product_counters_insert(
                    user_id=uuid.UUID(user_id),
                    product_id=uuid.UUID(product.get('product_id')),
                    product_name=product.get('product_name'),
                    order_cnt=product.get('product_iquantity', 1)
                )
                
    def insert_data_to_users_category_counter(self, payload):
        products = payload.get('products_data')
        user_id = payload.get('user_id')
        
        if products:
            for product in products:
                self._cdm_repository.user_category_counters_insert(
                    user_id=uuid.UUID(user_id),
                    category_id=uuid.UUID(product.get('category_id')),
                    category_name=product.get('category_name'),
                    order_cnt=1
                )