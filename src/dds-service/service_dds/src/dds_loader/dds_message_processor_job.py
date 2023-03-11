import time, uuid
from typing import Any
from datetime import datetime
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from dds_loader.repository.dds_repository import DdsRepository
from logging import Logger


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: 100,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = batch_size
        self._logger = logger
        
        self._source_system = "orders-system-kafka"
        self._uuid_namespace = uuid.UUID("c52f8247-12e3-4955-93c4-4d656aff7735")

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            
            if not msg:
                self._logger.info(f"{datetime.utcnow()}: There is no message")   
                break
            
            self._logger.info(f"{datetime.utcnow()}: Message received")   
            
            payload = msg.get('payload')
            # Пишем в таблицу dds.h_user
            self.fill_h_user(payload)
            # Пишем в таблицу dds.h_product
            self.fill_h_product(payload)
            # Пишем в таблицу dds.h_category
            self.fill_h_category(payload)
            # Пишем в таблицу dds.h_restaurant
            self.fill_h_restaurant(payload)
            # Пишем в таблицу dds.h_order
            self.fill_h_order(payload)
            # Пишем в таблицу dds.l_order_product
            self.fill_l_order_product(payload)
            # Пишем в таблицу dds.l_product_restaurant(
            self.fill_l_product_restaurant(payload)
            # Пишем в таблицу dds.l_product_category
            self.fill_l_product_category(payload)
            # Пишем в таблицу dds.l_order_user
            self.fill_l_order_user(payload)
            # Пишем в таблицу dds.s_user_names
            self.fill_s_user_names(payload)
            # Пишем в таблицу dds.s_product_names
            self.fill_s_product_names(payload)
            # Пишем в таблицу dds.s_restaurant_names
            self.fill_s_restaurant_names(payload)
            # Пишем в таблицу dds.s_order_cost
            self.fill_s_order_cost(payload)
            # Пишем в таблицу dds.s_order_status
            self.fill_s_order_status(payload)
            
            # Отправляем сообщение в dds топик
            self._logger.info('Message for send', self.prepair_cdm_message(msg))
            self._producer.produce(self.prepair_cdm_message(msg))
            self._logger.info(f"{datetime.utcnow()}: Message sent")
            

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")


    def fill_h_user(self, payload) -> None:
        user = payload.get('user')
        insert_data = {
           "h_user_pk": self._uuid(user.get('id')),
           "user_id": user.get('id'),
           "load_dt": str(datetime.utcnow()),
           "load_src": self._source_system
        }
        
        self._dds_repository.hub_table_insert(
            table_name="dds.h_user",
            data=insert_data,
            uniq_fields=["user_id"]
        )
    
    def fill_h_product(self, payload) -> None:
        products = payload.get('products')
        
        for product in products:            
            insert_data = {
                "h_product_pk": self._uuid(product.get('id')),
                "product_id": product.get('id'),
                "load_dt": str(datetime.utcnow()),
                "load_src": self._source_system
            }
        
            self._dds_repository.hub_table_insert(
                table_name="dds.h_product",
                data=insert_data,
                uniq_fields=["product_id"]
            )
            
    def fill_h_category(self, payload) -> None:
        products = payload.get('products')
        
        for product in products:
            insert_data = {
                "h_category_pk": self._uuid(product.get('category')),
                "category_name": product.get('category'),
                "load_dt": str(datetime.utcnow()),
                "load_src": self._source_system
            }
        
            self._dds_repository.hub_table_insert(
                table_name="dds.h_category",
                data=insert_data,
                uniq_fields=["category_name"]
            )
            
    def fill_h_restaurant(self, payload) -> None:
        restaurant = payload.get('restaurant')
        insert_data = {
           "h_restaurant_pk": self._uuid(restaurant.get('id')),
           "restaurant_id": restaurant.get('id'),
           "load_dt": str(datetime.utcnow()),
           "load_src": self._source_system
        }
        
        self._dds_repository.hub_table_insert(
            table_name="dds.h_restaurant",
            data=insert_data,
            uniq_fields=["restaurant_id"]
        )
        
    def fill_h_order(self, payload) -> None:
        insert_data = {
           "h_order_pk": self._uuid(payload.get('id')),
           "order_id": payload.get('id'),
           "order_dt": payload.get('date'),
           "load_dt": str(datetime.utcnow()),
           "load_src": self._source_system
        }
        
        self._dds_repository.hub_table_insert(
            table_name="dds.h_order",
            data=insert_data,
            uniq_fields=["order_id"]
        )
        
    def fill_l_order_product(self, payload) -> None:
        products = payload.get('products')
        
        for product in products:
            insert_data = {
                "hk_order_product_pk": self._uuid([payload.get('id'), product.get('id')]),
                "h_order_pk": self._uuid(payload.get('id')),
                "h_product_pk": self._uuid(product.get('id')),
                "load_dt": str(datetime.utcnow()),
                "load_src": self._source_system
            }
            
            self._dds_repository.hub_table_insert(
                table_name="dds.l_order_product",
                data=insert_data,
                uniq_fields=["hk_order_product_pk"]
            )
            
            
    def fill_l_product_restaurant(self, payload) -> None:
        products = payload.get('products')
        restaurant = payload.get('restaurant')
        
        for product in products:
            insert_data = {
                "hk_product_restaurant_pk": self._uuid([product.get('id'), restaurant.get('id')]),
                "h_restaurant_pk": self._uuid(restaurant.get('id')),
                "h_product_pk": self._uuid(product.get('id')),
                "load_dt": str(datetime.utcnow()),
                "load_src": self._source_system
            }
            
            self._dds_repository.hub_table_insert(
                table_name="dds.l_product_restaurant",
                data=insert_data,
                uniq_fields=["hk_product_restaurant_pk"]
            )
            
    def fill_l_product_category(self, payload) -> None:
        products = payload.get('products')
        
        for product in products:
            insert_data = {
                "hk_product_category_pk": self._uuid([product.get('id'), product.get('category')]),
                "h_product_pk": self._uuid(product.get('id')),
                "h_category_pk": self._uuid(product.get('category')),
                "load_dt": str(datetime.utcnow()),
                "load_src": self._source_system
            }
            
            self._dds_repository.hub_table_insert(
                table_name="dds.l_product_category",
                data=insert_data,
                uniq_fields=["hk_product_category_pk"]
            )
            
    def fill_l_order_user(self, payload) -> None:
        user = payload.get('user')
        
        insert_data = {
           "hk_order_user_pk": self._uuid([payload.get('id'), user.get('id')]),
           "h_order_pk": self._uuid(payload.get('id')),
           "h_user_pk": self._uuid(user.get('id')),
           "load_dt": str(datetime.utcnow()),
           "load_src": self._source_system
        }
        
        self._dds_repository.hub_table_insert(
            table_name="dds.l_order_user",
            data=insert_data,
            uniq_fields=["hk_order_user_pk"]
        )
        
    def fill_s_user_names(self, payload) -> None:
        user = payload.get('user')
        
        insert_data = {
           "hk_user_names_pk": self._uuid([user.get('id'), user.get('id')]),
           "h_user_pk": self._uuid(user.get('id')),
           "username": user.get('name'),
           "userlogin": user.get('login', ''),
           "load_dt": str(datetime.utcnow()),
           "load_src": self._source_system
        }
        
        self._dds_repository.hub_table_insert(
            table_name="dds.s_user_names",
            data=insert_data,
            uniq_fields=["hk_user_names_pk"]
        )
    
    def fill_s_product_names(self, payload) -> None:
        products = payload.get('products')
        
        for product in products:
            insert_data = {
            "hk_product_names_pk": self._uuid([product.get('id'), product.get('name')]),
            "h_product_pk": self._uuid(product.get('id')),
            "name": product.get('name'),
            "load_dt": str(datetime.utcnow()),
            "load_src": self._source_system
            }
            
            self._dds_repository.hub_table_insert(
                table_name="dds.s_product_names",
                data=insert_data,
                uniq_fields=["hk_product_names_pk"]
            )
    
    def fill_s_restaurant_names(self, payload) -> None:
        restaurant = payload.get('restaurant')
        
        insert_data = {
           "hk_restaurant_names_pk": self._uuid([restaurant.get('id'), restaurant.get('name')]),
           "h_restaurant_pk": self._uuid(restaurant.get('id')),
           "name": restaurant.get('name'),
           "load_dt": str(datetime.utcnow()),
           "load_src": self._source_system
        }
        
        self._dds_repository.hub_table_insert(
            table_name="dds.s_restaurant_names",
            data=insert_data,
            uniq_fields=["hk_restaurant_names_pk"]
        )
        
    def fill_s_order_cost(self, payload) -> None:        
        insert_data = {
           "hk_order_cost_pk": self._uuid([payload.get('id'), payload.get('cost'), payload.get('payment')]),
           "h_order_pk": self._uuid(payload.get('id')),
           "cost": payload.get('cost'),
           "payment": payload.get('payment'),
           "load_dt": str(datetime.utcnow()),
           "load_src": self._source_system
        }
        
        self._dds_repository.hub_table_insert(
            table_name="dds.s_order_cost",
            data=insert_data,
            uniq_fields=["hk_order_cost_pk"]
        )
        
    def fill_s_order_status(self, payload) -> None:        
        insert_data = {
           "hk_order_status_pk": self._uuid([payload.get('id'), payload.get('status')]),
           "h_order_pk": self._uuid(payload.get('id')),
           "status": payload.get('status'),
           "load_dt": str(datetime.utcnow()),
           "load_src": self._source_system
        }
        
        self._dds_repository.hub_table_insert(
            table_name="dds.s_order_status",
            data=insert_data,
            uniq_fields=["hk_order_status_pk"]
        )
        
    def prepair_cdm_message(self, msg):
        payload = msg.get('payload')
        user = payload.get('user')
        products = payload.get('products')
        
        products_data = []
        
        for product in products:
            products_data.append({
                "product_id": str(self._uuid(product.get('id'))),
                "product_name": product.get('name'),
                "product_name": product.get('name'),
                "category_id": str(self._uuid(product.get('category'))),
                "category_name": product.get('category'),
            })
            
        return {
            "object_id": msg.get('object_id'),
            "object_type": msg.get('object_type'),
            "payload": {
                "order_id": str(self._uuid(payload.get('id'))),
                "user_id": str(self._uuid(user.get('id'))),
                "products_data": products_data
            }
        }
    
    def _uuid(self, obj: Any) -> uuid.UUID:
       return uuid.uuid5(namespace=self._uuid_namespace, name=str(obj)) 