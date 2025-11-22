# 1. Генерація потоку даних:
# Вхідні дані — це дані з Kafka-топіку, такі самі, як і в попередньому домашньому завданні. Згенеруйте потік даних,
# що містить id, temperature , humidity, timestamp . Можна використати раніше написаний вами скрипт та топік.

from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random
import os

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назва топіку
my_name = "oksana"
topic_name = f'{my_name}_building_sensors'
# Фіксований ID сенсора на один запуск
SENSOR_ID = os.getenv("SENSOR_ID") or str(random.randint(100000, 999999))


for i in range(10):
    # Відправлення повідомлення в топік
    try:
        data = {
            "id": SENSOR_ID,
            "temperature": random.randint(-100, 100),
            "humidity": random.randint(20, 100),
            "event_time": time.time(),  # Часова мітка
        }
        producer.send(topic_name, key=SENSOR_ID, value=data)
        producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
        print(f"Message {i} sent {SENSOR_ID} to topic {topic_name} data {data} successfully")
        time.sleep(3)
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()  # Закриття producer
