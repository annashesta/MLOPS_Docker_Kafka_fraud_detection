"""Логика чтения Kafka и записи в БД"""

# file: database_writer/app.py
import os
import json
import time
import logging
from kafka import KafkaConsumer
import psycopg2
from psycopg2 import OperationalError

# Настройка логгирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_env_variable(var_name, default_value=None):
    """Получает переменную окружения или возвращает значение по умолчанию."""
    value = os.getenv(var_name, default_value)
    if value is None:
        logger.error(f"Переменная окружения '{var_name}' не установлена.")
        raise ValueError(f"Переменная окружения '{var_name}' должна быть установлена.")
    return value

def wait_for_db(max_retries=30, retry_interval=5):
    """Ожидает доступности базы данных PostgreSQL."""
    logger.info("Ожидание подключения к PostgreSQL...")
    retries = 0
    while retries < max_retries:
        try:
            conn = psycopg2.connect(
                host=get_env_variable("POSTGRES_HOST"),
                port=get_env_variable("POSTGRES_PORT"),
                dbname=get_env_variable("POSTGRES_DB"),
                user=get_env_variable("POSTGRES_USER"),
                password=get_env_variable("POSTGRES_PASSWORD"),
                connect_timeout=3
            )
            conn.close()
            logger.info("PostgreSQL готов к работе.")
            return
        except OperationalError as e:
            logger.warning(f"PostgreSQL недоступен, попытка {retries + 1}/{max_retries}. Ошибка: {e}")
            retries += 1
            time.sleep(retry_interval)
    logger.error("Не удалось подключиться к PostgreSQL после нескольких попыток.")
    raise SystemExit(1)

def main():
    """Основная функция для чтения из Kafka и записи в PostgreSQL."""
    # Получение конфигурации из переменных окружения
    kafka_bootstrap_servers = get_env_variable("KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic = get_env_variable("KAFKA_TOPIC")
    
    # Ожидание готовности БД
    wait_for_db()
    
    # Подключение к PostgreSQL
    conn = psycopg2.connect(
        host=get_env_variable("POSTGRES_HOST"),
        port=get_env_variable("POSTGRES_PORT"),
        dbname=get_env_variable("POSTGRES_DB"),
        user=get_env_variable("POSTGRES_USER"),
        password=get_env_variable("POSTGRES_PASSWORD")
    )
    
    # Настройка Kafka Consumer
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        auto_offset_reset='earliest',
        group_id='database_writer_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    logger.info(f"Начинаем слушать топик '{kafka_topic}'...")

    for message in consumer:
        data = message.value
        logger.info(f"Получено сообщение: {data}")
        
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO scores (transaction_id, score, fraud_flag) VALUES (%s, %s, %s) ON CONFLICT (transaction_id) DO NOTHING",
                    (data['transaction_id'], data['score'], data['fraud_flag'])
                )
                conn.commit()
            logger.info(f"Транзакция {data['transaction_id']} успешно записана в базу.")
        except Exception as e:
            logger.error(f"Ошибка записи в базу данных: {e}")
            # В реальном проекте можно добавить логику отката или отправки в "dead letter queue"
            conn.rollback()

if __name__ == "__main__":
    main()