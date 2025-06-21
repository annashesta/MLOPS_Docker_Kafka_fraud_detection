"""Основной скрипт для чтения Kafka и отправки в Kafka"""

# file: fraud_detector/app.py
import os
import json
import time
import logging
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer, errors

from src.preprocess import run_preproc
# vvv ИЗМЕНЕНИЕ ЗДЕСЬ vvv
from src.scorer import initialize_scorer, make_pred
from src.config import load_config

# Настройка логгирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_env_variable(var_name, default_value=None):
    """Получает переменную окружения или возвращает значение по умолчанию."""
    value = os.getenv(var_name, default_value)
    if value is None:
        logger.error(f"Переменная окружения '{var_name}' не установлена.")
        raise ValueError(f"Переменная окружения '{var_name}' должна быть установлена.")
    return value

def wait_for_kafka(bootstrap_servers, max_retries=30, retry_interval=5):
    """Ожидает доступности Kafka."""
    logger.info("Ожидание подключения к Kafka...")
    retries = 0
    while retries < max_retries:
        try:
            # Попытка создать временного продюсера для проверки соединения
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                request_timeout_ms=5000,
                security_protocol="PLAINTEXT"
            )
            producer.close()
            logger.info("Kafka готова к работе.")
            return
        except errors.NoBrokersAvailable as e:
            logger.warning(f"Kafka недоступна, попытка {retries + 1}/{max_retries}. Ошибка: {e}")
            retries += 1
            time.sleep(retry_interval)
    logger.error("Не удалось подключиться к Kafka после нескольких попыток.")
    raise SystemExit(1)

def main():
    """Основной пайплайн сервиса: чтение, обработка, скоринг, отправка."""
    try:
        # Получение конфигурации из переменных окружения
        kafka_bootstrap_servers = get_env_variable("KAFKA_BOOTSTRAP_SERVERS")
        transactions_topic = get_env_variable("KAFKA_TRANSACTIONS_TOPIC")
        scoring_topic = get_env_variable("KAFKA_SCORING_TOPIC")

        # Ожидание Kafka
        wait_for_kafka(kafka_bootstrap_servers)

        # Загрузка конфига и инициализация скорера
        config = load_config('config.yaml')
        initialize_scorer(config) 
        
        # Настройка Kafka Consumer и Producer
        consumer = KafkaConsumer(
            transactions_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            auto_offset_reset='earliest',
            group_id='fraud_detector_group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        logger.info(f"Начинаем слушать топик '{transactions_topic}'...")

        for message in consumer:
            try:
                msg_data = message.value
                transaction_id = msg_data.get("transaction_id")
                transaction_data = msg_data.get("data")

                if not transaction_id or not transaction_data:
                    logger.warning(f"Некорректный формат сообщения: {msg_data}")
                    continue

                logger.info(f"Получена транзакция {transaction_id}")
                
                df = pd.DataFrame([transaction_data])
                
                # Препроцессинг и скоринг
                processed_df = run_preproc(df)
                score, fraud_flag = make_pred(processed_df)
                
                # Формирование и отправка результата
                result = {
                    "transaction_id": transaction_id,
                    "score": score,
                    "fraud_flag": bool(fraud_flag) # Преобразуем в bool для JSON
                }
                
                producer.send(scoring_topic, value=result)
                logger.info(f"Результат для транзакции {transaction_id} отправлен в топик '{scoring_topic}'.")

            except Exception as e:
                logger.error(f"Ошибка обработки сообщения: {e}", exc_info=True)

    except Exception as e:
        logger.critical(f"Критическая ошибка при запуске сервиса: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
