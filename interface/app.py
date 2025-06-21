"""Streamlit UI"""

# file: interface/app.py
import streamlit as st
import pandas as pd
from kafka import KafkaProducer
import json
import uuid
import os
import time
import psycopg2
from psycopg2 import OperationalError
import matplotlib.pyplot as plt # Добавлено для построения гистограммы

# --- Функции для подключения ---

def get_db_connection():
    """Устанавливает соединение с PostgreSQL, используя переменные окружения."""
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=os.getenv('POSTGRES_PORT', 5432),
            dbname=os.getenv('POSTGRES_DB', 'fraud_db'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', 'example')
        )
        return conn
    except OperationalError as e:
        st.error(f"Ошибка подключения к базе данных: {e}")
        return None

def get_kafka_producer():
    """Создает Kafka Producer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BROKERS", "kafka:9093"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT"
        )
        return producer
    except Exception as e:
        st.error(f"Ошибка подключения к Kafka: {e}")
        return None

# --- Основной интерфейс ---

st.set_page_config(layout="wide")
st.title("Система Детектирования Фрода Транзакций")

# --- Колонка 1: Отправка данных ---
col1, col2 = st.columns(2)

with col1:
    st.header("📤 Отправка транзакций в Kafka")
    uploaded_file = st.file_uploader(
        "Загрузите CSV файл с транзакциями",
        type=["csv"]
    )
    
    if uploaded_file:
        st.success(f"Файл '{uploaded_file.name}' успешно загружен.")
        if st.button("Отправить транзакции на обработку"):
            producer = get_kafka_producer()
            if producer:
                try:
                    df = pd.read_csv(uploaded_file)
                    st.write("Пример данных из файла:", df.head(3))
                    
                    progress_bar = st.progress(0, text="Отправка...")
                    total_rows = len(df)
                    
                    for idx, row in df.iterrows():
                        transaction_id = str(uuid.uuid4())
                        message = {
                            "transaction_id": transaction_id,
                            "data": row.to_dict()
                        }
                        producer.send(os.getenv("KAFKA_TOPIC", "transactions"), value=message)
                        
                        # Обновляем прогресс-бар
                        percent_complete = (idx + 1) / total_rows
                        progress_bar.progress(percent_complete, text=f"Отправка... {idx + 1}/{total_rows}")
                        time.sleep(0.01) # Небольшая задержка для визуализации
                    
                    producer.flush()
                    progress_bar.progress(1.0, text="Отправка завершена!")
                    st.success(f"Все {total_rows} транзакций успешно отправлены.")

                except Exception as e:
                    st.error(f"Ошибка при обработке файла: {e}")

# --- Колонка 2: Просмотр результатов ---
with col2:
    st.header("📊 Результаты Скоринга")
    if st.button("Обновить и Показать Результаты"):
        conn = get_db_connection()
        if conn:
            try:
                # 1. Запрос последних 10 фродовых транзакций
                st.subheader("🚩 Последние 10 транзакций с флагом фрода")
                query_fraud = "SELECT transaction_id, score, fraud_flag, received_at FROM scores WHERE fraud_flag = TRUE ORDER BY received_at DESC LIMIT 10"
                df_fraud = pd.read_sql_query(query_fraud, conn)
                
                if not df_fraud.empty:
                    st.dataframe(df_fraud)
                else:
                    st.info("Мошеннические транзакции за последнее время не найдены.")

                # 2. Запрос для гистограммы
                st.subheader("📈 Распределение скоров (последние 100 транзакций)")
                query_scores = "SELECT score FROM (SELECT score, received_at FROM scores ORDER BY received_at DESC LIMIT 100) AS latest_scores"
                df_scores = pd.read_sql_query(query_scores, conn)

                if not df_scores.empty:
                    fig, ax = plt.subplots()
                    ax.hist(df_scores['score'], bins=20, edgecolor='black') # 20 бинов или другое число по желанию
                    ax.set_title('Распределение скоров')
                    ax.set_xlabel('Скор')
                    ax.set_ylabel('Количество транзакций')
                    st.pyplot(fig) # Отображаем график Matplotlib
                else:
                    st.info("Нет данных для построения гистограммы.")
            
            except Exception as e:
                st.error(f"Ошибка при выполнении запроса к базе данных: {e}")
            finally:
                if conn: # Убедимся, что соединение было установлено
                    conn.close()