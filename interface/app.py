"""
Итоговый Streamlit UI для системы детекции фрода.
Обеспечивает отправку транзакций в Kafka и отображение результатов скоринга
по нажатию кнопки из PostgreSQL.
"""

import streamlit as st
import pandas as pd
from kafka import KafkaProducer
import json
import uuid
import os
import time
import psycopg2
from psycopg2 import OperationalError
import matplotlib.pyplot as plt
import matplotlib.dates as mdates # Для форматирования даты на оси X Matplotlib
# import datetime # Больше не нужен для симуляции времени


# --- Основной интерфейс Streamlit ---
# st.set_page_config() ДОЛЖЕН БЫТЬ АБСОЛЮТНО ПЕРВОЙ Streamlit командой в скрипте!
st.set_page_config(layout="wide") # Используем широкое расположение для лучшей видимости графиков
st.title("Система Детектирования Фрода Транзакций")


# --- Глобальные переменные или загрузка конфигурации ---
# Порог теперь задается вручную. Файл threshold.json больше не загружается.
OPTIMAL_THRESHOLD = 0.32 # Порог теперь задается вручную

# Сообщение о заданном вручную пороге на боковой панели
st.sidebar.info(f"Порог фрода: {OPTIMAL_THRESHOLD:.4f}")

# Переменные состояния для отступа времени (больше не нужны для симуляции времени)
# Удалены, т.к. режим "реального времени" по секундам убран.


# --- Функции для подключения к сервисам ---

def get_db_connection():
    """
    Устанавливает соединение с PostgreSQL, используя переменные окружения.
    Возвращает объект соединения или None в случае ошибки.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=os.getenv('POSTGRES_PORT', 5432),
            dbname=os.getenv('POSTGRES_DB', 'fraud_db'), # Имя базы данных, как в docker-compose.yaml
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', 'example')
        )
        return conn
    except OperationalError as e:
        st.error(f"Ошибка подключения к базе данных: {e}")
        return None

def get_kafka_producer():
    """
    Создает Kafka Producer.
    Возвращает объект Producer или None в случае ошибки.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BROKERS", "kafka:9093"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"), # Сериализуем сообщения в JSON
            security_protocol="PLAINTEXT" # Используем простой протокол для локальной разработки
        )
        return producer
    except Exception as e:
        st.error(f"Ошибка подключения к Kafka: {e}")
        return None

def fetch_scores_from_db(conn):
    """
    Получает данные о скоринге из PostgreSQL:
    - Последние 10 фродовых транзакций.
    - Последние 1000 скоров для гистограммы.
    - Последние 1000 скоров со временем для линейного графика.
    Возвращает три DataFrame.
    """
    if not conn:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame() # Возвращаем пустые DataFrame при отсутствии соединения

    try:
        # 1. Запрос последних 10 фродовых транзакций (по убыванию времени получения)
        query_fraud = """
        SELECT transaction_id, score, fraud_flag, received_at
        FROM scores
        WHERE fraud_flag = TRUE
        ORDER BY received_at DESC
        LIMIT 10
        """
        df_fraud = pd.read_sql_query(query_fraud, conn)

        # 2. Запрос последних 1000 скоров со временем для графиков
        # Важно: выбираем 1000 последних записей (DESC)
        query_scores_and_time = """
        SELECT transaction_id, score, received_at
        FROM scores
        ORDER BY received_at DESC
        LIMIT 1000
        """
        df_scores_and_time = pd.read_sql_query(query_scores_and_time, conn)

        # Подготовка данных для гистограммы (только столбец 'score')
        df_scores_hist = df_scores_and_time[['score']]

        # Подготовка данных для линейного графика: убеждаемся, что 'received_at' - datetime
        df_scores_plot = df_scores_and_time.copy()
        df_scores_plot['received_at'] = pd.to_datetime(df_scores_plot['received_at'])
        
        # Важно: Сортируем данные по времени по возрастанию для правильного отображения хронологии на линейном графике
        df_scores_plot = df_scores_plot.sort_values(by='received_at', ascending=True)

        return df_fraud, df_scores_hist, df_scores_plot
    except Exception as e:
        st.error(f"Ошибка при выполнении запроса к базе данных: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# Создание вкладок для навигации
tab1, tab2 = st.tabs(["📤 Отправка транзакций", "📊 Результаты Скоринга"]) # Вкладка теперь без "Real-time"

# Вкладка "Отправка транзакций"
with tab1:
    st.header("Отправка тестовых транзакций в Kafka")
    uploaded_file = st.file_uploader(
        "Загрузите CSV файл с транзакциями (например, test_transactions.csv)",
        type=["csv"]
    )
    
    if uploaded_file:
        st.success(f"Файл '{uploaded_file.name}' успешно загружен.")
        
        # Кнопка для инициирования отправки
        if st.button("Отправить транзакции на обработку"):
            producer = get_kafka_producer()
            if producer:
                try:
                    df = pd.read_csv(uploaded_file)
                    st.write("Первые 3 строки из загруженного файла:", df.head(3))
                    
                    progress_text = "Отправка транзакций в Kafka..."
                    progress_bar = st.progress(0, text=progress_text)
                    total_rows = len(df)
                    
                    for idx, row in df.iterrows():
                        transaction_id = str(uuid.uuid4()) # Генерируем уникальный ID для каждой транзакции
                        message = {
                            "transaction_id": transaction_id,
                            "data": row.to_dict() # Преобразуем строку DataFrame в словарь для отправки в Kafka
                        }
                        producer.send(os.getenv("KAFKA_TOPIC", "transactions"), value=message)
                        
                        # Обновляем прогресс-бар
                        percent_complete = (idx + 1) / total_rows
                        progress_bar.progress(percent_complete, text=f"Отправка... {idx + 1}/{total_rows}")
                        time.sleep(0.005) # Небольшая задержка для визуализации прогресса
                    
                    producer.flush() # Убеждаемся, что все сообщения отправлены перед завершением
                    progress_bar.progress(1.0, text="Отправка завершена!")
                    st.success(f"Все {total_rows} транзакций успешно отправлены в Kafka.")

                except Exception as e:
                    st.error(f"Ошибка при обработке и отправке файла: {e}")
            else:
                st.warning("Продюсер Kafka не инициализирован. Проверьте подключение к Kafka.")

# Вкладка "Результаты Скоринга"
with tab2:
    st.header("📊 Результаты Скоринга")

    # Кнопка для обновления и отображения результатов
    if st.button("Посмотреть результаты"):
        conn = get_db_connection()
        if conn:
            try:
                # Отображение порога срабатывания
                st.markdown(f"**Порог срабатывания флага фрода (fraud_flag = 1, если score >= порога):** `{OPTIMAL_THRESHOLD:.4f}`")
                
                # Получаем данные из БД
                df_fraud, df_scores_hist, df_scores_plot = fetch_scores_from_db(conn)
                
                # --- Отображение последних фродовых транзакций ---
                st.subheader("🚩 Последние 10 транзакций с флагом фрода:")
                if not df_fraud.empty:
                    st.dataframe(df_fraud, use_container_width=True) # Отображаем DataFrame
                else:
                    st.info("Пока нет транзакций с флагом фрода.")

                # --- Гистограмма распределения скоров ---
                st.subheader("📈 Гистограмма распределения скоров (последние 1000 транзакций):")
                if not df_scores_hist.empty:
                    fig_hist, ax_hist = plt.subplots(figsize=(10, 4)) # Создаем фигуру Matplotlib
                    ax_hist.hist(df_scores_hist['score'], bins=20, edgecolor='black', alpha=0.7)
                    ax_hist.set_title('Распределение скоров транзакций')
                    ax_hist.set_xlabel('Скор')
                    ax_hist.set_ylabel('Количество транзакций')
                    ax_hist.set_xlim(0, 1) # Явно устанавливаем предел оси X от 0 до 1
                    ax_hist.axvline(OPTIMAL_THRESHOLD, color='r', linestyle='dashed', linewidth=2, label=f'Порог: {OPTIMAL_THRESHOLD:.4f}')
                    ax_hist.legend() # Отображаем легенду для порога
                    st.pyplot(fig_hist) # Отображаем фигуру в Streamlit
                    plt.close(fig_hist) # Важно закрыть фигуру, чтобы избежать утечек памяти
                else:
                    st.info("Пока нет данных для построения гистограммы.")

                # --- Линейный график скоров по времени ---
                st.subheader("📊 График скоров последних 1000 транзакций (по времени):")
                if not df_scores_plot.empty:
                    fig_line, ax_line = plt.subplots(figsize=(12, 5)) # Создаем фигуру Matplotlib

                    # Строим линейный график: ось X - время, ось Y - скор
                    ax_line.plot(df_scores_plot['received_at'], df_scores_plot['score'], 
                                 marker='o', linestyle='-', markersize=4, alpha=0.7)
                    ax_line.set_title('Скор транзакций по времени (последние 1000)')
                    ax_line.set_xlabel('Время получения транзакции') # Четкая подпись оси X
                    ax_line.set_ylabel('Скор') # Четкая подпись оси Y
                    ax_line.set_ylim(0, 1) # Устанавливаем предел оси Y от 0 до 1 для скоров

                    # Форматирование оси X (времени) для лучшей читаемости
                    ax_line.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S')) # Формат: Год-Месяц-День Часы:Минуты:Секунды
                    fig_line.autofmt_xdate() # Автоматический наклон меток для предотвращения наложений

                    # Добавляем горизонтальную линию порога на график скоров
                    ax_line.axhline(OPTIMAL_THRESHOLD, color='r', linestyle='dashed', linewidth=2, label=f'Порог: {OPTIMAL_THRESHOLD:.4f}')
                    ax_line.legend() # Отображаем легенду для порога

                    st.pyplot(fig_line) # Отображаем фигуру в Streamlit
                    plt.close(fig_line) # Закрываем фигуру
                else:
                    st.info("Пока нет данных для построения графика скоров.")
            
            except Exception as e:
                st.error(f"Ошибка при выполнении запроса к базе данных: {e}")
            finally:
                if conn:
                    conn.close()
