

# Real-Time Fraud Detection System

## DISCLAIMER
Сервис подготовлен в демонстрационных целях в рамках занятий МТС ШАД 2025 по MLOps. Датасеты предоставлены в рамках соревнования [Teta ML-1 2025](https://www.kaggle.com/competitions/teta-ml-1-2025). Система для обнаружения мошеннических транзакций в реальном времени с использованием ML-модели и Kafka для потоковой обработки данных.

## 🏗️ Архитектура
Компоненты системы:
1. **`interface`** (Streamlit UI):
   - Создан для удобной симуляции потоковых данных с транзакциями.
   - Имитирует отправку транзакций в Kafka через CSV-файлы.
   - Генерирует уникальные ID для транзакций.
   - Загружает транзакции отдельными сообщениями формата JSON в топик Kafka `transactions`.
2. **`fraud_detector`** (ML Service):
   - Загружает предобученную модель CatBoost (`catboost_model.cbm`).
   - Выполняет препроцессинг данных:
     - Извлечение временных признаков.
     - Расчет гео-расстояний.
     - Загрузка списка категориальных переменных.
   - Производит скоринг с загруженным порогом.
   - Выгружает результат скоринга в топик Kafka `scoring`.
3. **`database_writer`** (Database Writer):
   - Читает сообщения из топика Kafka `scoring`.
   - Записывает результаты скоринга в PostgreSQL базу данных.
4. **Kafka Infrastructure**:
   - **Zookeeper + Kafka брокер**:
     - Автоматическое создание топиков `transactions` и `scoring`.
   - **Kafka UI**:
     - Веб-интерфейс для мониторинга сообщений (порт 8080).
5. **PostgreSQL Database**:
   - Хранит результаты скоринга для дальнейшего анализа и отчетности.

## 🚀 Быстрый старт
### Требования
- Docker 20.10+
- Docker Compose 2.0+

### Запуск
```bash
git clone https://github.com/your-repo/fraud-detection-system.git
cd fraud-detection-system
# Сборка и запуск всех сервисов
docker-compose up --build
```
После запуска:
- **Streamlit UI**: [http://localhost:8501](http://localhost:8501)
- **Kafka UI**: [http://localhost:8080](http://localhost:8080)
- **Логи сервисов**:
  ```bash
  docker-compose logs <service_name>  # Например: fraud_detector, kafka, interface
  ```

## 🛠️ Использование
### 1. Загрузка данных:
 - Загрузите CSV через интерфейс Streamlit. Для тестирования работы проекта используется файл формата `test.csv` из соревнования [Teta ML-1 2025](https://www.kaggle.com/competitions/teta-ml-1-2025).
 - Пример структуры данных:
    ```csv
    transaction_time,amount,lat,lon,merchant_lat,merchant_lon,gender,...
    2023-01-01 12:30:00,150.50,40.7128,-74.0060,40.7580,-73.9855,M,...
    ```
 - Для первых тестов рекомендуется загружать небольшой семпл данных (до 100 транзакций) за раз, чтобы исполнение кода не заняло много времени.

### 2. Мониторинг:
 - **Kafka UI**: Просматривайте сообщения в топиках `transactions` и `scoring`.
 - **Логи обработки**: `/app/logs/service.log` внутри контейнера `fraud_detector`.

### 3. Результаты:
 - Скоринговые оценки пишутся в топик `scoring` в формате:
    ```json
    {
      "score": 0.995, 
      "fraud_flag": 1, 
      "transaction_id": "d6b0f7a0-8e1a-4a3c-9b2d-5c8f9d1e2f3a"
    }
    ```

## Структура проекта
```
├── fraud_detector/
│   ├── app.py              # Основной скрипт для чтения Kafka и отправки в Kafka
│   ├── input/              # Директория для входных файлов (test.csv)
│   ├── model/              # Модель и артефакты
│   │   ├── catboost_model.cbm     # Сохраненная модель CatBoost
│   │   ├── threshold.json         # Порог классификации
│   │   └── categorical_features.json  # Список категориальных признаков
│   ├── src/                # Исходный код сервиса
│   │   ├── preprocess.py          # Препроцессинг данных
│   │   ├── scorer.py              # Инференс модели
│   │   └── config.py
│   ├── train_data/         # Тренировочный датасет
│   │   └── train.csv
│   ├── config.yaml           # Конфигурационный файл
│   ├── .dockerignore         # Файл для исключения ненужных файлов
│   ├── Dockerfile            # Файл для сборки Docker-образа
│   └── requirements.txt      # Зависимости Python
├── interface/
│   ├── .streamlit/
│   │   └── config.toml
│   ├── app.py              # Streamlit UI
│   ├── Dockerfile          # Обновленный Dockerfile для использования Python 3.9
│   └── requirements.txt    # Обновленные зависимости без rpds-py
├── database_writer/
│   ├── app.py              # Логика чтения Kafka и записи в БД
│   ├── init.sql
│   ├── Dockerfile          # Файл для сборки Docker-образа
│   └── requirements.txt    # Зависимости Python
├── docker-compose.yaml       # Обновленный файл для запуска всего окружения
└── README.md               # Описание проекта
```

## Настройки Kafka
```yaml
Топики:
- transactions (входные данные)
- scoring (результаты скоринга)
Репликация: 1 (для разработки)
Партиции: 3
```
*Примечание:* 
Для полной функциональности убедитесь, что:
1. Модель `catboost_model.cbm` размещена в `fraud_detector/model/`
2. Тренировочные данные находятся в `fraud_detector/train_data/`
3. Порты 8080, 8501 и 9092 свободны на хосте

## Описание файлов

### `fraud_detector/app.py`
Основной скрипт для чтения сообщений из Kafka, препроцессинга данных, выполнения скоринга и отправки результатов в Kafka.

### `fraud_detector/src/preprocess.py`
Модуль для предобработки данных перед подачей в модель.

### `fraud_detector/src/scorer.py`
Модуль для выполнения предсказаний с помощью модели CatBoost.

### `interface/app.py`
Streamlit UI для загрузки CSV файлов с транзакциями и отправки их в Kafka.

### `database_writer/app.py`
Сервис для чтения сообщений из Kafka `scoring` и записи результатов в PostgreSQL базу данных.

### `docker-compose.yaml`
Файл для запуска всех компонентов системы с использованием Docker Compose.

