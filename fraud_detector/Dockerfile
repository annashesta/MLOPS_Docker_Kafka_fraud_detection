# Базовый образ
FROM python:3.10-slim

# Создание и настройка рабочей директории
WORKDIR /app

# Создание директорий
RUN mkdir -p /app/input /app/output /app/model /app/src /app/logs /app/train_data && \
    useradd -m appuser && \
    chown -R appuser:appuser /app

# Установка зависимостей (копируем отдельно для лучшего кэширования)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    rm -rf /root/.cache /var/lib/apt/lists/*

# Копирование остальных файлов
COPY model/catboost_model.cbm /app/model/
COPY model/threshold.json /app/model/
COPY model/categorical_features.json /app/model/
COPY config.yaml /app/config.yaml
COPY src/ /app/src/
COPY app.py /app/app.py

# Настройка прав доступа
RUN chmod -R 755 /app/logs

# Переключаемся на непривилегированного пользователя
USER appuser

# Команда для запуска сервиса с использованием JSON-синтаксиса
CMD ["python", "app.py"]