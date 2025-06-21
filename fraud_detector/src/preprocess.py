"""Модуль для предобработки данных перед подачей в модель."""

# file: fraud_detector/src/preprocess.py
import logging
import pandas as pd
import numpy as np
from geopy.distance import great_circle

# Настройка логгера
logger = logging.getLogger(__name__)

# Определяем колонки как константы, т.к. они зависят от обученной модели
CATEGORICAL_COLS = ["gender", "merch", "cat_id", "one_city", "us_state", "jobs"]
CONTINUOUS_COLS = ["amount", "population_city"]
COORDS_COLS = ["lat", "lon", "merchant_lat", "merchant_lon"]
DATETIME_COL = "transaction_time"

def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет временные признаки из столбца 'transaction_time'."""
    df[DATETIME_COL] = pd.to_datetime(df[DATETIME_COL])
    dt = df[DATETIME_COL].dt
    df["hour"] = dt.hour
    df["year"] = dt.year
    df["month"] = dt.month
    df["day_of_month"] = dt.day
    df["day_of_week"] = dt.dayofweek
    return df.drop(columns=DATETIME_COL)

def add_distance_features(df: pd.DataFrame) -> pd.DataFrame:
    """Рассчитывает расстояние между клиентом и продавцом в километрах."""
    def calc_distance(row):
        try:
            return great_circle(
                (row["lat"], row["lon"]), (row["merchant_lat"], row["merchant_lon"])
            ).km
        except (ValueError, TypeError):
            return np.nan # Возвращаем NaN, если координаты некорректны

    df["distance"] = df.apply(calc_distance, axis=1)
    return df.drop(columns=COORDS_COLS, errors="ignore")

def run_preproc(input_df: pd.DataFrame) -> pd.DataFrame:
    """Выполняет полный пайплайн препроцессинга данных для инференса."""
    logger.info("Начало препроцессинга данных...")
    df = input_df.copy()

    # Проверка наличия колонок
    required_cols = [DATETIME_COL] + COORDS_COLS + CATEGORICAL_COLS + CONTINUOUS_COLS
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"В данных отсутствуют необходимые колонки: {missing_cols}")

    df = add_time_features(df)
    logger.info("Временные признаки добавлены.")

    df = add_distance_features(df)
    logger.info("Расстояния рассчитаны.")

    # Обработка пропусков: заполняем средним для числовых
    # В реальном проекте лучше использовать импьютеры, обученные на трейне
    df['distance'] = df['distance'].fillna(df['distance'].mean())
    df['population_city'] = df['population_city'].fillna(df['population_city'].mean())
    
    # Логарифмическое преобразование
    for col in CONTINUOUS_COLS + ["distance"]:
        df[f"{col}_log"] = np.log(df[col] + 1)
    logger.info("Логарифмическое преобразование выполнено.")

    logger.info("Препроцессинг завершен.")
    return df