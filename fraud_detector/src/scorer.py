"""Модуль для выполнения предсказаний с помощью CatBoost модели."""

# file: fraud_detector/src/scorer.py
import json
import logging
from typing import Dict, List, Tuple
import pandas as pd
from catboost import CatBoostClassifier

logger = logging.getLogger(__name__)

MODEL: CatBoostClassifier = None
OPTIMAL_THRESHOLD: float = 0.5
CATEGORICAL_FEATURES: List[str] = []

def initialize_scorer(config: Dict):
    """Инициализирует модель, порог и признаки на основе конфига."""
    global MODEL, OPTIMAL_THRESHOLD, CATEGORICAL_FEATURES
    
    model_path = config["paths"]["model_path"]
    threshold_path = config["paths"]["threshold_path"]
    features_path = config["paths"]["categorical_features_path"]

    logger.info("Загрузка артефактов модели...")
    try:
        with open(features_path, "r", encoding="utf-8") as f:
            CATEGORICAL_FEATURES = json.load(f).get("categorical_features", [])
        
        MODEL = CatBoostClassifier(cat_features=CATEGORICAL_FEATURES)
        MODEL.load_model(model_path)

        with open(threshold_path, "r", encoding="utf-8") as f:
            OPTIMAL_THRESHOLD = json.load(f).get("threshold", 0.5)

        logger.info("Артефакты модели успешно загружены.")
        logger.info(f"Модель: {type(MODEL)}, Порог: {OPTIMAL_THRESHOLD:.4f}")
        logger.info(f"Категориальные признаки: {CATEGORICAL_FEATURES}")

    except Exception as e:
        logger.error(f"Ошибка при инициализации скорера: {e}")
        raise

def make_pred(data: pd.DataFrame) -> Tuple[float, int]:
    """
    Выполняет предсказание для одной строки данных.
    
    Args:
        data: DataFrame, содержащий одну строку с предобработанными данными.
        
    Returns:
        Кортеж (вероятность фрода, флаг фрода).
    """
    if MODEL is None or OPTIMAL_THRESHOLD is None:
        raise RuntimeError("Скорер не инициализирован. Вызовите initialize_scorer() перед предсказанием.")
    
    # Убедимся, что все нужные колонки есть
    required_features = MODEL.feature_names_
    missing_features = [f for f in required_features if f not in data.columns]
    if missing_features:
        raise ValueError(f"Отсутствуют признаки для предсказания: {missing_features}")

    # Предсказание вероятности
    proba = MODEL.predict_proba(data[required_features])[:, 1][0]
    
    # Применение порога для получения флага
    fraud_flag = 1 if proba >= OPTIMAL_THRESHOLD else 0
    
    logger.info(f"Предсказание выполнено. Вероятность: {proba:.4f}, Флаг: {fraud_flag}")
    return proba, fraud_flag