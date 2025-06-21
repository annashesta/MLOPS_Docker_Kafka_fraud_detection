# file: fraud_detector/src/__init__.py

# Версия пакета
__version__ = "1.0.0"

# Экспорт ключевых функций для удобства импорта
from .preprocess import run_preproc
from .scorer import make_pred, initialize_scorer # <-- ИЗМЕНЕНИЕ ЗДЕСЬ

__all__ = [
    "run_preproc",
    "make_pred",
    "initialize_scorer" # <-- И ИЗМЕНЕНИЕ ЗДЕСЬ
]