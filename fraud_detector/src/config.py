# file: fraud_detector/src/config.py
import yaml
import os

def load_config(config_path='config.yaml'):
    """
    Загружает конфигурационный файл YAML.
    
    Путь ищется относительно текущего рабочего каталога.
    В Docker контейнере это будет /app/config.yaml.
    """
    # В Docker контейнере CWD будет /app
    full_path = os.path.join(os.getcwd(), config_path)
    
    if not os.path.exists(full_path):
        # Попробуем подняться на уровень выше, если скрипт запускается из src
        full_path = os.path.join(os.path.dirname(__file__), '..', config_path)

    try:
        with open(full_path, 'r') as f:
            config = yaml.safe_load(f)
        print(f"Конфигурация успешно загружена из {full_path}")
        return config
    except FileNotFoundError:
        print(f"Ошибка: Конфигурационный файл не найден по пути {full_path}")
        # Можно завершить программу или вернуть дефолтные значения
        return None
    except Exception as e:
        print(f"Ошибка при загрузке конфигурации: {e}")
        return None

# Пример использования
if __name__ == '__main__':
    cfg = load_config()
    if cfg:
        print("Содержимое конфига:")
        print(cfg)