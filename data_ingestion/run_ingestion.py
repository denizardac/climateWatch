import yaml
import os
from datetime import datetime, timedelta
import glob
import logging

from data_ingestion.fetch_gdelt_news import fetch_gdelt_for_date
from data_ingestion.fetch_climate_data import fetch_and_save_climate_data
from data_ingestion.fetch_disaster_events import fetch_disaster_events
from data_ingestion.fetch_policy_changes import fetch_policy_changes
from data_ingestion.fetch_climate_summits import fetch_summits
from data_ingestion.ingest_gdelt import ingest_gdelt
from data_ingestion.ingest_climate import ingest_climate
from data_ingestion.ingest_disasters import ingest_disasters
from data_ingestion.ingest_policies import ingest_policies
from data_ingestion.ingest_summits import ingest_summits
from data_ingestion.ingest_open_datasets import ingest_open_datasets
from data_ingestion.ingest_kaggle import ingest_kaggle
from data_ingestion.ingest_pdf import ingest_pdf

def load_config(config_path='config.yaml'):
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def ensure_dirs(paths):
    for p in paths:
        os.makedirs(p, exist_ok=True)

def log_file_info(file_path, log_path):
    logger = logging.getLogger()
    if os.path.exists(file_path):
        size = os.path.getsize(file_path)
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = sum(1 for _ in f)
        except Exception:
            lines = 'N/A'
        logger.info(f"Çıktı dosyası: {file_path} | Boyut: {size} bytes | Satır: {lines}")
    else:
        logger.warning(f"Çıktı dosyası bulunamadı: {file_path}")

def run_all_ingestions(config):
    # Klasörleri oluştur
    ensure_dirs([
        config['data_paths']['gdelt'],
        config['data_paths']['climate'],
        config['data_paths']['disasters'],
        config['data_paths']['policies'],
        config['data_paths']['summits'],
        config['data_paths']['logs'],
    ])

    # Her ingestion adımını sırayla çağır
    try:
        ingest_gdelt(config)
    except Exception as e:
        print(f"GDELT ingestion hatası: {e}")
    try:
        ingest_climate(config)
    except Exception as e:
        print(f"İklim ingestion hatası: {e}")
    try:
        ingest_disasters(config)
    except Exception as e:
        print(f"Afet ingestion hatası: {e}")
    try:
        ingest_policies(config)
    except Exception as e:
        print(f"Politika ingestion hatası: {e}")
    try:
        ingest_summits(config)
    except Exception as e:
        print(f"Zirve ingestion hatası: {e}")
    try:
        ingest_open_datasets(config)
    except Exception as e:
        print(f"Open datasets ingestion hatası: {e}")
    try:
        ingest_kaggle(config)
    except Exception as e:
        print(f"Kaggle ingestion hatası: {e}")
    try:
        ingest_pdf(config)
    except Exception as e:
        print(f"PDF ingestion hatası: {e}")

if __name__ == '__main__':
    config = load_config()
    run_all_ingestions(config) 