import logging
import os
from data_ingestion.fetch_kaggle_datasets import download_kaggle_datasets

def ingest_kaggle(config, log_path='logs/ingestion_kaggle.log'):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    try:
        target_dir = config['data_paths'].get('kaggle', 'data_storage/kaggle')
        dataset_name = config.get('kaggle_dataset', 'climate')
        max_datasets = config.get('kaggle_max_datasets', 3)
        download_kaggle_datasets(dataset_name, target_dir, max_datasets=max_datasets, log_path=log_path)
        if not os.path.exists(target_dir):
            logging.warning(f"Kaggle klasörü bulunamadı: {target_dir}")
            return
        for fname in os.listdir(target_dir):
            fpath = os.path.join(target_dir, fname)
            if os.path.isfile(fpath):
                size = os.path.getsize(fpath)
                with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = sum(1 for _ in f)
                logging.info(f"Kaggle çıktı: {fpath} | Boyut: {size} bytes | Satır: {lines}")
    except Exception as e:
        logging.error(f"Kaggle ingestion hatası: {e}") 