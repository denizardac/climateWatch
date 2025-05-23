import logging
import os
from data_ingestion.fetch_open_datasets import download_files

def ingest_open_datasets(config, log_path='logs/ingestion_open_datasets.log'):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    try:
        target_dir = config['data_paths'].get('open_datasets', 'data_storage/open_datasets')
        url_list = config.get('open_datasets_urls', [])
        if url_list:
            download_files(url_list, target_dir, log_path=log_path)
        if not os.path.exists(target_dir):
            logging.warning(f"Open datasets klasörü bulunamadı: {target_dir}")
            return
        for fname in os.listdir(target_dir):
            fpath = os.path.join(target_dir, fname)
            if os.path.isfile(fpath):
                size = os.path.getsize(fpath)
                with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = sum(1 for _ in f)
                logging.info(f"Open datasets çıktı: {fpath} | Boyut: {size} bytes | Satır: {lines}")
    except Exception as e:
        logging.error(f"Open datasets ingestion hatası: {e}") 