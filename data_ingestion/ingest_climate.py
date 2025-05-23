import logging
import os
from data_ingestion.fetch_climate_data import fetch_and_save_climate_data

def ingest_climate(config, log_path='logs/ingestion_climate.log'):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    try:
        target_dir = config['data_paths']['climate']
        mongo_host = config['mongodb']['host']
        fetch_and_save_climate_data(
            target_dir=target_dir,
            save_to_mongo=True,
            log_path=log_path,
            mongo_host=mongo_host
        )
        for fname in os.listdir(target_dir):
            fpath = os.path.join(target_dir, fname)
            if os.path.isfile(fpath):
                size = os.path.getsize(fpath)
                with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = sum(1 for _ in f)
                logging.info(f"İklim çıktı: {fpath} | Boyut: {size} bytes | Satır: {lines}")
    except Exception as e:
        logging.error(f"İklim ingestion hatası: {e}") 