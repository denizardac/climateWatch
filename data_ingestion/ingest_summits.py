import logging
import os
from data_ingestion.fetch_climate_summits import fetch_summits

def ingest_summits(config, log_path='logs/ingestion_summits.log'):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    try:
        target_dir = config['data_paths']['summits']
        csv_path = config.get('summits_csv_path', None)
        if not csv_path or not os.path.exists(csv_path):
            logging.warning('Zirve CSV dosyası eksik veya manuel indirme gerektiriyor!')
        fetch_summits(
            target_dir=target_dir,
            csv_path=csv_path,
            log_path=log_path
        )
        for fname in os.listdir(target_dir):
            fpath = os.path.join(target_dir, fname)
            if os.path.isfile(fpath):
                size = os.path.getsize(fpath)
                with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = sum(1 for _ in f)
                logging.info(f"Zirve çıktı: {fpath} | Boyut: {size} bytes | Satır: {lines}")
    except Exception as e:
        logging.error(f"Zirve ingestion hatası: {e}") 