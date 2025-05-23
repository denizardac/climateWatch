import logging
import os
from data_ingestion.fetch_disaster_events import fetch_disaster_events

def ingest_disasters(config, log_path='logs/ingestion_disasters.log'):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    try:
        target_dir = config['data_paths']['disasters']
        fetch_disaster_events(
            api_url='https://api.reliefweb.int/v1/disasters',
            start_date=config['date_range']['start_date'].replace('-', ''),
            end_date=config['date_range']['end_date'].replace('-', ''),
            target_dir=target_dir,
            log_path=log_path
        )
        for fname in os.listdir(target_dir):
            fpath = os.path.join(target_dir, fname)
            if os.path.isfile(fpath):
                size = os.path.getsize(fpath)
                with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = sum(1 for _ in f)
                logging.info(f"Afet çıktı: {fpath} | Boyut: {size} bytes | Satır: {lines}")
    except Exception as e:
        logging.error(f"Afet ingestion hatası: {e}") 