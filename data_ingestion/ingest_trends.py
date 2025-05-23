import logging
import os

def ingest_trends(config, log_path='logs/ingestion_trends.log'):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    try:
        target_dir = config['data_paths'].get('trends', 'data_storage/trends')
        if not os.path.exists(target_dir):
            logging.warning(f"Trends klasörü bulunamadı: {target_dir}")
            return
        for fname in os.listdir(target_dir):
            fpath = os.path.join(target_dir, fname)
            if os.path.isfile(fpath) and fname.endswith('.csv'):
                size = os.path.getsize(fpath)
                with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = sum(1 for _ in f)
                logging.info(f"Trends çıktı: {fpath} | Boyut: {size} bytes | Satır: {lines}")
    except Exception as e:
        logging.error(f"Trends ingestion hatası: {e}") 