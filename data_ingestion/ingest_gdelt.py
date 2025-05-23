import logging
import os
from datetime import datetime, timedelta
from data_ingestion.fetch_gdelt_news import fetch_gdelt_for_date

def ingest_gdelt(config, log_path='logs/ingestion_gdelt.log'):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    start_date = datetime.strptime(config['date_range']['start_date'], '%Y-%m-%d')
    end_date = datetime.strptime(config['date_range']['end_date'], '%Y-%m-%d')
    keywords = config['keywords']
    mongo_host = config['mongodb']['host']
    target_dir = config['data_paths']['gdelt']
    current = start_date
    while current <= end_date:
        try:
            fetch_gdelt_for_date(
                current,
                target_dir=target_dir,
                save_to_mongo=True,
                keywords=keywords,
                log_path=log_path,
                mongo_host=mongo_host
            )
            out_file = os.path.join(target_dir, f"{current.strftime('%Y%m%d')}.csv")
            if os.path.exists(out_file):
                size = os.path.getsize(out_file)
                with open(out_file, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = sum(1 for _ in f)
                logging.info(f"GDELT çıktı: {out_file} | Boyut: {size} bytes | Satır: {lines}")
            else:
                logging.warning(f"GDELT çıktı dosyası bulunamadı: {out_file}")
        except Exception as e:
            logging.error(f"GDELT ingestion hatası ({current}): {e}")
        current += timedelta(days=1) 