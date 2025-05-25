"""
fetch_climate_summits.py
Uluslararası iklim zirveleri ve önemli çevre etkinlikleri verilerini çekmek için iskelet script.
Gerçek API veya veri kaynağı entegrasyonu için endpoint ve anahtarlar eklenmelidir.
"""

import argparse
import pandas as pd
import os
import logging
from pymongo import MongoClient

def setup_logger(log_path):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def fetch_summits(target_dir, csv_path=None, log_path="logs/data_ingestion.log"):
    setup_logger(log_path)
    os.makedirs(target_dir, exist_ok=True)
    if csv_path and os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        out_path = os.path.join(target_dir, 'climate_summits.csv')
        df.to_csv(out_path, index=False)
        print(f"Zirve verisi kaydedildi: {out_path}")
        logging.info(f"Zirve verisi kaydedildi: {out_path}")

        # Kafka'ya veri gönder
        try:
            from utils.kafka_utils import ClimateDataProducer
            producer = ClimateDataProducer(bootstrap_servers=['localhost:9092'], topic='summit-data')
            for idx, record in enumerate(df.to_dict("records")):
                producer.send_climate_data(key=str(idx), data=record)
            producer.close()
            print("Zirve verisi Kafka'ya gönderildi.")
            logging.info("Zirve verisi Kafka'ya gönderildi.")
        except Exception as e:
            print(f"Kafka'ya veri gönderilemedi: {e}")
            logging.error(f"Kafka'ya veri gönderilemedi: {e}")

        # MongoDB'ye veri kaydet
        try:
            client = MongoClient("mongodb://localhost:27017/")
            db = client['climatewatch']
            collection = db['climate_summits']
            collection.delete_many({})
            records = df.to_dict("records")
            if records:
                collection.insert_many(records)
                print("Zirve verisi MongoDB'ye kaydedildi.")
                logging.info("Zirve verisi MongoDB'ye kaydedildi.")
        except Exception as e:
            print(f"MongoDB'ye veri kaydedilemedi: {e}")
            logging.error(f"MongoDB'ye veri kaydedilemedi: {e}")
    else:
        print("Lütfen UNFCCC COP zirveleri CSV dosyasını indirip --csv_path ile belirtin.")
        logging.error("UNFCCC COP zirveleri CSV dosyası eksik veya yolu yanlış.")

def main():
    parser = argparse.ArgumentParser(description='İklim zirvesi verisi çekme scripti (UNFCCC CSV)')
    parser.add_argument('--target_dir', type=str, required=True, help='Verinin kaydedileceği klasör')
    parser.add_argument('--csv_path', type=str, required=False, help='Elle indirilen COP CSV dosyası')
    parser.add_argument('--log_path', type=str, default='logs/data_ingestion.log', help='Log dosyası yolu')
    args = parser.parse_args()
    fetch_summits(args.target_dir, args.csv_path, args.log_path)

if __name__ == '__main__':
    main() 