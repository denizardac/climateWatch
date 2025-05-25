"""
fetch_policy_changes.py
Ülkelerin iklim politikası değişiklikleri, anlaşmalar ve yasalar için veri çekme scripti (iskele).
Gerçek API veya veri kaynağı entegrasyonu için endpoint ve anahtarlar eklenmelidir.
"""

import argparse
import requests
import os
import logging
import pandas as pd
from utils.kafka_utils import ClimateDataProducer
from pymongo import MongoClient

def setup_logger(log_path):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def fetch_policy_changes(target_dir, log_path="logs/data_ingestion.log"):
    setup_logger(log_path)
    os.makedirs(target_dir, exist_ok=True)
    url = 'https://zenodo.org/records/10842614/files/POLICY_DATABASE_1.0.csv?download=1'
    response = requests.get(url)
    if response.status_code == 200:
        out_path = os.path.join(target_dir, 'climate_policy_changes.csv')
        with open(out_path, 'wb') as f:
            f.write(response.content)
        print(f"Politika değişikliği verisi kaydedildi: {out_path}")
        logging.info(f"Politika değişikliği verisi kaydedildi: {out_path}")

        # Kafka'ya veri gönder
        try:
            df = pd.read_csv(out_path)
            producer = ClimateDataProducer(bootstrap_servers=['localhost:9092'], topic='policy-data')
            for idx, record in enumerate(df.to_dict("records")):
                producer.send_climate_data(key=str(idx), data=record)
            producer.close()
            print("Politika verisi Kafka'ya gönderildi.")
            logging.info("Politika verisi Kafka'ya gönderildi.")
        except Exception as e:
            print(f"Kafka'ya veri gönderilemedi: {e}")
            logging.error(f"Kafka'ya veri gönderilemedi: {e}")

        # MongoDB'ye veri kaydet
        try:
            client = MongoClient("mongodb://localhost:27017/")
            db = client['climatewatch']
            collection = db['policy_changes']
            collection.delete_many({})
            records = df.to_dict("records")
            if records:
                collection.insert_many(records)
                print("Politika değişikliği verisi MongoDB'ye kaydedildi.")
                logging.info("Politika değişikliği verisi MongoDB'ye kaydedildi.")
        except Exception as e:
            print(f"MongoDB'ye veri kaydedilemedi: {e}")
            logging.error(f"MongoDB'ye veri kaydedilemedi: {e}")
    else:
        print(f"API hatası: {response.status_code}. Dosya indirilemedi.")
        logging.error(f"API hatası: {response.status_code}. Dosya indirilemedi.")

def main():
    parser = argparse.ArgumentParser(description='İklim politikası değişikliği verisi çekme scripti (climate-laws.org)')
    parser.add_argument('--target_dir', type=str, required=True, help='Verinin kaydedileceği klasör')
    parser.add_argument('--log_path', type=str, default='logs/data_ingestion.log', help='Log dosyası yolu')
    args = parser.parse_args()
    fetch_policy_changes(args.target_dir, args.log_path)

if __name__ == '__main__':
    main() 