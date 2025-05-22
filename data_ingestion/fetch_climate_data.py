import pandas as pd
import os
import logging
import argparse
from datetime import datetime

def setup_logger(log_path):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def fetch_and_save_climate_data(target_dir="data_storage/climate", save_to_mongo=True, log_path="logs/data_ingestion.log", mongo_host="localhost"):
    setup_logger(log_path)
    try:
        url = "https://datahub.io/core/global-temp/r/annual.csv"
        df = pd.read_csv(url)
        os.makedirs(target_dir, exist_ok=True)
        csv_path = os.path.join(target_dir, "global_temp.csv")
        df.to_csv(csv_path, index=False)
        print(f"Veri indirildi ve kaydedildi: {csv_path}")
        logging.info(f"İklim verisi indirildi ve kaydedildi: {csv_path}")

        if save_to_mongo:
            from pymongo import MongoClient
            client = MongoClient(f"mongodb://{mongo_host}:27017/")
            db = client['climatewatch']
            collection = db['global_temp']
            collection.delete_many({})
            collection.insert_many(df.to_dict("records"))
            print("Veri MongoDB'ye kaydedildi.")
            logging.info("İklim verisi MongoDB'ye kaydedildi.")
    except Exception as e:
        print(f"Hata oluştu: {e}")
        logging.error(f"Hata oluştu: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="İklim verisi indirici ve MongoDB'ye kaydedici.")
    parser.add_argument('--target_dir', type=str, default="data_storage/climate", help='Verinin kaydedileceği klasör')
    parser.add_argument('--no_mongo', action='store_true', help='MongoDB kaydını kapat')
    parser.add_argument('--log_path', type=str, default="logs/data_ingestion.log", help='Log dosyası yolu')
    parser.add_argument('--mongo_host', type=str, default="mongodb", help='MongoDB host adresi (localhost veya mongodb)')
    args = parser.parse_args()
    fetch_and_save_climate_data(target_dir=args.target_dir, save_to_mongo=not args.no_mongo, log_path=args.log_path, mongo_host=args.mongo_host) 