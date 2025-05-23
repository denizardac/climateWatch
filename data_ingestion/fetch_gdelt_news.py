import pandas as pd
import requests
import zipfile
import io
import os
import logging
import argparse
from datetime import datetime, timedelta

def setup_logger(log_path):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def fetch_gdelt_for_date(date, target_dir="data_storage/gdelt", save_to_mongo=True, keywords=None, log_path="logs/data_ingestion.log", mongo_host="localhost"):
    try:
        date_str = date.strftime("%Y%m%d")
        url = f"http://data.gdeltproject.org/events/{date_str}.export.CSV.zip"
        r = requests.get(url)
        if r.status_code != 200:
            msg = f"Veri bulunamadı: {url}"
            print(msg)
            logging.warning(msg)
            return
        z = zipfile.ZipFile(io.BytesIO(r.content))
        csv_filename = z.namelist()[0]
        with z.open(csv_filename) as f:
            df = pd.read_csv(f, sep='\t', header=None, low_memory=False)
        os.makedirs(target_dir, exist_ok=True)
        csv_path = os.path.join(target_dir, f"{date_str}.csv")
        df['date'] = date.strftime("%Y-%m-%d")
        df.to_csv(csv_path, index=False)
        print(f"{date_str} verisi indirildi ve kaydedildi: {csv_path}")
        logging.info(f"{date_str} GDELT verisi indirildi ve kaydedildi: {csv_path}")

        # Filtreleme
        if keywords:
            mask = df[27].astype(str).str.contains('|'.join(keywords), na=False) | \
                   df[28].astype(str).str.contains('|'.join(keywords), na=False)
            df = df[mask]
            print(f"Filtrelenen satır sayısı: {len(df)}")
            logging.info(f"{date_str} için filtrelenen satır sayısı: {len(df)}")

        if save_to_mongo and not df.empty:
            from pymongo import MongoClient
            client = MongoClient(f"mongodb://{mongo_host}:27017/")
            db = client['climatewatch']
            collection = db['gdelt_events']
            collection.delete_many({"date": date_str})
            records = df.to_dict("records")
            for rec in records:
                rec["date"] = date_str
            collection.insert_many(records)
            print("Veri MongoDB'ye kaydedildi.")
            logging.info(f"{date_str} verisi MongoDB'ye kaydedildi.")
    except Exception as e:
        print(f"Hata oluştu: {e}")
        logging.error(f"Hata oluştu: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GDELT haber verisi indirici ve MongoDB'ye kaydedici.")
    parser.add_argument('--start_date', type=str, help='Başlangıç tarihi (YYYYMMDD)')
    parser.add_argument('--end_date', type=str, help='Bitiş tarihi (YYYYMMDD)')
    parser.add_argument('--target_dir', type=str, default="data_storage/gdelt", help='Verinin kaydedileceği klasör')
    parser.add_argument('--no_mongo', action='store_true', help='MongoDB kaydını kapat')
    parser.add_argument('--keywords', type=str, nargs='*', default=["CLIMATE", "ENVIRONMENT", "WEATHER", "GLOBAL WARMING", "CO2", "CARBON", "EMISSION"], help='Filtre anahtar kelimeleri')
    parser.add_argument('--log_path', type=str, default="logs/data_ingestion.log", help='Log dosyası yolu')
    parser.add_argument('--mongo_host', type=str, default="mongodb", help='MongoDB host adresi (localhost veya mongodb)')
    args = parser.parse_args()

    if args.start_date and args.end_date:
        start = datetime.strptime(args.start_date, "%Y%m%d")
        end = datetime.strptime(args.end_date, "%Y%m%d")
        delta = (end - start).days
        for i in range(delta + 1):
            fetch_gdelt_for_date(start + timedelta(days=i), target_dir=args.target_dir, save_to_mongo=not args.no_mongo, keywords=args.keywords, log_path=args.log_path, mongo_host=args.mongo_host)
    else:
        # Varsayılan: son 7 gün
        for i in range(7):
            fetch_gdelt_for_date(datetime.today() - timedelta(days=i), target_dir=args.target_dir, save_to_mongo=not args.no_mongo, keywords=args.keywords, log_path=args.log_path, mongo_host=args.mongo_host) 