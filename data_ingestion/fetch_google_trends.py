"""
fetch_google_trends.py
Google Trends API veya pytrends ile iklim değişikliğiyle ilgili arama ilgisi verisi çekmek için iskelet script.
"""

import argparse
import os
import pandas as pd
from pytrends.request import TrendReq
from pymongo import MongoClient

def fetch_trends(keyword, start_date, end_date, target_dir):
    os.makedirs(target_dir, exist_ok=True)
    pytrends = TrendReq(hl='en-US', tz=360)
    kw_list = [keyword]
    try:
        pytrends.build_payload(kw_list, timeframe=f'{start_date} {end_date}')
        df = pytrends.interest_over_time()
        if df.empty:
            print(f"Google Trends verisi bulunamadı: {keyword} {start_date}-{end_date}")
            return
        out_path = os.path.join(target_dir, f'trends_{keyword.replace(" ", "_")}_{start_date}_{end_date}.csv')
        df.to_csv(out_path)
        print(f"Google Trends verisi kaydedildi: {out_path}")

        # MongoDB'ye veri kaydet
        client = MongoClient("mongodb://localhost:27017/")
        db = client['climatewatch']
        collection = db['google_trends']
        # Aynı keyword ve tarih aralığına ait kayıtları sil
        collection.delete_many({"keyword": keyword, "start_date": start_date, "end_date": end_date})
        records = df.reset_index().to_dict("records")
        for rec in records:
            rec["keyword"] = keyword
            rec["start_date"] = start_date
            rec["end_date"] = end_date
        if records:
            collection.insert_many(records)
            print("Google Trends verisi MongoDB'ye kaydedildi.")
    except Exception as e:
        print(f"Google Trends API hatası: {e}")

def main():
    parser = argparse.ArgumentParser(description='Google Trends verisi çekme scripti (pytrends)')
    parser.add_argument('--keyword', type=str, required=True, help='Anahtar kelime')
    parser.add_argument('--start_date', type=str, required=True, help='Başlangıç tarihi (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, required=True, help='Bitiş tarihi (YYYY-MM-DD)')
    parser.add_argument('--target_dir', type=str, required=True, help='Verinin kaydedileceği klasör')
    args = parser.parse_args()
    fetch_trends(args.keyword, args.start_date, args.end_date, args.target_dir)

if __name__ == '__main__':
    main() 