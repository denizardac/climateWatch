"""
fetch_disaster_events.py
Küresel veya bölgesel doğal afet (yangın, sel, kasırga, deprem vb.) verilerini açık API veya veri kaynağından çekmek için iskelet script.
Gerçek API entegrasyonu için ilgili endpoint ve anahtarlar eklenmelidir.
"""

import argparse
import requests
import os
import json
import logging

def setup_logger(log_path):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def fetch_disaster_events(api_url, start_date, end_date, target_dir, log_path="logs/data_ingestion.log"):
    setup_logger(log_path)
    os.makedirs(target_dir, exist_ok=True)
    params = {
        'appname': 'climatewatch',
        'limit': 100,
        'profile': 'list',
        'disaster_type': '',
        'date': f'{start_date}|{end_date}'
    }
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        data = response.json()
        out_path = os.path.join(target_dir, f'disasters_{start_date}_{end_date}.json')
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Afet verisi kaydedildi: {out_path}")
        logging.info(f"Afet verisi kaydedildi: {out_path}")
    else:
        print(f"API hatası: {response.status_code}")
        logging.error(f"API hatası: {response.status_code}")

def main():
    parser = argparse.ArgumentParser(description='Doğal afet verisi çekme scripti (ReliefWeb API)')
    parser.add_argument('--api_url', type=str, default='https://api.reliefweb.int/v1/disasters', help='Afet API endpoint URL')
    parser.add_argument('--start_date', type=str, required=True, help='Başlangıç tarihi (YYYYMMDD)')
    parser.add_argument('--end_date', type=str, required=True, help='Bitiş tarihi (YYYYMMDD)')
    parser.add_argument('--target_dir', type=str, required=True, help='Verinin kaydedileceği klasör')
    parser.add_argument('--log_path', type=str, default='logs/data_ingestion.log', help='Log dosyası yolu')
    args = parser.parse_args()
    fetch_disaster_events(args.api_url, args.start_date, args.end_date, args.target_dir, args.log_path)

if __name__ == '__main__':
    main() 