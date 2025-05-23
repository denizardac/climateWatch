"""
fetch_policy_changes.py
Ülkelerin iklim politikası değişiklikleri, anlaşmalar ve yasalar için veri çekme scripti (iskele).
Gerçek API veya veri kaynağı entegrasyonu için endpoint ve anahtarlar eklenmelidir.
"""

import argparse
import requests
import os
import logging

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