import os
import subprocess
import sys
import logging

def setup_logger(log_path):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def download_kaggle_datasets(keyword, target_dir, max_datasets=3, log_path="logs/data_ingestion.log"):
    setup_logger(log_path)
    os.makedirs(target_dir, exist_ok=True)
    # Kaggle arama komutu
    search_cmd = [
        'kaggle', 'datasets', 'list', '--search', keyword, '--sort-by', 'hottest'
    ]
    print(f"Kaggle'da '{keyword}' için veri kümeleri aranıyor...")
    logging.info(f"Kaggle'da '{keyword}' için veri kümeleri aranıyor...")
    result = subprocess.run(search_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Kaggle arama hatası: {result.stderr}")
        logging.error(f"Kaggle arama hatası: {result.stderr}")
        return
    lines = result.stdout.strip().split('\n')
    if len(lines) <= 1:
        print("Hiç veri kümesi bulunamadı.")
        logging.warning("Hiç veri kümesi bulunamadı.")
        return
    # İlk satır başlık, diğerleri veri kümesi
    datasets = lines[1:max_datasets+1]  # Sadece ilk max_datasets kadarını al
    for line in datasets:
        parts = line.split()
        if not parts:
            continue
        ref = parts[0]
        # Sadece geçerli Kaggle dataset referanslarını işle (örn: kullanici/dataset-adi)
        if '/' not in ref or ref.startswith('-'):
            continue
        print(f"İndiriliyor: {ref}")
        logging.info(f"İndiriliyor: {ref}")
        download_cmd = [
            'kaggle', 'datasets', 'download', '-d', ref, '-p', target_dir, '--unzip'
        ]
        dl_result = subprocess.run(download_cmd, capture_output=True, text=True)
        if dl_result.returncode == 0:
            print(f"Başarılı: {ref}")
            logging.info(f"Başarılı: {ref}")
        else:
            print(f"Hata: {ref} -> {dl_result.stderr}")
            logging.error(f"Hata: {ref} -> {dl_result.stderr}")

    # --- MongoDB'ye CSV dosyalarını kaydet ---
    try:
        from pymongo import MongoClient
        import pandas as pd
        client = MongoClient("mongodb://localhost:27017/")
        db = client['climatewatch']
        collection = db['kaggle_data']
        csv_files = [f for f in os.listdir(target_dir) if f.endswith('.csv')]
        for csv_file in csv_files:
            csv_path = os.path.join(target_dir, csv_file)
            try:
                df = pd.read_csv(csv_path)
                collection.delete_many({"source_file": csv_file})
                records = df.to_dict("records")
                for rec in records:
                    rec["source_file"] = csv_file
                if records:
                    collection.insert_many(records)
                    print(f"{csv_file} MongoDB'ye kaydedildi.")
            except Exception as e:
                print(f"MongoDB'ye CSV kaydedilemedi: {csv_file} ({e})")
    except Exception as e:
        print(f"MongoDB bağlantı hatası: {e}")

if __name__ == "__main__":
    # Komut satırından anahtar kelime ve hedef klasör al
    import argparse
    parser = argparse.ArgumentParser(description='Kaggle veri kümelerini anahtar kelimeyle indir.')
    parser.add_argument('--keyword', type=str, required=True, help='Aranacak anahtar kelime')
    parser.add_argument('--target_dir', type=str, default='data_storage/kaggle', help='Kayıt klasörü')
    parser.add_argument('--max_datasets', type=int, default=3, help='Kaç veri kümesi indirilecek (varsayılan: 3)')
    parser.add_argument('--log_path', type=str, default='logs/data_ingestion.log', help='Log dosyası yolu')
    args = parser.parse_args()
    download_kaggle_datasets(args.keyword, args.target_dir, args.max_datasets, args.log_path) 