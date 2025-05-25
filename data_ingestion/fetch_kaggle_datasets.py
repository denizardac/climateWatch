import os
import subprocess
import logging
import json
import pandas as pd
import io
from datetime import datetime

def setup_logger(log_path):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def download_kaggle_datasets(search_term, target_dir, max_datasets=3, log_path="logs/data_ingestion.log"):
    setup_logger(log_path)
    try:
        print(f"Kaggle'da '{search_term}' için veri kümeleri aranıyor...")
        
        # Kaggle API'sini kullanarak veri setlerini ara
        search_cmd = f"kaggle datasets list -s {search_term} --csv"
        result = subprocess.run(search_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"Kaggle arama hatası: {result.stderr}")
            
        # CSV çıktısını DataFrame'e dönüştür
        df = pd.read_csv(io.StringIO(result.stdout))
        
        # En fazla max_datasets kadar veri seti indir
        for i, row in df.head(max_datasets).iterrows():
            dataset_name = row['ref']
            print(f"Veri seti indiriliyor: {dataset_name}")
            
            # Veri setini indir
            download_cmd = f"kaggle datasets download {dataset_name} -p {target_dir} --unzip"
            result = subprocess.run(download_cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode != 0:
                logging.error(f"Veri seti indirme hatası ({dataset_name}): {result.stderr}")
                continue
                
            logging.info(f"Veri seti indirildi: {dataset_name}")
            print(f"Veri seti indirildi: {dataset_name}")
            
    except Exception as e:
        print(f"Hata oluştu: {e}")
        logging.error(f"Hata oluştu: {e}")

if __name__ == "__main__":
    download_kaggle_datasets("climate", "data_storage/kaggle") 