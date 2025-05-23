import os
import pandas as pd
import json
import logging

def clean_disasters(input_dir='data_storage/disasters', output_file='data_storage/processed/cleaned_disasters.csv', log_path='logs/clean_disasters.log'):
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    try:
        all_dfs = []
        for fname in os.listdir(input_dir):
            if fname.endswith('.json'):
                fpath = os.path.join(input_dir, fname)
                try:
                    with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                        data = json.load(f)
                    # ReliefWeb API formatına göre veri çıkarımı
                    if isinstance(data, dict) and 'data' in data:
                        records = [item['fields'] for item in data['data'] if 'fields' in item]
                        df = pd.DataFrame(records)
                    else:
                        df = pd.DataFrame(data)
                    # Temizlik: eksik önemli alanları at, duplikasyonları kaldır
                    if 'name' in df.columns:
                        df = df.dropna(subset=['name'])
                        df = df.drop_duplicates(subset=['name'])
                    all_dfs.append(df)
                    logging.info(f"Okundu ve temizlendi: {fpath} | Satır: {len(df)}")
                except Exception as e:
                    logging.error(f"Dosya okunamadı/temizlenemedi: {fpath} | Hata: {e}")
        if all_dfs:
            cleaned = pd.concat(all_dfs, ignore_index=True)
            cleaned.to_csv(output_file, index=False)
            size = os.path.getsize(output_file)
            with open(output_file, 'r', encoding='utf-8', errors='ignore') as f:
                lines = sum(1 for _ in f)
            logging.info(f"Tüm afet verisi temizlendi ve kaydedildi: {output_file} | Boyut: {size} bytes | Satır: {lines}")
        else:
            logging.warning("Hiçbir afet JSON dosyası işlenemedi!")
    except Exception as e:
        logging.error(f"Afet temizlik pipeline hatası: {e}")

if __name__ == '__main__':
    clean_disasters() 