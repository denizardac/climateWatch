import os
import pandas as pd
import logging

def clean_trends(input_dir='data_storage/trends', output_file='data_storage/processed/cleaned_trends.csv', log_path='logs/clean_trends.log'):
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    try:
        all_dfs = []
        for fname in os.listdir(input_dir):
            if fname.endswith('.csv'):
                fpath = os.path.join(input_dir, fname)
                try:
                    df = pd.read_csv(fpath)
                    # Temizlik: eksik önemli alanları at, duplikasyonları kaldır
                    if 'trend' in df.columns:
                        df = df.dropna(subset=['trend'])
                        df = df.drop_duplicates(subset=['trend'])
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
            logging.info(f"Tüm trends verisi temizlendi ve kaydedildi: {output_file} | Boyut: {size} bytes | Satır: {lines}")
        else:
            logging.warning("Hiçbir trends CSV dosyası işlenemedi!")
    except Exception as e:
        logging.error(f"Trends temizlik pipeline hatası: {e}")

if __name__ == '__main__':
    clean_trends() 