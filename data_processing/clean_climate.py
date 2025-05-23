import os
import pandas as pd
import logging

def clean_climate(input_dir='data_storage/climate', output_file='data_storage/processed/cleaned_climate.csv', log_path='logs/clean_climate.log'):
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
                    # Temizlik: sayısal değerleri kontrol et, aykırı değerleri temizle, yılları sırala
                    if 'Mean' in df.columns:
                        df['Mean'] = pd.to_numeric(df['Mean'], errors='coerce')
                        mean = df['Mean'].mean()
                        std = df['Mean'].std()
                        df = df[abs(df['Mean'] - mean) <= 3 * std]
                    if 'Year' in df.columns:
                        df = df.sort_values('Year')
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
            logging.info(f"Tüm iklim verisi temizlendi ve kaydedildi: {output_file} | Boyut: {size} bytes | Satır: {lines}")
        else:
            logging.warning("Hiçbir iklim CSV dosyası işlenemedi!")
    except Exception as e:
        logging.error(f"İklim temizlik pipeline hatası: {e}")

if __name__ == '__main__':
    clean_climate() 