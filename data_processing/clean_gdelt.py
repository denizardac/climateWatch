import os
import pandas as pd
import logging

def clean_gdelt(input_dir='data_storage/gdelt', output_file='data_storage/processed/cleaned_gdelt.csv', log_path='logs/clean_gdelt.log'):
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
                    # Temizlik: eksik başlık/metinleri at, küçük harfe çevir, duplikasyonları kaldır
                    if 'title' in df.columns and 'text' in df.columns:
                        df['title'] = df['title'].astype(str).str.lower()
                        df['text'] = df['text'].astype(str).str.lower()
                        df = df.dropna(subset=['title', 'text'])
                        df = df.drop_duplicates(subset=['title', 'text'])
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
            logging.info(f"Tüm GDELT verisi temizlendi ve kaydedildi: {output_file} | Boyut: {size} bytes | Satır: {lines}")
        else:
            logging.warning("Hiçbir GDELT CSV dosyası işlenemedi!")
    except Exception as e:
        logging.error(f"GDELT temizlik pipeline hatası: {e}")

if __name__ == '__main__':
    clean_gdelt() 