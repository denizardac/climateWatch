import os
import pandas as pd
import logging

def clean_policies(input_dir='data_storage/policies', output_file='data_storage/processed/cleaned_policies.csv', log_path='logs/clean_policies.log'):
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
                    if 'policy_name' in df.columns:
                        df = df.dropna(subset=['policy_name'])
                        df = df.drop_duplicates(subset=['policy_name'])
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
            logging.info(f"Tüm politika verisi temizlendi ve kaydedildi: {output_file} | Boyut: {size} bytes | Satır: {lines}")
        else:
            logging.warning("Hiçbir politika CSV dosyası işlenemedi!")
    except Exception as e:
        logging.error(f"Politika temizlik pipeline hatası: {e}")

if __name__ == '__main__':
    clean_policies() 