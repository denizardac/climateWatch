import os
import pandas as pd
import logging

def clean_pdf(input_dir='data_storage/policies', output_file='data_storage/processed/cleaned_pdf.csv', log_path='logs/clean_pdf.log'):
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    try:
        rows = []
        for fname in os.listdir(input_dir):
            if fname.endswith('.txt'):
                fpath = os.path.join(input_dir, fname)
                try:
                    with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                        text = f.read().strip()
                    if text:
                        rows.append({'filename': fname, 'text': text})
                        logging.info(f"Okundu ve temizlendi: {fpath} | Karakter: {len(text)}")
                except Exception as e:
                    logging.error(f"Dosya okunamadı/temizlenemedi: {fpath} | Hata: {e}")
        if rows:
            df = pd.DataFrame(rows)
            df.to_csv(output_file, index=False)
            size = os.path.getsize(output_file)
            with open(output_file, 'r', encoding='utf-8', errors='ignore') as f:
                lines = sum(1 for _ in f)
            logging.info(f"Tüm PDF extraction verisi temizlendi ve kaydedildi: {output_file} | Boyut: {size} bytes | Satır: {lines}")
        else:
            logging.warning("Hiçbir PDF extraction TXT dosyası işlenemedi!")
    except Exception as e:
        logging.error(f"PDF extraction temizlik pipeline hatası: {e}")

if __name__ == '__main__':
    clean_pdf() 