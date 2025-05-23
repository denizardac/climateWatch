import logging
import os
import glob
from data_ingestion.extract_pdf_data import extract_one_column_pdf

def ingest_pdf(config, log_path='logs/ingestion_pdf.log'):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    try:
        pdf_dir = config['data_paths'].get('policies', 'data_storage/policies')
        pdf_files = glob.glob(os.path.join(pdf_dir, '*.pdf'))
        if not pdf_files:
            logging.warning('PDF extraction için PDF dosyası bulunamadı. Lütfen ilgili klasöre PDF ekleyin.')
        for pdf_path in pdf_files:
            out_path = pdf_path.replace('.pdf', '.txt')
            extract_one_column_pdf(pdf_path, out_path, log_path=log_path)
            if os.path.exists(out_path):
                size = os.path.getsize(out_path)
                with open(out_path, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = sum(1 for _ in f)
                logging.info(f"PDF extraction çıktı: {out_path} | Boyut: {size} bytes | Satır: {lines}")
            else:
                logging.warning(f"PDF extraction çıktı dosyası bulunamadı: {out_path}")
    except Exception as e:
        logging.error(f"PDF extraction ingestion hatası: {e}") 