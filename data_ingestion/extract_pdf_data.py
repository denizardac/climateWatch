import pdfplumber
import argparse
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

def extract_one_column_pdf(pdf_path, out_path, log_path="logs/data_ingestion.log"):
    setup_logger(log_path)
    data = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                lines = text.split('\n')
                data.extend(lines)
    with open(out_path, 'w', encoding='utf-8') as f:
        for line in data:
            f.write(line + '\n')
    print(f"Extracted text saved to: {out_path}")
    logging.info(f"Extracted text saved to: {out_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UNFCCC PDF'den metin çıkarımı (tek sütunlu metin PDF)")
    parser.add_argument('--pdf_path', type=str, required=True, help='Girdi PDF dosyası')
    parser.add_argument('--out_path', type=str, required=True, help='Çıktı metin dosyası')
    parser.add_argument('--log_path', type=str, default='logs/data_ingestion.log', help='Log dosyası yolu')
    args = parser.parse_args()
    extract_one_column_pdf(args.pdf_path, args.out_path, args.log_path) 