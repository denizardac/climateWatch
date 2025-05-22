import pdfplumber
import argparse
import os

def extract_one_column_pdf(pdf_path, out_path):
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UNFCCC PDF'den metin çıkarımı (tek sütunlu metin PDF)")
    parser.add_argument('--pdf_path', type=str, required=True, help='Girdi PDF dosyası')
    parser.add_argument('--out_path', type=str, required=True, help='Çıktı metin dosyası')
    args = parser.parse_args()
    extract_one_column_pdf(args.pdf_path, args.out_path) 