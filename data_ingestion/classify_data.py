import argparse
import csv
import re
import logging
import os

def setup_logger(log_path):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def classify_lines(txt_path, out_csv, log_path="logs/data_ingestion.log"):
    setup_logger(log_path)
    with open(txt_path, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip()]

    # Basit örnek: Satırda ülke/kurum, isim ve unvanı ayırmaya çalış
    rows = []
    for line in lines:
        # Çok basit bir pattern: "Mr./Ms./Dr." ile başlayanlar isim/unvan olabilir
        m = re.match(r"(Mr\.|Ms\.|Dr\.|Mrs\.|H\.E\.|Prof\.) ([A-Za-z .'-]+)(,? ?)(.*)", line)
        if m:
            honorific = m.group(1)
            name = m.group(2)
            rest = m.group(4)
            rows.append({'Honorific': honorific, 'Name': name, 'Rest': rest})
        else:
            # Ülke/kurum satırı olabilir
            rows.append({'Honorific': '', 'Name': '', 'Rest': line})

    with open(out_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['Honorific', 'Name', 'Rest'])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"Classified data saved to: {out_csv}")
    logging.info(f"Classified data saved to: {out_csv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UNFCCC PDF metnini temel alan satır sınıflandırıcı")
    parser.add_argument('--txt_path', type=str, required=True, help='Girdi metin dosyası')
    parser.add_argument('--out_csv', type=str, required=True, help='Çıktı CSV dosyası')
    parser.add_argument('--log_path', type=str, default='logs/data_ingestion.log', help='Log dosyası yolu')
    args = parser.parse_args()
    classify_lines(args.txt_path, args.out_csv, args.log_path) 