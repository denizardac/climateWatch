import requests
import csv
import os
import re

def fetch_all_climate_trends(target_dir, keyword_list=["climate", "climate change"]):
    sheet_url = 'https://docs.google.com/spreadsheets/d/1XbUaC5KDd-EMLUOolzj53Sn4sk5P9hL9ZbYxBEchZpY/pub?output=csv'
    response = requests.get(sheet_url)
    if response.status_code != 200:
        print(f"Google Trends meta CSV erişilemedi: {response.status_code}")
        return
    os.makedirs(target_dir, exist_ok=True)
    found = 0
    lines = response.content.decode('utf-8').splitlines()
    reader = csv.DictReader(lines)
    for row in reader:
        title = row.get('title', '').lower()
        subject = row.get('subject', '').lower()
        link = row.get('link', '')
        for keyword in keyword_list:
            if keyword in title or keyword in subject:
                # Dosya adını başlık ve tarih ile oluştur
                safe_title = re.sub(r'[^a-zA-Z0-9_\-]', '_', row.get('title', 'trend'))
                date = row.get('date', 'unknown')
                ext = os.path.splitext(link)[-1].lower()
                if ext in ['.csv', '.xlsx']:
                    # Link tam URL değilse başına ana URL ekle
                    if not link.startswith('http'):
                        link_full = f'https://googletrends.github.io/data/{link}'
                    else:
                        link_full = link
                    out_path = os.path.join(target_dir, f"trends_{safe_title}_{date}{ext}")
                    print(f"İndiriliyor: {out_path}\nLink: {link_full}")
                    r = requests.get(link_full)
                    print(f"HTTP status: {r.status_code}")
                    if r.status_code == 200:
                        with open(out_path, 'wb') as f:
                            f.write(r.content)
                        print(f"Başarılı: {out_path}")
                        found += 1
                    else:
                        print(f"Hata: {link_full} (status {r.status_code})")
                break
    if found == 0:
        print("Hiçbir 'climate' anahtar kelimesi geçen CSV/XLSX bulunamadı.")
    else:
        print(f"Toplam {found} dosya indirildi.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Google Trends arşiv verisi çekme scripti (Datastore)')
    parser.add_argument('--target_dir', type=str, required=True, help='Verinin kaydedileceği klasör')
    args = parser.parse_args()
    fetch_all_climate_trends(args.target_dir) 