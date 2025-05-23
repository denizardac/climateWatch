import requests
import os
from urllib.parse import urlparse
import zipfile
import glob
import logging

def setup_logger(log_path):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def download_files(url_list, target_dir, chunk_size=1024*1024, log_path="logs/data_ingestion.log"):
    setup_logger(log_path)
    os.makedirs(target_dir, exist_ok=True)
    for url in url_list:
        parsed = urlparse(url)
        filename = os.path.basename(parsed.path)
        if not filename:
            filename = 'downloaded_file'
        # Eğer dosya adı çok genel ise linkin bir kısmını ekle
        if filename in ['download', 'file', 'data']:
            filename = parsed.netloc + '_' + filename
        out_path = os.path.join(target_dir, filename)
        print(f"İndiriliyor: {url} -> {out_path}")
        logging.info(f"İndiriliyor: {url} -> {out_path}")
        try:
            with requests.get(url, stream=True) as r:
                if r.status_code == 200:
                    with open(out_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=chunk_size):
                            if chunk:
                                f.write(chunk)
                    print(f"Başarılı: {out_path}")
                    logging.info(f"Başarılı: {out_path}")
                else:
                    print(f"Hata: {url} (status {r.status_code})")
                    logging.error(f"Hata: {url} (status {r.status_code})")
        except Exception as e:
            print(f"İndirme hatası: {url} ({e})")
            logging.error(f"İndirme hatası: {url} ({e})")
    # İndirilen zip dosyalarını aç ve temizle
    extract_and_clean(target_dir, log_path)

def extract_and_clean(target_dir, log_path="logs/data_ingestion.log"):
    # Tüm zip dosyalarını bul
    zip_files = glob.glob(os.path.join(target_dir, '*.zip'))
    for zip_path in zip_files:
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(target_dir)
            print(f"Açıldı ve çıkarıldı: {zip_path}")
            logging.info(f"Açıldı ve çıkarıldı: {zip_path}")
            os.remove(zip_path)
        except Exception as e:
            print(f"Zip açma hatası: {zip_path} ({e})")
            logging.error(f"Zip açma hatası: {zip_path} ({e})")
    # Tekrar zip, rar, exe dosyalarını sil
    for ext in ['*.zip', '*.rar', '*.exe']:
        for f in glob.glob(os.path.join(target_dir, ext)):
            try:
                os.remove(f)
                print(f"Silindi: {f}")
                logging.info(f"Silindi: {f}")
            except Exception as e:
                print(f"Silme hatası: {f} ({e})")
                logging.error(f"Silme hatası: {f} ({e})")

if __name__ == "__main__":
    # Örnek kullanım: scripti elle çalıştırırken linkleri buraya ekleyebilirsin
    import argparse
    parser = argparse.ArgumentParser(description='Açık veri kaynaklarından dosya indirme scripti')
    parser.add_argument('--log_path', type=str, default='logs/data_ingestion.log', help='Log dosyası yolu')
    args = parser.parse_args()
    url_list = [
        'https://zenodo.org/records/10396148/files/population.zip?download=1',
        'https://zenodo.org/records/10396148/files/yield_productivity.zip?download=1',
        'https://zenodo.org/records/10396148/files/mask.zip?download=1',
        'https://zenodo.org/records/10396148/files/code.zip?download=1',
        'https://zenodo.org/records/7803903/files/SurfaceWaterArea_India_Rivers_Basins_1991_2020.csv?download=1',
    ]
    download_files(url_list, "data_storage/open_datasets", log_path=args.log_path) 