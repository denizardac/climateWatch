import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import time
import logging

# Logging ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GDELTDownloader:
    def __init__(self, output_dir='data_storage/gdelt'):
        self.base_url = "https://api.gdeltproject.org/api/v2/doc/doc"
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
    def download_data(self, start_date, end_date, query="climate change OR global warming"):
        """
        GDELT'den veri indirir
        
        Args:
            start_date (str): Başlangıç tarihi (YYYY-MM-DD)
            end_date (str): Bitiş tarihi (YYYY-MM-DD)
            query (str): Arama sorgusu
        """
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y%m%d')
            output_file = os.path.join(self.output_dir, f"{date_str}.csv")
            
            if os.path.exists(output_file):
                logger.info(f"{date_str} için veri zaten mevcut, atlanıyor...")
                current_date += timedelta(days=1)
                continue
            
            try:
                # API parametreleri
                params = {
                    'query': query,
                    'mode': 'artlist',
                    'format': 'csv',
                    'startdatetime': f"{date_str}000000",
                    'enddatetime': f"{date_str}235959",
                    'maxrecords': 250,  # Günlük maksimum kayıt sayısı
                    'sort': 'hybridrel',
                    'output': 'artlist'
                }
                
                logger.info(f"{date_str} için veri indiriliyor...")
                response = requests.get(self.base_url, params=params)
                
                if response.status_code == 200:
                    # Veriyi CSV olarak kaydet
                    with open(output_file, 'w', encoding='utf-8') as f:
                        f.write(response.text)
                    
                    # Veriyi kontrol et
                    df = pd.read_csv(output_file)
                    logger.info(f"{date_str} için {len(df)} kayıt indirildi")
                    
                    # Sütunları kontrol et
                    required_columns = ['Actor1CountryCode', 'Actor2CountryCode', 'ActionGeo_CountryCode']
                    missing_columns = [col for col in required_columns if col not in df.columns]
                    
                    if missing_columns:
                        logger.warning(f"{date_str} için eksik sütunlar: {missing_columns}")
                        os.remove(output_file)  # Eksik sütunlu dosyayı sil
                    else:
                        logger.info(f"{date_str} için veri başarıyla kaydedildi")
                
                else:
                    logger.error(f"{date_str} için veri indirilemedi: {response.status_code}")
                    logger.error(f"API Yanıtı: {response.text}")
                    logger.error(f"API URL: {response.url}")
                
                # API limit aşımını önlemek için bekle
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"{date_str} için hata oluştu: {str(e)}")
            
            current_date += timedelta(days=1)

def main():
    # İndiriciyi başlat
    downloader = GDELTDownloader()
    
    # Son 7 günün verilerini indir (test için daha kısa bir süre)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)  # 7 gün
    
    # Tarihleri string formatına çevir
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    logger.info(f"Veri indirme başlıyor: {start_date_str} - {end_date_str}")
    
    # Verileri indir
    downloader.download_data(
        start_date=start_date_str,
        end_date=end_date_str,
        query="climate change OR global warming OR sustainability OR environmental"
    )
    
    logger.info("Veri indirme tamamlandı")

if __name__ == "__main__":
    main() 