import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
import os

# Logging ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NOAADownloader:
    def __init__(self):
        self.base_url = "https://www.ncei.noaa.gov/access/services/data/v1"
        self.data_dir = "data_storage/climate"
        os.makedirs(self.data_dir, exist_ok=True)
        
    def download_temperature_data(self, start_date, end_date):
        """
        NOAA'dan sıcaklık verilerini indirir
        
        Args:
            start_date (str): Başlangıç tarihi (YYYY-MM-DD)
            end_date (str): Bitiş tarihi (YYYY-MM-DD)
        """
        try:
            # API parametreleri
            params = {
                'dataset': 'global-temperatures',
                'startDate': start_date,
                'endDate': end_date,
                'format': 'json',
                'units': 'metric'
            }
            
            # API isteği
            response = requests.get(f"{self.base_url}/temperature", params=params)
            response.raise_for_status()
            
            # Veriyi DataFrame'e çevir
            data = response.json()
            df = pd.DataFrame(data)
            
            # Tarih sütununu düzenle
            df['date'] = pd.to_datetime(df['date'])
            
            # Dosyayı kaydet
            output_file = os.path.join(self.data_dir, f"temperature_{start_date}_{end_date}.csv")
            df.to_csv(output_file, index=False)
            
            logger.info(f"Sıcaklık verileri indirildi ve kaydedildi: {output_file}")
            return df
            
        except Exception as e:
            logger.error(f"Veri indirme hatası: {str(e)}")
            return None
            
    def download_co2_data(self, start_date, end_date):
        """
        NOAA'dan CO2 verilerini indirir
        
        Args:
            start_date (str): Başlangıç tarihi (YYYY-MM-DD)
            end_date (str): Bitiş tarihi (YYYY-MM-DD)
        """
        try:
            # API parametreleri
            params = {
                'dataset': 'co2-concentrations',
                'startDate': start_date,
                'endDate': end_date,
                'format': 'json'
            }
            
            # API isteği
            response = requests.get(f"{self.base_url}/co2", params=params)
            response.raise_for_status()
            
            # Veriyi DataFrame'e çevir
            data = response.json()
            df = pd.DataFrame(data)
            
            # Tarih sütununu düzenle
            df['date'] = pd.to_datetime(df['date'])
            
            # Dosyayı kaydet
            output_file = os.path.join(self.data_dir, f"co2_{start_date}_{end_date}.csv")
            df.to_csv(output_file, index=False)
            
            logger.info(f"CO2 verileri indirildi ve kaydedildi: {output_file}")
            return df
            
        except Exception as e:
            logger.error(f"Veri indirme hatası: {str(e)}")
            return None

def main():
    # Downloader'ı başlat
    downloader = NOAADownloader()
    
    # Son 5 yıllık veriyi indir
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=5*365)).strftime('%Y-%m-%d')
    
    # Sıcaklık verilerini indir
    temp_df = downloader.download_temperature_data(start_date, end_date)
    if temp_df is not None:
        print("\nSıcaklık verisi örneği:")
        print(temp_df.head())
        
    # CO2 verilerini indir
    co2_df = downloader.download_co2_data(start_date, end_date)
    if co2_df is not None:
        print("\nCO2 verisi örneği:")
        print(co2_df.head())

if __name__ == "__main__":
    main() 