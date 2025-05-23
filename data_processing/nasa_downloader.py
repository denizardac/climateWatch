import requests
import pandas as pd
import logging
import os
from datetime import datetime

# Logging ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NASADownloader:
    def __init__(self):
        self.base_url = "https://data.giss.nasa.gov/gistemp/tabledata_v4"
        self.data_dir = "data_storage/climate"
        os.makedirs(self.data_dir, exist_ok=True)
        
    def download_temperature_data(self):
        """
        NASA'nın GISTEMP veri setini indirir
        """
        try:
            # Global sıcaklık verisi için URL
            url = f"{self.base_url}/GLB.Ts+dSST.txt"
            
            # Veriyi indir
            response = requests.get(url)
            response.raise_for_status()
            
            # Veriyi DataFrame'e çevir
            # İlk 8 satırı atla (başlık ve açıklamalar)
            lines = response.text.split('\n')[8:]
            
            # Veriyi parse et
            data = []
            for line in lines:
                if line.strip():  # Boş satırları atla
                    parts = line.split()
                    if len(parts) >= 19:  # Yeterli veri var mı kontrol et
                        try:
                            year = int(parts[0])
                            # Yıllık ortalama sıcaklık anomalisi (13. sütun)
                            anomaly = float(parts[13])
                            data.append({'Year': year, 'Temperature_Anomaly': anomaly})
                        except (ValueError, IndexError):
                            continue  # Hatalı satırları atla
            
            # DataFrame oluştur
            df = pd.DataFrame(data)
            
            # Dosyayı kaydet
            output_file = os.path.join(self.data_dir, "nasa_temperature.csv")
            df.to_csv(output_file, index=False)
            
            logger.info(f"Sıcaklık verileri indirildi ve kaydedildi: {output_file}")
            return df
            
        except Exception as e:
            logger.error(f"Veri indirme hatası: {str(e)}")
            return None
            
    def download_co2_data(self):
        """
        NOAA'nın CO2 veri setini indirir
        """
        try:
            # CO2 verisi için URL
            url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_annmean_mlo.txt"
            
            # Veriyi indir
            response = requests.get(url)
            response.raise_for_status()
            
            # Veriyi DataFrame'e çevir
            # İlk 56 satırı atla (başlık ve açıklamalar)
            lines = response.text.split('\n')[56:]
            
            # Veriyi parse et
            data = []
            for line in lines:
                if line.strip():  # Boş satırları atla
                    parts = line.split()
                    if len(parts) >= 2:  # Yeterli veri var mı kontrol et
                        try:
                            year = int(parts[0])
                            co2 = float(parts[1])
                            data.append({'Year': year, 'CO2': co2})
                        except (ValueError, IndexError):
                            continue  # Hatalı satırları atla
            
            # DataFrame oluştur
            df = pd.DataFrame(data)
            
            # Dosyayı kaydet
            output_file = os.path.join(self.data_dir, "noaa_co2.csv")
            df.to_csv(output_file, index=False)
            
            logger.info(f"CO2 verileri indirildi ve kaydedildi: {output_file}")
            return df
            
        except Exception as e:
            logger.error(f"Veri indirme hatası: {str(e)}")
            return None

def main():
    # Downloader'ı başlat
    downloader = NASADownloader()
    
    # Sıcaklık verilerini indir
    temp_df = downloader.download_temperature_data()
    if temp_df is not None:
        print("\nSıcaklık verisi örneği:")
        print(temp_df.head())
        
    # CO2 verilerini indir
    co2_df = downloader.download_co2_data()
    if co2_df is not None:
        print("\nCO2 verisi örneği:")
        print(co2_df.head())

if __name__ == "__main__":
    main() 