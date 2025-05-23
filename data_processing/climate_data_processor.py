import pandas as pd
import requests
import logging
import os
from datetime import datetime

# Logging ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ClimateDataProcessor:
    def __init__(self):
        self.data_dir = "data_storage/climate"
        os.makedirs(self.data_dir, exist_ok=True)
        
    def download_nasa_temperature(self):
        """
        NASA'nın küresel sıcaklık anomalisi verilerini indirir
        """
        try:
            # NASA GISTEMP verilerini indir
            url = "https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.txt"
            response = requests.get(url)
            response.raise_for_status()
            
            # Veriyi oku
            lines = response.text.split('\n')
            data = []
            
            # Başlık satırlarını atla
            start_line = 0
            for i, line in enumerate(lines):
                if line.startswith('Year'):
                    start_line = i + 1
                    break
            
            # Verileri işle
            for line in lines[start_line:]:
                if line.strip() and not line.startswith('Year'):
                    parts = line.split()
                    if len(parts) >= 2:
                        year = int(parts[0])
                        # Yıllık ortalama sıcaklık anomalisi
                        anomaly = float(parts[13]) if len(parts) > 13 else None
                        if anomaly is not None:
                            data.append({'year': year, 'Temperature_Anomaly': anomaly})
            
            # DataFrame oluştur
            df = pd.DataFrame(data)
            
            # Sonuçları kaydet
            output_file = os.path.join(self.data_dir, "nasa_temperature.csv")
            df.to_csv(output_file, index=False)
            
            logger.info(f"NASA sıcaklık verileri indirildi ve kaydedildi: {output_file}")
            return df
            
        except Exception as e:
            logger.error(f"NASA veri indirme hatası: {str(e)}")
            return None
            
    def download_noaa_co2(self):
        """
        NOAA'nın CO2 verilerini indirir
        """
        try:
            # NOAA CO2 verilerini indir
            url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_annmean_mlo.txt"
            response = requests.get(url)
            response.raise_for_status()
            
            # Veriyi oku
            lines = response.text.split('\n')
            data = []
            
            # Başlık satırlarını atla
            start_line = 0
            for i, line in enumerate(lines):
                if not line.startswith('#'):
                    start_line = i
                    break
            
            # Verileri işle
            for line in lines[start_line:]:
                if line.strip():
                    parts = line.split()
                    if len(parts) >= 2:
                        year = int(parts[0])
                        co2 = float(parts[1])
                        data.append({'year': year, 'CO2': co2})
            
            # DataFrame oluştur
            df = pd.DataFrame(data)
            
            # Sonuçları kaydet
            output_file = os.path.join(self.data_dir, "noaa_co2.csv")
            df.to_csv(output_file, index=False)
            
            logger.info(f"NOAA CO2 verileri indirildi ve kaydedildi: {output_file}")
            return df
            
        except Exception as e:
            logger.error(f"NOAA veri indirme hatası: {str(e)}")
            return None
            
    def process_all_data(self):
        """
        Tüm iklim verilerini indirir ve işler
        """
        # NASA sıcaklık verilerini indir
        temp_df = self.download_nasa_temperature()
        if temp_df is None:
            return None
            
        # NOAA CO2 verilerini indir
        co2_df = self.download_noaa_co2()
        if co2_df is None:
            return None
            
        return temp_df, co2_df

def main():
    processor = ClimateDataProcessor()
    temp_df, co2_df = processor.process_all_data()
    
    if temp_df is not None and co2_df is not None:
        print("\nNASA Sıcaklık Verileri Örneği:")
        print(temp_df.head())
        print("\nNOAA CO2 Verileri Örneği:")
        print(co2_df.head())

if __name__ == "__main__":
    main() 