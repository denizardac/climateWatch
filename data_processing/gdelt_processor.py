import requests
import pandas as pd
import logging
import os
from datetime import datetime, timedelta
import zipfile
import io

# Logging ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GDELTProcessor:
    def __init__(self):
        self.base_url = "http://data.gdeltproject.org/gdeltv2"
        self.data_dir = "data_storage/news"
        os.makedirs(self.data_dir, exist_ok=True)
        
    def download_daily_data(self, date):
        """
        Belirli bir tarih için GDELT verilerini indirir
        
        Args:
            date (datetime): İndirilecek verinin tarihi
        """
        try:
            # URL oluştur
            date_str = date.strftime('%Y%m%d')
            url = f"{self.base_url}/{date_str}000000.export.CSV.zip"
            
            # Veriyi indir
            response = requests.get(url)
            response.raise_for_status()
            
            # ZIP dosyasını aç
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                # CSV dosyasını oku
                with z.open(z.namelist()[0]) as f:
                    df = pd.read_csv(f, sep='\t', header=None)
            
            # Sütun isimlerini ayarla
            columns = [
                'GLOBALEVENTID', 'SQLDATE', 'MonthYear', 'Year', 'FractionDate',
                'Actor1Code', 'Actor1Name', 'Actor1CountryCode', 'Actor1KnownGroupCode',
                'Actor1EthnicCode', 'Actor1Religion1Code', 'Actor1Religion2Code',
                'Actor1Type1Code', 'Actor1Type2Code', 'Actor1Type3Code',
                'Actor2Code', 'Actor2Name', 'Actor2CountryCode', 'Actor2KnownGroupCode',
                'Actor2EthnicCode', 'Actor2Religion1Code', 'Actor2Religion2Code',
                'Actor2Type1Code', 'Actor2Type2Code', 'Actor2Type3Code',
                'IsRootEvent', 'EventCode', 'EventBaseCode', 'EventRootCode',
                'QuadClass', 'GoldsteinScale', 'NumMentions', 'NumSources',
                'NumArticles', 'AvgTone', 'Actor1Geo_Type', 'Actor1Geo_FullName',
                'Actor1Geo_CountryCode', 'Actor1Geo_ADM1Code', 'Actor1Geo_ADM2Code',
                'Actor1Geo_Lat', 'Actor1Geo_Long', 'Actor1Geo_FeatureID',
                'Actor2Geo_Type', 'Actor2Geo_FullName', 'Actor2Geo_CountryCode',
                'Actor2Geo_ADM1Code', 'Actor2Geo_ADM2Code', 'Actor2Geo_Lat',
                'Actor2Geo_Long', 'Actor2Geo_FeatureID', 'ActionGeo_Type',
                'ActionGeo_FullName', 'ActionGeo_CountryCode', 'ActionGeo_ADM1Code',
                'ActionGeo_ADM2Code', 'ActionGeo_Lat', 'ActionGeo_Long',
                'ActionGeo_FeatureID', 'DATEADDED', 'SOURCEURL'
            ]
            df.columns = columns
            
            # İklim değişikliği ile ilgili haberleri filtrele
            climate_keywords = [
                'climate change', 'global warming', 'greenhouse gas',
                'carbon emission', 'renewable energy', 'sustainability',
                'environmental protection', 'climate action', 'carbon footprint'
            ]
            
            # Haberleri filtrele
            climate_news = df[
                df['SOURCEURL'].str.contains('|'.join(climate_keywords), case=False, na=False)
            ]
            
            # Günlük istatistikleri hesapla
            daily_stats = pd.DataFrame({
                'date': [date],
                'article_count': [len(climate_news)],
                'avg_tone': [climate_news['AvgTone'].mean() if not climate_news.empty else 0]
            })
            
            # Sonuçları kaydet
            output_file = os.path.join(self.data_dir, f"gdelt_{date_str}.csv")
            daily_stats.to_csv(output_file, index=False)
            
            logger.info(f"GDELT verileri indirildi ve işlendi: {output_file}")
            return daily_stats
            
        except Exception as e:
            logger.error(f"Veri indirme hatası: {str(e)}")
            return None
            
    def process_historical_data(self, start_date, end_date):
        """
        Belirli bir tarih aralığı için GDELT verilerini indirir ve işler
        
        Args:
            start_date (datetime): Başlangıç tarihi
            end_date (datetime): Bitiş tarihi
        """
        all_stats = []
        current_date = start_date
        
        while current_date <= end_date:
            daily_stats = self.download_daily_data(current_date)
            if daily_stats is not None:
                all_stats.append(daily_stats)
            current_date += timedelta(days=1)
            
        if all_stats:
            # Tüm günlük istatistikleri birleştir
            combined_stats = pd.concat(all_stats, ignore_index=True)
            
            # Sonuçları kaydet
            output_file = os.path.join(self.data_dir, "processed_news.csv")
            combined_stats.to_csv(output_file, index=False)
            
            logger.info(f"Tüm GDELT verileri işlendi ve kaydedildi: {output_file}")
            return combined_stats
            
        return None

def main():
    # Processor'ı başlat
    processor = GDELTProcessor()
    
    # Son 5 yıllık veriyi indir
    end_date = datetime.now()
    start_date = end_date - timedelta(days=5*365)
    
    # Verileri işle
    stats_df = processor.process_historical_data(start_date, end_date)
    if stats_df is not None:
        print("\nİşlenmiş haber verisi örneği:")
        print(stats_df.head())

if __name__ == "__main__":
    main() 