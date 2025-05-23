import pandas as pd
import os
from datetime import datetime, timedelta
import requests
import zipfile
import io

def fetch_gdelt_data(start_date, end_date):
    """
    GDELT verilerini belirtilen tarih aralığı için indirir ve işler.
    
    Args:
        start_date (str): Başlangıç tarihi (YYYYMMDD formatında)
        end_date (str): Bitiş tarihi (YYYYMMDD formatında)
    """
    # GDELT sütun isimleri
    GDELT_COLUMNS = [
        'GlobalEventID', 'Day', 'MonthYear', 'Year', 'FractionDate',
        'Actor1Code', 'Actor1Name', 'Actor1CountryCode', 'Actor1KnownGroupCode',
        'Actor1EthnicCode', 'Actor1Religion1Code', 'Actor1Religion2Code',
        'Actor1Type1Code', 'Actor1Type2Code', 'Actor1Type3Code',
        'Actor2Code', 'Actor2Name', 'Actor2CountryCode', 'Actor2KnownGroupCode',
        'Actor2EthnicCode', 'Actor2Religion1Code', 'Actor2Religion2Code',
        'Actor2Type1Code', 'Actor2Type2Code', 'Actor2Type3Code',
        'IsRootEvent', 'EventCode', 'EventBaseCode', 'EventRootCode',
        'QuadClass', 'GoldsteinScale', 'NumMentions', 'NumSources',
        'NumArticles', 'AvgTone', 'Actor1Geo_Type', 'Actor1Geo_FullName',
        'Actor1Geo_CountryCode', 'Actor1Geo_ADM1Code', 'Actor1Geo_Lat',
        'Actor1Geo_Long', 'Actor1Geo_FeatureID', 'Actor2Geo_Type',
        'Actor2Geo_FullName', 'Actor2Geo_CountryCode', 'Actor2Geo_ADM1Code',
        'Actor2Geo_Lat', 'Actor2Geo_Long', 'Actor2Geo_FeatureID',
        'ActionGeo_Type', 'ActionGeo_FullName', 'ActionGeo_CountryCode',
        'ActionGeo_ADM1Code', 'ActionGeo_Lat', 'ActionGeo_Long',
        'ActionGeo_FeatureID', 'DATEADDED', 'SOURCEURL'
    ]
    
    # Çıktı dizini
    output_dir = 'data_storage/gdelt'
    os.makedirs(output_dir, exist_ok=True)
    
    # Tarih aralığını oluştur
    current_date = datetime.strptime(start_date, '%Y%m%d')
    end_date = datetime.strptime(end_date, '%Y%m%d')
    
    while current_date <= end_date:
        date_str = current_date.strftime('%Y%m%d')
        print(f'İndiriliyor: {date_str}')
        
        # GDELT URL'si
        url = f'http://data.gdeltproject.org/events/{date_str}.export.CSV.zip'
        
        try:
            # Veriyi indir
            response = requests.get(url)
            if response.status_code == 200:
                # ZIP dosyasını aç
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    # CSV dosyasını oku
                    with z.open(f'{date_str}.export.CSV') as f:
                        # Veri tiplerini belirterek oku
                        df = pd.read_csv(f, sep='\t', header=None, names=GDELT_COLUMNS,
                                       dtype={
                                           'GlobalEventID': str,
                                           'Day': str,  # Önce string olarak oku
                                           'MonthYear': int,
                                           'Year': int,
                                           'Actor1Code': str,
                                           'Actor1Name': str,
                                           'Actor2Code': str,
                                           'Actor2Name': str,
                                           'EventCode': str,
                                           'NumMentions': int,
                                           'NumSources': int,
                                           'NumArticles': int,
                                           'AvgTone': float,
                                           'SOURCEURL': str
                                       },
                                       low_memory=False)
                        
                        # Day sütununu düzelt (YYYYMMDD formatından gün değerine)
                        df['Day'] = df['Day'].astype(str).str[-2:].astype(int)
                        
                        # Tarih sütununu oluştur
                        df['date'] = pd.to_datetime({
                            'year': df['Year'],
                            'month': df['MonthYear'].astype(str).str[-2:].astype(int),
                            'day': df['Day']
                        }, utc=True)
                        
                        # Sadece iklim değişikliği ile ilgili haberleri filtrele
                        climate_keywords = ['climate change', 'global warming', 'greenhouse gas', 
                                         'carbon emission', 'renewable energy', 'sustainability']
                        mask = df['SOURCEURL'].str.contains('|'.join(climate_keywords), 
                                                         case=False, na=False)
                        df = df[mask]
                        
                        # Önemli sütunları seç
                        df = df[['date', 'Actor1Name', 'Actor2Name', 'EventCode', 
                               'NumMentions', 'NumSources', 'NumArticles', 'AvgTone', 
                               'SOURCEURL']]
                        
                        # Dosyaya kaydet
                        output_file = os.path.join(output_dir, f'{date_str}.csv')
                        df.to_csv(output_file, index=False)
                        print(f'Kaydedildi: {output_file}')
                        print(f'Bulunan haber sayısı: {len(df)}')
            
        except Exception as e:
            print(f'Hata ({date_str}): {str(e)}')
        
        # Bir sonraki güne geç
        current_date += timedelta(days=1)

if __name__ == '__main__':
    # Son 7 günün verilerini indir
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    fetch_gdelt_data(
        start_date.strftime('%Y%m%d'),
        end_date.strftime('%Y%m%d')
    ) 