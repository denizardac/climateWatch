import os
import pandas as pd
import logging
from datetime import datetime

def clean_gdelt(input_dir='data_storage/gdelt', output_file='data_storage/processed/cleaned_gdelt.csv', log_path='logs/clean_gdelt.log'):
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    
    # GDELT sütun tipleri
    dtype_dict = {
        'GLOBALEVENTID': str,
        'SQLDATE': str,
        'MonthYear': str,
        'Year': str,
        'FractionDate': float,
        'Actor1Code': str,
        'Actor1Name': str,
        'Actor1CountryCode': str,
        'Actor1KnownGroupCode': str,
        'Actor1EthnicCode': str,
        'Actor1Religion1Code': str,
        'Actor1Religion2Code': str,
        'Actor1Type1Code': str,
        'Actor1Type2Code': str,
        'Actor1Type3Code': str,
        'Actor2Code': str,
        'Actor2Name': str,
        'Actor2CountryCode': str,
        'Actor2KnownGroupCode': str,
        'Actor2EthnicCode': str,
        'Actor2Religion1Code': str,
        'Actor2Religion2Code': str,
        'Actor2Type1Code': str,
        'Actor2Type2Code': str,
        'Actor2Type3Code': str,
        'IsRootEvent': int,
        'EventCode': str,
        'EventBaseCode': str,
        'EventRootCode': str,
        'QuadClass': int,
        'GoldsteinScale': float,
        'NumMentions': int,
        'NumSources': int,
        'NumArticles': int,
        'AvgTone': float,
        'Actor1Geo_Type': int,
        'Actor1Geo_FullName': str,
        'Actor1Geo_CountryCode': str,
        'Actor1Geo_ADM1Code': str,
        'Actor1Geo_ADM2Code': str,
        'Actor1Geo_Lat': float,
        'Actor1Geo_Long': float,
        'Actor1Geo_FeatureID': str,
        'Actor2Geo_Type': int,
        'Actor2Geo_FullName': str,
        'Actor2Geo_CountryCode': str,
        'Actor2Geo_ADM1Code': str,
        'Actor2Geo_ADM2Code': str,
        'Actor2Geo_Lat': float,
        'Actor2Geo_Long': float,
        'Actor2Geo_FeatureID': str,
        'ActionGeo_Type': int,
        'ActionGeo_FullName': str,
        'ActionGeo_CountryCode': str,
        'ActionGeo_ADM1Code': str,
        'ActionGeo_ADM2Code': str,
        'ActionGeo_Lat': float,
        'ActionGeo_Long': float,
        'ActionGeo_FeatureID': str,
        'DATEADDED': str,
        'SOURCEURL': str
    }

    try:
        all_dfs = []
        total_files = len([f for f in os.listdir(input_dir) if f.endswith('.csv')])
        processed_files = 0
        
        for fname in os.listdir(input_dir):
            if fname.endswith('.csv'):
                fpath = os.path.join(input_dir, fname)
                try:
                    # CSV'yi okurken veri tiplerini belirt ve düşük bellek modunu kapat
                    df = pd.read_csv(fpath, dtype=dtype_dict, low_memory=False)
                    
                    # Tarih sütunlarını düzelt
                    date_columns = ['SQLDATE', 'DATEADDED']
                    for col in date_columns:
                        if col in df.columns:
                            df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')
                    
                    # Sayısal sütunlardaki NaN değerleri 0 ile doldur
                    numeric_columns = ['NumMentions', 'NumSources', 'NumArticles', 'AvgTone', 'GoldsteinScale']
                    for col in numeric_columns:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    
                    # Koordinat sütunlarını düzelt
                    coord_columns = ['Actor1Geo_Lat', 'Actor1Geo_Long', 'Actor2Geo_Lat', 'Actor2Geo_Long', 
                                  'ActionGeo_Lat', 'ActionGeo_Long']
                    for col in coord_columns:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # Metin sütunlarındaki NaN değerleri boş string ile doldur
                    text_columns = ['Actor1Name', 'Actor2Name', 'Actor1Geo_FullName', 'Actor2Geo_FullName', 
                                  'ActionGeo_FullName', 'SOURCEURL']
                    for col in text_columns:
                        if col in df.columns:
                            df[col] = df[col].fillna('')
                    
                    # Duplikasyonları kaldır
                    df = df.drop_duplicates(subset=['GLOBALEVENTID'])
                    
                    all_dfs.append(df)
                    processed_files += 1
                    logging.info(f"Okundu ve temizlendi: {fpath} | Satır: {len(df)} | İlerleme: {processed_files}/{total_files}")
                    
                except Exception as e:
                    logging.error(f"Dosya okunamadı/temizlenemedi: {fpath} | Hata: {e}")
                    continue
        
        if all_dfs:
            cleaned = pd.concat(all_dfs, ignore_index=True)
            
            # Son temizlik adımları
            cleaned = cleaned.dropna(subset=['GLOBALEVENTID', 'SQLDATE'])  # Temel sütunlardaki NaN'ları kaldır
            cleaned = cleaned.sort_values('SQLDATE')  # Tarihe göre sırala
            
            # Çıktıyı kaydet
            cleaned.to_csv(output_file, index=False)
            size = os.path.getsize(output_file)
            with open(output_file, 'r', encoding='utf-8', errors='ignore') as f:
                lines = sum(1 for _ in f)
            
            logging.info(f"Tüm GDELT verisi temizlendi ve kaydedildi: {output_file} | Boyut: {size} bytes | Satır: {lines}")
            logging.info(f"Toplam işlenen dosya: {processed_files}/{total_files}")
        else:
            logging.warning("Hiçbir GDELT CSV dosyası işlenemedi!")
            
    except Exception as e:
        logging.error(f"GDELT temizlik pipeline hatası: {e}")

if __name__ == '__main__':
    clean_gdelt() 