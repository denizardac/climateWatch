import pandas as pd
import os
from collections import Counter

# Dosya yolları
dir_processed = 'data_storage/processed'
climate_file = os.path.join(dir_processed, 'processed_climate.csv')
trends_file = os.path.join(dir_processed, 'processed_trends.csv')
gdelt_file = os.path.join(dir_processed, 'processed_gdelt.csv')

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
    'ActionGeo_FeatureID', 'date', 'url'
]

print('--- İKLİM VERİSİ ---')
climate_df = pd.read_csv(climate_file)
print('Toplam kayıt:', len(climate_df))
print('Yıllar:', climate_df['Year'].min(), '-', climate_df['Year'].max())
print('Ortalama sıcaklık:', climate_df['Mean'].mean())
print('Min sıcaklık:', climate_df['Mean'].min())
print('Max sıcaklık:', climate_df['Mean'].max())
print('Kaynaklar:', climate_df['Source'].unique())
print(climate_df.head())

print('\n--- GOOGLE TRENDS VERİSİ ---')
trends_df = pd.read_csv(trends_file)
print('Toplam kayıt:', len(trends_df))
print('Tarih aralığı:', trends_df['date'].min(), '-', trends_df['date'].max())
print('Ortalama trend puanı:', trends_df['climate change'].mean())
print('Min trend puanı:', trends_df['climate change'].min())
print('Max trend puanı:', trends_df['climate change'].max())
print(trends_df.head())

print('\n--- GDELT VERİSİ ---')
# GDELT çok büyük, sadece ilk 10000 satırı oku
try:
    gdelt_df = pd.read_csv(gdelt_file, nrows=10000)
    print('Mevcut sütun sayısı:', len(gdelt_df.columns))
    print('Mevcut sütunlar:', gdelt_df.columns.tolist())
    print('\nİlk 5 satır:')
    print(gdelt_df.head())
except Exception as e:
    print('GDELT verisi okunurken hata:', e) 