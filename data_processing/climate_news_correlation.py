import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import requests
from io import StringIO
import os
from pytrends.request import TrendReq
import time
from scipy import stats
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from wordcloud import WordCloud
from collections import Counter
import re

# NLTK gerekli dosyaları indir
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')
try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except LookupError:
    nltk.download('vader_lexicon')

def metin_temizle(metin):
    """Metni temizler ve tokenize eder."""
    if not isinstance(metin, str):
        return []
    
    # Küçük harfe çevir
    metin = metin.lower()
    
    # Noktalama işaretlerini kaldır
    metin = re.sub(r'[^\w\s]', '', metin)
    
    # Tokenize et
    tokens = word_tokenize(metin)
    
    # Stop words'leri kaldır
    stop_words = set(stopwords.words('english'))
    tokens = [token for token in tokens if token not in stop_words]
    
    # Lemmatization
    lemmatizer = WordNetLemmatizer()
    tokens = [lemmatizer.lemmatize(token) for token in tokens]
    
    return tokens

def duygu_analizi_yap(metin):
    """Metnin duygu analizini yapar."""
    if not isinstance(metin, str):
        return {'neg': 0, 'neu': 0, 'pos': 0, 'compound': 0}
    
    sia = SentimentIntensityAnalyzer()
    return sia.polarity_scores(metin)

def kelime_bulutu_olustur(metinler, baslik):
    """Verilen metinlerden kelime bulutu oluşturur."""
    if not metinler:
        return None
        
    # Tüm metinleri birleştir
    tum_metin = ' '.join([str(m) for m in metinler if isinstance(m, str)])
    
    # Kelime bulutu oluştur
    wordcloud = WordCloud(
        width=800,
        height=400,
        background_color='white',
        max_words=100
    ).generate(tum_metin)
    
    # Görselleştir
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.title(baslik)
    
    # Kaydet
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(os.path.join(output_dir, f'kelime_bulutu_{baslik}.png'))
    plt.close()

def haber_analizi_yap(haber_verileri):
    """Haber verilerini analiz eder ve görselleştirir."""
    if haber_verileri is None or len(haber_verileri) == 0:
        print("Analiz edilecek haber verisi bulunamadı.")
        return None
        
    # Duygu analizi
    duygu_skorlari = []
    for _, row in haber_verileri.iterrows():
        if 'metin' in row:
            skor = duygu_analizi_yap(row['metin'])
            duygu_skorlari.append(skor)
    
    if duygu_skorlari:
        duygu_df = pd.DataFrame(duygu_skorlari)
        print("\nDuygu Analizi Sonuçları:")
        print(duygu_df.describe())
        
        # Duygu skorlarını görselleştir
        plt.figure(figsize=(10, 6))
        duygu_df[['neg', 'neu', 'pos']].plot(kind='bar', stacked=True)
        plt.title('Haber Duygu Analizi Dağılımı')
        plt.xlabel('Haber İndeksi')
        plt.ylabel('Duygu Skoru')
        
        output_dir = 'output'
        os.makedirs(output_dir, exist_ok=True)
        plt.savefig(os.path.join(output_dir, 'duygu_analizi.png'))
        plt.close()
    
    # Kelime bulutu
    if 'metin' in haber_verileri.columns:
        kelime_bulutu_olustur(haber_verileri['metin'], 'haber_kelimeleri')
    
    return duygu_df if duygu_skorlari else None

def fetch_google_trends(city_name, start_date, end_date):
    """
    Google Trends verilerini indirir.
    
    Args:
        city_name (str): Şehir adı
        start_date (str): Başlangıç tarihi
        end_date (str): Bitiş tarihi
    """
    try:
        # Google Trends bağlantısı
        pytrends = TrendReq(hl='en-US', tz=360)
        
        # Arama terimleri
        search_terms = [
            f"{city_name} weather",
            f"{city_name} climate",
            f"{city_name} temperature",
            f"{city_name} news"
        ]
        
        # Tarih aralığını ayarla
        timeframe = f"{start_date} {end_date}"
        
        # Arama yap
        pytrends.build_payload(
            search_terms,
            cat=0,
            timeframe=timeframe,
            geo='US'
        )
        
        # Verileri al
        trends_data = pytrends.interest_over_time()
        
        if trends_data.empty:
            print(f"Uyarı: {city_name} için Google Trends verisi bulunamadı!")
            return None
        
        # Veriyi düzenle
        trends_data = trends_data.reset_index()
        trends_data = trends_data.rename(columns={'date': 'date'})
        
        # Veri kalitesi kontrolü
        print(f"\nGoogle Trends Veri Kalitesi Kontrolü - {city_name}:")
        print(f"Toplam gün sayısı: {len(trends_data)}")
        print(f"Benzersiz tarih sayısı: {trends_data['date'].nunique()}")
        
        return trends_data
        
    except Exception as e:
        print(f"Hata: Google Trends verisi alınamadı - {e}")
        return None

def fetch_climate_data(city_code, start_date, end_date):
    """NOAA'dan iklim verilerini çek ve işle"""
    try:
        # Alternatif istasyon kodları
        backup_stations = {
            'USW00094728': ['USC00280907', 'USC00280908'],  # New York
            'USW00023174': ['USC00047767', 'USC00047768'],  # Los Angeles
            'USW00094846': ['USC00115097', 'USC00115098'],  # Chicago
            'USW00012918': ['USC00414333', 'USC00414334']   # Houston
        }
        
        # NOAA API URL'si
        base_url = "https://www.ncei.noaa.gov/access/services/data/v1"
        
        # Her istasyon için veri çekmeyi dene
        all_data = []
        stations_tried = []
        
        # Ana istasyon kodu ile başla
        stations_to_try = [city_code] + backup_stations.get(city_code, [])
        
        for station in stations_to_try:
            # Bu istasyon için daha önce deneme yapılmışsa atla
            if station in stations_tried:
                continue
                
            stations_tried.append(station)
                
            try:
                # Veri çekme parametreleri
                params = {
                    'dataset': 'daily-summaries',
                    'stations': station,
                    'startDate': start_date.strftime('%Y-%m-%d'),
                    'endDate': end_date.strftime('%Y-%m-%d'),
                    'format': 'json',
                    'units': 'metric'
                }
                
                # API'den veri çek
                response = requests.get(base_url, params=params, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    if data:
                        df = pd.DataFrame(data)
                        if not df.empty:
                            df['STATION'] = station  # İstasyon kodunu sabit olarak ekle
                            all_data.append(df)
                            print(f"İstasyon {station} için veri bulundu - {len(df)} kayıt")
                else:
                    print(f"Uyarı: İstasyon {station} için veri alınamadı - {response.status_code}")
                    
            except Exception as e:
                print(f"Uyarı: İstasyon {station} için hata - {str(e)}")
                continue
        
        if not all_data:
            print("Hata: Hiçbir istasyondan veri alınamadı!")
            return None
            
        # Tüm verileri birleştir
        combined_data = pd.concat(all_data, ignore_index=True)
        
        # Tarih sütununu düzenle
        combined_data['DATE'] = pd.to_datetime(combined_data['DATE'])
        
        # Sayısal sütunları düzenle - hata kontrolü ile
        numeric_columns = ['TMAX', 'TMIN', 'PRCP', 'SNOW', 'SNWD', 'AWND']
        for col in numeric_columns:
            if col in combined_data.columns:
                try:
                    combined_data[col] = pd.to_numeric(combined_data[col], errors='coerce')
                except Exception as e:
                    print(f"Uyarı: {col} sütunu sayısal dönüşümde hata - {str(e)}")
                    combined_data[col] = None
        
        # Ortalama sıcaklığı hesapla
        if 'TMAX' in combined_data.columns and 'TMIN' in combined_data.columns:
            combined_data['temperature'] = (
                pd.to_numeric(combined_data['TMAX'], errors='coerce') + 
                pd.to_numeric(combined_data['TMIN'], errors='coerce')
            ) / 2
        else:
            print("Uyarı: TMAX veya TMIN sütunları bulunamadı, ortalama sıcaklık hesaplanamadı")
            combined_data['temperature'] = None
        
        # Sütun isimlerini düzenle
        column_mapping = {
            'DATE': 'date',
            'TMAX': 'max_temperature',
            'TMIN': 'min_temperature',
            'PRCP': 'precipitation',
            'SNOW': 'snow',
            'SNWD': 'snow_depth',
            'AWND': 'wind_speed'
        }
        
        # Mevcut sütunları kontrol et ve yeniden adlandır
        climate_data = combined_data[['DATE']].copy()
        climate_data = climate_data.rename(columns={'DATE': 'date'})
        
        for old_col, new_col in column_mapping.items():
            if old_col != 'DATE' and old_col in combined_data.columns:
                climate_data[new_col] = combined_data[old_col]
        
        # temperature sütunu zaten oluşturuldu
        if 'temperature' in combined_data.columns:
            climate_data['temperature'] = combined_data['temperature']
        
        # Veri kalitesi kontrolü
        print("\nVeri Kalitesi Kontrolü:")
        print(f"Toplam gün sayısı: {len(climate_data)}")
        print(f"Benzersiz tarih sayısı: {climate_data['date'].nunique()}")
        print(f"Tarih aralığı: {climate_data['date'].min()} - {climate_data['date'].max()}")
        expected_days = (end_date - start_date).days + 1
        print(f"Beklenen gün sayısı: {expected_days}")
        print(f"Eksik tarih sayısı: {expected_days - climate_data['date'].nunique()}")
        
        # Eksik değerleri doldurmak için date sütununu index olarak ayarla
        climate_data = climate_data.set_index('date')
        
        for col in climate_data.columns:
            missing = climate_data[col].isna().sum()
            if missing > 0:
                print(f"Uyarı: {col} için {missing} eksik değer var ({(missing/len(climate_data)*100):.1f}%)")
                if missing > len(climate_data) * 0.5:
                    print(f"Uyarı: {col} için çok fazla eksik veri var, bu sütun kullanılmayacak")
                    climate_data = climate_data.drop(columns=[col])
                else:
                    # Eksik değerleri doldur
                    if col in ['temperature', 'max_temperature', 'min_temperature']:
                        # Sıcaklık değerleri için interpolasyon
                        climate_data[col] = climate_data[col].interpolate(method='time')
                    elif col in ['precipitation', 'snow', 'snow_depth']:
                        # Yağış ve kar değerleri için 0 ile doldur
                        climate_data[col] = climate_data[col].fillna(0)
                    elif col == 'wind_speed':
                        # Rüzgar hızı için hareketli ortalama
                        climate_data[col] = climate_data[col].fillna(climate_data[col].rolling(window=3, min_periods=1).mean())
        
        # index'i sıfırla
        climate_data = climate_data.reset_index()
        
        # Son veri kalitesi kontrolü
        print("\nSon Veri Kalitesi Kontrolü:")
        print(f"Toplam gün sayısı: {len(climate_data)}")
        print(f"Benzersiz tarih sayısı: {climate_data['date'].nunique()}")
        
        # Son kontrol - tüm gerekli sütunların mevcut olduğundan emin olalım
        required_columns = ['date', 'temperature']
        missing_columns = [col for col in required_columns if col not in climate_data.columns]
        
        if missing_columns:
            print(f"Hata: Gerekli sütunlar eksik: {missing_columns}")
            return None
        
        # İstatistikler
        for col in climate_data.columns:
            if col != 'date':
                nan_count = climate_data[col].isna().sum()
                print(f"{col} için eksik değer sayısı: {nan_count} ({nan_count/len(climate_data)*100:.1f}%)")
                if nan_count < len(climate_data):  # Tüm değerler eksik değilse istatistikler yazdır
                    print(f"{col} için ortalama değer: {climate_data[col].mean():.1f}")
                    print(f"{col} için minimum değer: {climate_data[col].min():.1f}")
                    print(f"{col} için maksimum değer: {climate_data[col].max():.1f}")
        
        return climate_data
        
    except Exception as e:
        print(f"Hata: İklim verisi alınamadı - {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def gdelt_dosyalarindan_populer_ulkeleri_bul(gdelt_klasoru, n=5):
    """GDELT CSV dosyalarından en çok adı geçen N ülkeyi bulur."""
    ulke_sayilari = pd.Series(dtype='int64')
    ulke_kodu_sutunlari = ['Actor1CountryCode', 'Actor2CountryCode', 'ActionGeo_CountryCode']
    
    print(f"\n{gdelt_klasoru} içindeki GDELT dosyalarından en çok geçen ülke kodları taranıyor...")
    
    for dosya_adi in os.listdir(gdelt_klasoru):
        if dosya_adi.endswith('.csv'):
            try:
                dosya_yolu = os.path.join(gdelt_klasoru, dosya_adi)
                
                # Önce dosyanın başlık satırını oku
                try:
                    baslik = pd.read_csv(dosya_yolu, nrows=0, sep=',')
                    print(f"\nDosya: {dosya_adi}")
                    print(f"Gerçek sütun sayısı: {len(baslik.columns)}")
                    
                    # Mevcut sütunları kontrol et
                    mevcut_sutunlar = baslik.columns.tolist()
                    kullanilabilir_ulke_sutunlari = [sutun for sutun in ulke_kodu_sutunlari if sutun in mevcut_sutunlar]
                    
                    if not kullanilabilir_ulke_sutunlari:
                        print(f"Uyarı: {dosya_adi} dosyasında ülke kodu sütunları bulunamadı.")
                        continue
                    
                    # Sadece mevcut sütunları oku
                    df = pd.read_csv(
                        dosya_yolu,
                        sep=',',
                        usecols=kullanilabilir_ulke_sutunlari,
                        low_memory=False,
                        on_bad_lines='skip'
                    )
                    
                    # Her ülke kodu sütunu için sayım yap
                    for sutun in kullanilabilir_ulke_sutunlari:
                        if sutun in df.columns:
                            # Geçerli ülke kodlarını temizle ve say
                            gecerli_kodlar = df[sutun].dropna()
                            gecerli_kodlar = gecerli_kodlar[gecerli_kodlar.str.len() == 2]  # Sadece 2 harfli kodlar
                            ulke_sayilari = pd.concat([ulke_sayilari, gecerli_kodlar.value_counts()])
                    
                    print(f"İşlenen kayıt sayısı: {len(df)}")
                    
                except Exception as e:
                    print(f"Hata: {dosya_adi} dosyası okunamadı - {str(e)}")
                    continue
                    
            except Exception as e:
                print(f"Hata: {dosya_adi} dosyası işlenirken hata oluştu - {str(e)}")
                continue
    
    if ulke_sayilari.empty:
        print("\nUyarı: GDELT dosyalarından hiç ülke kodu bulunamadı.")
        return []

    # En çok geçen N ülkeyi bul
    en_populer_ulkeler = ulke_sayilari.groupby(ulke_sayilari.index).sum().nlargest(n)
    
    print(f"\nEn çok adı geçen ilk {n} ülke kodu:")
    for ulke_kodu, sayi in en_populer_ulkeler.items():
        print(f"- {ulke_kodu}: {sayi} kez")
    
    return en_populer_ulkeler.index.tolist()

def haber_verilerini_cek(hedef_ulke_kodu, baslangic_tarihi, bitis_tarihi):
    """GDELT'den haber verilerini çek ve ülke koduna göre filtrele"""
    try:
        # GDELT sütun isimleri - sadece ihtiyacımız olanlar
        gerekli_sutunlar = [
            'Day', 'Actor1CountryCode', 'Actor2CountryCode', 'ActionGeo_CountryCode',
            'NumArticles', 'NumSources', 'AvgTone', 'Actor1Name', 'Actor2Name',
            'EventCode', 'EventBaseCode', 'EventRootCode', 'QuadClass',
            'GoldsteinScale', 'MentionDocTone', 'MentionDocLen', 'Confidence',
            'MentionSourceName', 'MentionIdentifier', 'SentenceID', 'Actor1Geo_Type',
            'Actor1Geo_FullName', 'Actor1Geo_CountryCode', 'Actor1Geo_ADM1Code',
            'Actor1Geo_Lat', 'Actor1Geo_Long', 'Actor2Geo_Type', 'Actor2Geo_FullName',
            'Actor2Geo_CountryCode', 'Actor2Geo_ADM1Code', 'Actor2Geo_Lat',
            'Actor2Geo_Long', 'ActionGeo_Type', 'ActionGeo_FullName',
            'ActionGeo_CountryCode', 'ActionGeo_ADM1Code', 'ActionGeo_Lat',
            'ActionGeo_Long', 'DATEADDED', 'SOURCEURL'
        ]

        # Tüm CSV dosyalarını oku
        tum_veriler = []
        gdelt_klasoru = 'data_storage/gdelt'
        
        for dosya_adi in os.listdir(gdelt_klasoru):
            if dosya_adi.endswith('.csv'):
                try:
                    dosya_yolu = os.path.join(gdelt_klasoru, dosya_adi)
                    
                    # Önce dosyayı okuyabilir miyiz kontrol et
                    try:
                        # Önce başlık satırını oku
                        baslik = pd.read_csv(dosya_yolu, nrows=0, sep=',')
                        print(f"\nDosya: {dosya_adi}")
                        print(f"Sütun sayısı: {len(baslik.columns)}")
                        
                        # Mevcut sütunları kontrol et
                        mevcut_sutunlar = baslik.columns.tolist()
                        kullanilabilir_sutunlar = [sutun for sutun in gerekli_sutunlar if sutun in mevcut_sutunlar]
                        
                        if not kullanilabilir_sutunlar:
                            print(f"Uyarı: {dosya_adi} dosyasında gerekli sütunlar bulunamadı.")
                            continue
                        
                        # Sadece mevcut sütunları oku
                        df = pd.read_csv(
                            dosya_yolu,
                            sep=',',
                            usecols=kullanilabilir_sutunlar,
                            low_memory=False,
                            on_bad_lines='skip'
                        )
                        
                        if df.empty:
                            print(f"Uyarı: {dosya_adi} dosyası boş veya geçersiz veri içeriyor.")
                            continue
                            
                        # Tarih sütunu oluştur
                        if 'Day' not in df.columns:
                            print(f"Hata: {dosya_adi} dosyasında 'Day' sütunu bulunamadı.")
                            continue
                        
                        df.dropna(subset=['Day'], inplace=True)
                        df['date'] = pd.to_datetime(df['Day'].astype(str), format='%Y%m%d', errors='coerce')
                        df.dropna(subset=['date'], inplace=True)
                        
                        if df.empty:
                            print(f"Uyarı: {dosya_adi} dosyasında geçerli tarih içeren veri bulunamadı.")
                            continue
                        
                        tum_veriler.append(df)
                        print(f"Okunan kayıt sayısı: {len(df)}")
                        
                    except Exception as e:
                        print(f"Hata: {dosya_adi} dosyası okunamadı - {str(e)}")
                        continue
                        
                except Exception as e:
                    print(f"Hata: {dosya_adi} dosyası işlenirken hata oluştu - {str(e)}")
                    continue

        if not tum_veriler:
            print("Uyarı: Hiç GDELT verisi bulunamadı!")
            return None

        birlesik_veri = pd.concat(tum_veriler, ignore_index=True)
        
        # Tarih filtreleme
        baslangic_tarihi_ts = pd.Timestamp(baslangic_tarihi)
        bitis_tarihi_ts = pd.Timestamp(bitis_tarihi)
        tarih_maskesi = (birlesik_veri['date'] >= baslangic_tarihi_ts) & (birlesik_veri['date'] <= bitis_tarihi_ts)
        birlesik_veri = birlesik_veri[tarih_maskesi]
        
        if birlesik_veri.empty:
            print(f"Belirtilen tarih aralığında ({baslangic_tarihi_ts.date()} - {bitis_tarihi_ts.date()}) GDELT verisi bulunamadı.")
            return None
            
        # Ülke bazlı filtreleme
        ulke_sutunlari = ['Actor1CountryCode', 'Actor2CountryCode', 'ActionGeo_CountryCode']
        kullanilabilir_ulke_sutunlari = [sutun for sutun in ulke_sutunlari if sutun in birlesik_veri.columns]

        if not kullanilabilir_ulke_sutunlari:
            print(f"Uyarı: {hedef_ulke_kodu} için ülke filtrelemesi yapılamadı - gerekli ülke sütunları bulunamadı.")
            return None
            
        ulke_maskesi = pd.Series([False] * len(birlesik_veri))
        for sutun in kullanilabilir_ulke_sutunlari:
            ulke_maskesi |= (birlesik_veri[sutun].astype(str).str.upper() == hedef_ulke_kodu.upper())
        
        filtrelenmis_veri = birlesik_veri[ulke_maskesi]
        
        print(f"\n{hedef_ulke_kodu} için haber filtreleme sonuçları:")
        print(f"Toplam haber sayısı: {len(filtrelenmis_veri)}")
        
        if len(filtrelenmis_veri) == 0:
            print(f"Uyarı: {hedef_ulke_kodu} için belirtilen kriterlerde haber bulunamadı!")
            return None
            
        # Günlük istatistikler
        gunluk_istatistikler = filtrelenmis_veri.groupby(filtrelenmis_veri['date'].dt.date).agg({
            'NumArticles': 'sum',
            'NumSources': 'sum',
            'AvgTone': 'mean',
            'GoldsteinScale': 'mean',
            'Confidence': 'mean'
        }).reset_index()
        
        gunluk_istatistikler['date'] = pd.to_datetime(gunluk_istatistikler['date'])
        gunluk_istatistikler['haber_sayisi'] = gunluk_istatistikler.index.map(
            filtrelenmis_veri.groupby(filtrelenmis_veri['date'].dt.date).size().to_dict()
        ).fillna(0).astype(int)
        
        # Olay türlerine göre analiz
        if 'EventCode' in filtrelenmis_veri.columns:
            olay_turleri = filtrelenmis_veri['EventCode'].value_counts()
            print("\nEn sık karşılaşılan olay türleri:")
            print(olay_turleri.head())
            
            # Olay türlerini görselleştir
            plt.figure(figsize=(12, 6))
            olay_turleri.head(10).plot(kind='bar')
            plt.title(f'{hedef_ulke_kodu} - En Sık Karşılaşılan Olay Türleri')
            plt.xlabel('Olay Kodu')
            plt.ylabel('Frekans')
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            output_dir = 'output'
            os.makedirs(output_dir, exist_ok=True)
            plt.savefig(os.path.join(output_dir, f'olay_turleri_{hedef_ulke_kodu}.png'))
            plt.close()
        
        # Coğrafi analiz
        if all(col in filtrelenmis_veri.columns for col in ['ActionGeo_Lat', 'ActionGeo_Long']):
            print("\nCoğrafi dağılım analizi yapılıyor...")
            # Coğrafi verileri temizle
            geo_veri = filtrelenmis_veri.dropna(subset=['ActionGeo_Lat', 'ActionGeo_Long'])
            if not geo_veri.empty:
                # Basit bir scatter plot
                plt.figure(figsize=(10, 6))
                plt.scatter(geo_veri['ActionGeo_Long'], geo_veri['ActionGeo_Lat'], 
                          alpha=0.5, c=geo_veri['AvgTone'], cmap='coolwarm')
                plt.colorbar(label='Ortalama Ton')
                plt.title(f'{hedef_ulke_kodu} - Haber Lokasyonları ve Tonları')
                plt.xlabel('Boylam')
                plt.ylabel('Enlem')
                plt.savefig(os.path.join(output_dir, f'cografi_dagilim_{hedef_ulke_kodu}.png'))
                plt.close()
        
        # Zaman serisi analizi
        print("\nZaman serisi analizi yapılıyor...")
        zaman_serisi = gunluk_istatistikler.set_index('date')
        
        # Hareketli ortalama hesapla
        zaman_serisi['haber_sayisi_ma7'] = zaman_serisi['haber_sayisi'].rolling(window=7).mean()
        zaman_serisi['AvgTone_ma7'] = zaman_serisi['AvgTone'].rolling(window=7).mean()
        
        # Zaman serisi grafiği
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
        
        # Haber sayısı
        ax1.plot(zaman_serisi.index, zaman_serisi['haber_sayisi'], label='Günlük Haber Sayısı')
        ax1.plot(zaman_serisi.index, zaman_serisi['haber_sayisi_ma7'], 
                label='7 Günlük Hareketli Ortalama', linewidth=2)
        ax1.set_title(f'{hedef_ulke_kodu} - Günlük Haber Sayısı')
        ax1.legend()
        
        # Haber tonu
        ax2.plot(zaman_serisi.index, zaman_serisi['AvgTone'], label='Günlük Ortalama Ton')
        ax2.plot(zaman_serisi.index, zaman_serisi['AvgTone_ma7'], 
                label='7 Günlük Hareketli Ortalama', linewidth=2)
        ax2.set_title(f'{hedef_ulke_kodu} - Haber Tonu')
        ax2.legend()
        
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f'zaman_serisi_{hedef_ulke_kodu}.png'))
        plt.close()
        
        print("\nVeri Özeti:")
        if not gunluk_istatistikler.empty:
            print(f"Tarih aralığı: {gunluk_istatistikler['date'].min().date()} - {gunluk_istatistikler['date'].max().date()}")
            print("\nGünlük haber sayıları:")
            print(gunluk_istatistikler['haber_sayisi'].describe())
            print("\nGünlük makale sayıları:")
            print(gunluk_istatistikler['NumArticles'].describe())
            print("\nGünlük kaynak sayıları:")
            print(gunluk_istatistikler['NumSources'].describe())
            print("\nGünlük haber tonu:")
            print(gunluk_istatistikler['AvgTone'].describe())
            print("\nGoldstein ölçeği:")
            print(gunluk_istatistikler['GoldsteinScale'].describe())
        else:
            print("Filtreleme sonrası istatistik çıkarılacak haber verisi bulunamadı.")
            
        return gunluk_istatistikler
        
    except Exception as e:
        print(f"Hata: Haber verisi ({hedef_ulke_kodu} için) alınamadı - {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def analyze_correlation(climate_data, news_data):
    """İklim ve haber verilerini birleştir ve korelasyon analizi yap"""
    try:
        if climate_data is None or news_data is None:
            print("Hata: İklim veya haber verisi bulunamadı!")
            return None
            
        print("\nVeri Birleştirme:")
        print(f"İklim veri sayısı: {len(climate_data)}")
        print(f"Haber veri sayısı: {len(news_data)}")
        
        # İki veri setini tarih sütununa göre birleştir
        merged_data = pd.merge(climate_data, news_data, on='date', how='inner')
        
        print(f"Birleştirilen veri sayısı: {len(merged_data)}")
        print(f"Tarih aralığı: {merged_data['date'].min()} - {merged_data['date'].max()}")
        
        if len(merged_data) == 0:
            print("Hata: Veri birleştirme sonucunda hiç veri kalmadı!")
            return None
            
        # Korelasyon hesapla
        correlation_columns = [
            'temperature', 'max_temperature', 'min_temperature', 
            'precipitation', 'snow', 'wind_speed',
            'NumArticles', 'NumSources', 'AvgTone', 'GoldsteinScale',
            'Confidence', 'haber_sayisi'
        ]
        
        # Sadece mevcut olan sütunları kullan
        available_columns = [col for col in correlation_columns if col in merged_data.columns]
        
        if len(available_columns) < 2:
            print("Hata: Korelasyon hesaplaması için yeterli veri sütunu yok!")
            return None
            
        # Korelasyon matrisi
        correlation = merged_data[available_columns].corr()
        
        print("\nKorelasyon Matrisi:")
        print(correlation.round(2))
        
        # Korelasyon ısı haritası
        plt.figure(figsize=(12, 8))
        sns.heatmap(correlation, annot=True, cmap='coolwarm', fmt='.2f', linewidths=0.5)
        plt.title('İklim ve Haber Verileri Korelasyon Matrisi')
        plt.tight_layout()
        
        output_dir = 'output'
        os.makedirs(output_dir, exist_ok=True)
        plt.savefig(os.path.join(output_dir, 'korelasyon_heatmap.png'))
        plt.close()
        
        # Zaman serisi analizi
        print("\nZaman serisi analizi yapılıyor...")
        
        # Veriyi tarihe göre sırala
        merged_data = merged_data.sort_values('date')
        
        # Hareketli ortalamalar
        window_size = 7
        merged_data['temp_ma'] = merged_data['temperature'].rolling(window=window_size).mean()
        merged_data['haber_ma'] = merged_data['haber_sayisi'].rolling(window=window_size).mean()
        merged_data['tone_ma'] = merged_data['AvgTone'].rolling(window=window_size).mean()
        
        # Zaman serisi grafikleri
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
        
        # Sıcaklık ve haber sayısı
        ax1.plot(merged_data['date'], merged_data['temperature'], label='Günlük Sıcaklık', alpha=0.5)
        ax1.plot(merged_data['date'], merged_data['temp_ma'], label=f'{window_size} Günlük Hareketli Ortalama', linewidth=2)
        ax1.set_ylabel('Sıcaklık (°C)')
        ax1.legend(loc='upper left')
        
        ax1_twin = ax1.twinx()
        ax1_twin.plot(merged_data['date'], merged_data['haber_sayisi'], 'g-', label='Günlük Haber Sayısı', alpha=0.5)
        ax1_twin.plot(merged_data['date'], merged_data['haber_ma'], 'g--', 
                     label=f'{window_size} Günlük Hareketli Ortalama', linewidth=2)
        ax1_twin.set_ylabel('Haber Sayısı')
        ax1_twin.legend(loc='upper right')
        
        ax1.set_title('Sıcaklık ve Haber Sayısı İlişkisi')
        
        # Sıcaklık ve haber tonu
        ax2.plot(merged_data['date'], merged_data['temperature'], label='Günlük Sıcaklık', alpha=0.5)
        ax2.plot(merged_data['date'], merged_data['temp_ma'], label=f'{window_size} Günlük Hareketli Ortalama', linewidth=2)
        ax2.set_ylabel('Sıcaklık (°C)')
        ax2.legend(loc='upper left')
        
        ax2_twin = ax2.twinx()
        ax2_twin.plot(merged_data['date'], merged_data['AvgTone'], 'r-', label='Günlük Haber Tonu', alpha=0.5)
        ax2_twin.plot(merged_data['date'], merged_data['tone_ma'], 'r--', 
                     label=f'{window_size} Günlük Hareketli Ortalama', linewidth=2)
        ax2_twin.set_ylabel('Haber Tonu')
        ax2_twin.legend(loc='upper right')
        
        ax2.set_title('Sıcaklık ve Haber Tonu İlişkisi')
        
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'zaman_serisi_korelasyon.png'))
        plt.close()
        
        # Scatter plot analizi
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Sıcaklık vs Haber Sayısı
        sns.regplot(x='temperature', y='haber_sayisi', data=merged_data, ax=ax1)
        ax1.set_title('Sıcaklık vs Haber Sayısı')
        ax1.set_xlabel('Sıcaklık (°C)')
        ax1.set_ylabel('Haber Sayısı')
        
        # Sıcaklık vs Haber Tonu
        sns.regplot(x='temperature', y='AvgTone', data=merged_data, ax=ax2)
        ax2.set_title('Sıcaklık vs Haber Tonu')
        ax2.set_xlabel('Sıcaklık (°C)')
        ax2.set_ylabel('Haber Tonu')
        
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'scatter_korelasyon.png'))
        plt.close()
        
        # İstatistiksel testler
        print("\nİstatistiksel Testler:")
        
        # Sıcaklık ve haber sayısı arasındaki korelasyon
        temp_haber_corr = stats.pearsonr(merged_data['temperature'], merged_data['haber_sayisi'])
        print(f"Sıcaklık-Haber Sayısı Korelasyonu: {temp_haber_corr[0]:.3f} (p-değeri: {temp_haber_corr[1]:.3f})")
        
        # Sıcaklık ve haber tonu arasındaki korelasyon
        temp_tone_corr = stats.pearsonr(merged_data['temperature'], merged_data['AvgTone'])
        print(f"Sıcaklık-Haber Tonu Korelasyonu: {temp_tone_corr[0]:.3f} (p-değeri: {temp_tone_corr[1]:.3f})")
        
        # Sıcak günler vs normal günler analizi
        temp_threshold = merged_data['temperature'].mean() + merged_data['temperature'].std()
        hot_days = merged_data['temperature'] > temp_threshold
        
        print("\nSıcak Günler vs Normal Günler Analizi:")
        print(f"Sıcak gün sayısı: {hot_days.sum()}")
        print(f"Normal gün sayısı: {len(hot_days) - hot_days.sum()}")
        
        # Haber sayısı karşılaştırması
        hot_days_news = merged_data.loc[hot_days, 'haber_sayisi']
        normal_days_news = merged_data.loc[~hot_days, 'haber_sayisi']
        
        t_stat, p_value = stats.ttest_ind(hot_days_news, normal_days_news, equal_var=False)
        print(f"\nHaber Sayısı T-Testi:")
        print(f"t-istatistiği: {t_stat:.3f}")
        print(f"p-değeri: {p_value:.3f}")
        print(f"Sıcak günlerde ortalama haber sayısı: {hot_days_news.mean():.2f}")
        print(f"Normal günlerde ortalama haber sayısı: {normal_days_news.mean():.2f}")
        print(f"Fark anlamlı mı? {'Evet (p<0.05)' if p_value < 0.05 else 'Hayır (p>0.05)'}")
        
        # Box plot
        plt.figure(figsize=(8, 6))
        plt.boxplot([normal_days_news, hot_days_news], labels=['Normal Günler', 'Sıcak Günler'])
        plt.title('Sıcak ve Normal Günlerde Haber Sayısı Karşılaştırması')
        plt.ylabel('Haber Sayısı')
        plt.savefig(os.path.join(output_dir, 'boxplot_karsilastirma.png'))
        plt.close()
        
        return merged_data
        
    except Exception as e:
        print(f"Hata: Korelasyon analizi yapılamadı - {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def create_visualizations(merged_data, country_name, start_date, end_date):
    """Birleştirilmiş verileri ülke bazında görselleştir"""
    try:
        if merged_data is None or len(merged_data) == 0:
            print("Hata: Görselleştirme için veri bulunamadı!")
            return
            
        output_dir = 'output'
        os.makedirs(output_dir, exist_ok=True)
        
        date_format = '%Y-%m-%d'
        start_date_str = start_date.strftime(date_format) if isinstance(start_date, datetime) else start_date.strftime(date_format) if isinstance(start_date, date) else str(start_date)
        end_date_str = end_date.strftime(date_format) if isinstance(end_date, datetime) else end_date.strftime(date_format) if isinstance(end_date, date) else str(end_date)
        
        base_filename = f"{country_name.replace(' ', '_')}_{start_date_str}_to_{end_date_str}"
        
        if 'temperature' in merged_data.columns and 'news_count' in merged_data.columns:
            fig, ax1 = plt.subplots(figsize=(12, 6))
            color = 'tab:red'
            ax1.set_xlabel('Tarih')
            ax1.set_ylabel('Sıcaklık (°C)', color=color)
            ax1.plot(merged_data['date'], merged_data['temperature'], color=color, label='Sıcaklık')
            ax1.tick_params(axis='y', labelcolor=color)
            ax2 = ax1.twinx()
            color = 'tab:blue'
            ax2.set_ylabel('Haber Sayısı', color=color)
            ax2.plot(merged_data['date'], merged_data['news_count'], color=color, label='Haber Sayısı')
            ax2.tick_params(axis='y', labelcolor=color)
            plt.title(f'{country_name} - Sıcaklık ve Haber Sayısı Karşılaştırması')
            fig.tight_layout()
            temp_news_file = os.path.join(output_dir, f"temp_news_{base_filename}.png")
            plt.savefig(temp_news_file)
            plt.close()
            print(f"Grafik kaydedildi: {temp_news_file}")
            
        if 'precipitation' in merged_data.columns and 'news_count' in merged_data.columns:
            fig, ax1 = plt.subplots(figsize=(12, 6))
            color = 'tab:green'
            ax1.set_xlabel('Tarih')
            ax1.set_ylabel('Yağış (mm)', color=color)
            ax1.bar(merged_data['date'], merged_data['precipitation'], color=color, label='Yağış', alpha=0.7)
            ax1.tick_params(axis='y', labelcolor=color)
            ax2 = ax1.twinx()
            color = 'tab:blue'
            ax2.set_ylabel('Haber Sayısı', color=color)
            ax2.plot(merged_data['date'], merged_data['news_count'], color=color, label='Haber Sayısı')
            ax2.tick_params(axis='y', labelcolor=color)
            plt.title(f'{country_name} - Yağış ve Haber Sayısı Karşılaştırması')
            fig.tight_layout()
            prcp_news_file = os.path.join(output_dir, f"prcp_news_{base_filename}.png")
            plt.savefig(prcp_news_file)
            plt.close()
            print(f"Grafik kaydedildi: {prcp_news_file}")
            
        if 'temperature' in merged_data.columns and 'AvgTone' in merged_data.columns:
            fig, ax1 = plt.subplots(figsize=(12, 6))
            color = 'tab:red'
            ax1.set_xlabel('Tarih')
            ax1.set_ylabel('Sıcaklık (°C)', color=color)
            ax1.plot(merged_data['date'], merged_data['temperature'], color=color, label='Sıcaklık')
            ax1.tick_params(axis='y', labelcolor=color)
            ax2 = ax1.twinx()
            color = 'tab:purple'
            ax2.set_ylabel('Haber Tonu', color=color)
            ax2.plot(merged_data['date'], merged_data['AvgTone'], color=color, label='Haber Tonu')
            ax2.tick_params(axis='y', labelcolor=color)
            plt.title(f'{country_name} - Sıcaklık ve Haber Tonu Karşılaştırması')
            fig.tight_layout()
            temp_tone_file = os.path.join(output_dir, f"temp_tone_{base_filename}.png")
            plt.savefig(temp_tone_file)
            plt.close()
            print(f"Grafik kaydedildi: {temp_tone_file}")
            
        correlation_columns = [
            'temperature', 'max_temperature', 'min_temperature', 
            'precipitation', 'snow', 'wind_speed',
            'news_count', 'total_articles', 'total_sources', 'AvgTone'
        ]
        available_columns = [col for col in correlation_columns if col in merged_data.columns]
        
        if len(available_columns) >= 2:
            plt.figure(figsize=(10, 8))
            correlation = merged_data[available_columns].corr()
            sns.heatmap(correlation, annot=True, cmap='coolwarm', fmt='.2f', linewidths=0.5)
            plt.title(f'{country_name} - İklim ve Haber Verileri Korelasyon Matrisi')
            plt.tight_layout()
            corr_file = os.path.join(output_dir, f"correlation_{base_filename}.png")
            plt.savefig(corr_file)
            plt.close()
            print(f"Grafik kaydedildi: {corr_file}")
            
        if 'temperature' in merged_data.columns and 'news_count' in merged_data.columns:
            plt.figure(figsize=(10, 6))
            sns.regplot(x='temperature', y='news_count', data=merged_data, scatter_kws={'alpha':0.5}, line_kws={'color':'red'})
            plt.title(f'{country_name} - Sıcaklık ve Haber Sayısı İlişkisi')
            plt.xlabel('Sıcaklık (°C)')
            plt.ylabel('Haber Sayısı')
            scatter_file = os.path.join(output_dir, f"scatter_{base_filename}.png")
            plt.savefig(scatter_file)
            plt.close()
            print(f"Grafik kaydedildi: {scatter_file}")
            
        if 'temperature' in merged_data.columns and 'news_count' in merged_data.columns:
            try:
                temp_threshold = merged_data['temperature'].mean() + merged_data['temperature'].std()
                hot_days = merged_data['temperature'] > temp_threshold
                hot_days_news = merged_data.loc[hot_days, 'news_count']
                normal_days_news = merged_data.loc[~hot_days, 'news_count']
                plt.figure(figsize=(8, 6))
                plt.boxplot([normal_days_news, hot_days_news], labels=['Normal Günler', 'Sıcak Günler'])
                plt.title(f'{country_name} - Sıcak ve Normal Günlerde Haber Sayısı Karşılaştırması')
                plt.ylabel('Haber Sayısı')
                plt.axhline(y=normal_days_news.mean(), color='b', linestyle='-', linewidth=1)
                plt.axhline(y=hot_days_news.mean(), color='r', linestyle='-', linewidth=1)
                box_file = os.path.join(output_dir, f"boxplot_{base_filename}.png")
                plt.savefig(box_file)
                plt.close()
                print(f"Grafik kaydedildi: {box_file}")
                
                t_stat, p_value = stats.ttest_ind(hot_days_news, normal_days_news, equal_var=False)
                print(f"\nSıcak ve Normal Günler T-Testi ({country_name}):")
                print(f"t-istatistiği: {t_stat:.3f}")
                print(f"p-değeri: {p_value:.3f}")
                print(f"Sıcak günlerde ortalama haber sayısı: {hot_days_news.mean():.2f}")
                print(f"Normal günlerde ortalama haber sayısı: {normal_days_news.mean():.2f}")
                print(f"Fark anlamlı mı? {'Evet (p<0.05)' if p_value < 0.05 else 'Hayır (p>0.05)'}")
                
            except Exception as e:
                print(f"Uyarı: {country_name} için özel analizler yapılamadı - {str(e)}")
            
        print("\nTüm grafikler başarıyla oluşturuldu.")
        return True
        
    except Exception as e:
        print(f"Hata: Görselleştirme ({country_name} için) yapılamadı - {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def analyze_country(station_code_for_climate, country_code_for_news, country_name_for_output, start_date, end_date):
    """Belirli bir ülke için iklim ve haber analizleri yapar"""
    try:
        print(f"\n{country_name_for_output} ({country_code_for_news}) için analiz yapılıyor...")
        
        climate_data = fetch_climate_data(station_code_for_climate, start_date, end_date)
        news_data = haber_verilerini_cek(country_code_for_news, start_date, end_date)
        
        if climate_data is None or news_data is None:
            print(f"Hata: {country_name_for_output} için veri çekilemedi (İklim: {climate_data is not None}, Haber: {news_data is not None})!")
            return False
            
        merged_data = analyze_correlation(climate_data, news_data)
        
        if merged_data is None:
            print(f"Hata: {country_name_for_output} için korelasyon analizi yapılamadı!")
            return False
            
        success = create_visualizations(merged_data, country_name_for_output, start_date, end_date)
        
        return success
        
    except Exception as e:
        print(f"Hata: {country_name_for_output} analizi yapılamadı - {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Ana program akışı"""
    try:
        print("Ülke Bazlı İklim ve Haber Verileri Analizi\n" + "="*40)
        
        end_date = datetime.now().date() - timedelta(days=10)
        start_date = end_date - timedelta(days=180) 
        
        print(f"Analiz tarih aralığı: {start_date} - {end_date}")
        
        gdelt_data_directory = 'data_storage/gdelt'
        top_country_codes = gdelt_dosyalarindan_populer_ulkeleri_bul(gdelt_data_directory, n=5)

        if not top_country_codes:
            print("GDELT verilerinden analiz edilecek popüler ülke bulunamadı. Program sonlandırılıyor.")
            return False

        # Ülke kodlarını NOAA istasyon kodları ve okunabilir ülke adlarıyla eşleştir
        # Bu eşleme manuel olarak genişletilmeli veya daha dinamik bir çözüm bulunmalı
        country_station_map = {
            'US': ('USW00094728', 'Amerika Birleşik Devletleri'), # New York City
            'CH': ('LFSB0000000', 'Çin'), # Pekin (Not: Bu istasyon kodu Çin için doğru olmayabilir, örnek amaçlıdır)
            'RS': ('UUEE0000000', 'Rusya'), # Moskova (Not: Bu istasyon kodu Rusya için doğru olmayabilir, örnek amaçlıdır)
            'IN': ('VIDP0000000', 'Hindistan'), # Yeni Delhi (Not: Bu istasyon kodu Hindistan için doğru olmayabilir, örnek amaçlıdır)
            'GB': ('EGLL0000000', 'Birleşik Krallık'), # Londra Heathrow
            'FR': ('LFPG0000000', 'Fransa'), # Paris Charles de Gaulle
            'DE': ('EDDB0000000', 'Almanya'), # Berlin Brandenburg (Not: EDDB Schönefeld idi, yeni kod farklı olabilir)
            'JP': ('RJTT0000000', 'Japonya'), # Tokyo Haneda
            'CA': ('CYYZ0000000', 'Kanada'), # Toronto Pearson
            'AU': ('YSSY0000000', 'Avustralya') # Sydney Kingsford Smith
            # Diğer popüler ülkeler için buraya eklemeler yapılabilir.
        }
        
        countries_to_analyze = {}
        print("\nAnaliz için seçilen ülkeler ve kullanılacak iklim istasyonları:")
        for code in top_country_codes:
            if code in country_station_map:
                station, name = country_station_map[code]
                countries_to_analyze[station] = (code, name)
                print(f"- {name} ({code}) için iklim istasyonu: {station}")
            else:
                print(f"Uyarı: {code} ülke kodu için tanımlı bir iklim istasyonu bulunamadı. Bu ülke atlanacak.")

        if not countries_to_analyze:
            print("Analiz edilecek uygun ülke bulunamadı (istasyon eşleşmesi yapılamadı). Program sonlandırılıyor.")
            return False

        successful_analyses = []
        failed_analyses = []
        
        print("\nUYARI: İklim verileri, belirtilen ülkenin sadece bir büyük şehrinin/bölgesinin istasyon kodundan alınmaktadır.")
        print("Daha doğru bir ülke geneli iklim analizi için bu metodoloji geliştirilmelidir.\n")

        for station_code, (country_code, country_name) in countries_to_analyze.items():
            success = analyze_country(station_code, country_code, country_name, start_date, end_date)
            if success:
                successful_analyses.append(country_name)
            else:
                failed_analyses.append(f"{country_name} ({country_code})")
        
        print("\nAnaliz Sonuçları:")
        print(f"Başarılı analizler: {', '.join(successful_analyses) if successful_analyses else 'Yok'}")
        print(f"Başarısız analizler: {', '.join(failed_analyses) if failed_analyses else 'Yok'}")
        
        if successful_analyses:
            print("\nBaşarılı analizler için grafikler 'output' klasöründe bulunabilir.")
        
        return True
        
    except Exception as e:
        print(f"Hata: Ana program yürütülemedi - {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    main() 