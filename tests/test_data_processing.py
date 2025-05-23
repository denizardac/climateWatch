import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys

# Proje kök dizinini Python path'ine ekle
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def create_sample_data():
    """Test için örnek veri oluştur"""
    # GDELT örnek verisi
    gdelt_data = {
        'date': pd.date_range(start='2023-01-01', end='2023-01-07'),
        'source': ['Reuters', 'AP', 'BBC', 'CNN', 'Al Jazeera', 'Reuters', 'AP'],
        'title': [
            'Climate change impacts global temperatures',
            'New study shows rising CO2 levels',
            'Extreme weather events increase',
            'Renewable energy adoption grows',
            'Climate summit concludes',
            'Global warming accelerates',
            'Climate action needed'
        ],
        'text': [
            'Scientists warn about climate change effects.',
            'Carbon dioxide levels reach new high.',
            'More frequent extreme weather events reported.',
            'Countries invest in renewable energy.',
            'International climate summit ends with new agreements.',
            'Global warming trends continue to rise.',
            'Experts call for immediate climate action.'
        ]
    }
    
    # İklim verisi örneği
    climate_data = {
        'Year': range(2015, 2023),
        'Mean': [0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6],
        'Source': ['NOAA'] * 8
    }
    
    # Google Trends örnek verisi
    trends_data = {
        'date': pd.date_range(start='2023-01-01', end='2023-01-07'),
        'climate change': [75, 80, 85, 90, 95, 85, 80]
    }
    
    return {
        'gdelt': pd.DataFrame(gdelt_data),
        'climate': pd.DataFrame(climate_data),
        'trends': pd.DataFrame(trends_data)
    }

@pytest.fixture
def sample_data():
    """Test için örnek veri fixture'ı"""
    return create_sample_data()

def test_data_cleaning(sample_data):
    """Veri temizleme işlemlerini test et"""
    # GDELT verisi temizleme
    df_gdelt = sample_data['gdelt'].copy()
    
    # Eksik değerleri kontrol et
    assert df_gdelt.isnull().sum().sum() == 0, "GDELT verisinde eksik değer var"
    
    # Tarih formatını kontrol et
    assert pd.api.types.is_datetime64_any_dtype(df_gdelt['date']), "Tarih sütunu datetime formatında değil"
    
    # Metin temizliği
    df_gdelt['text'] = df_gdelt['text'].str.lower()
    assert df_gdelt['text'].str.islower().all(), "Metinler küçük harfe çevrilmemiş"
    
    # İklim verisi temizleme
    df_climate = sample_data['climate'].copy()
    
    # Sayısal değerleri kontrol et
    assert pd.api.types.is_numeric_dtype(df_climate['Mean']), "Mean sütunu sayısal değil"
    assert df_climate['Mean'].min() >= 0, "Mean değerleri negatif"
    
    # Google Trends verisi temizleme
    df_trends = sample_data['trends'].copy()
    
    # Trend değerlerini kontrol et
    assert df_trends['climate change'].min() >= 0, "Trend değerleri negatif"
    assert df_trends['climate change'].max() <= 100, "Trend değerleri 100'den büyük"

def test_data_transformation(sample_data):
    """Veri dönüştürme işlemlerini test et"""
    # GDELT verisi dönüştürme
    df_gdelt = sample_data['gdelt'].copy()
    
    # Tarih bazlı gruplama
    daily_counts = df_gdelt.groupby('date').size()
    assert len(daily_counts) == 7, "Günlük gruplama hatalı"
    
    # İklim verisi dönüştürme
    df_climate = sample_data['climate'].copy()
    
    # Yıllık ortalama hesaplama
    yearly_avg = df_climate['Mean'].mean()
    assert isinstance(yearly_avg, (int, float)), "Yıllık ortalama hesaplanamadı"
    
    # Google Trends verisi dönüştürme
    df_trends = sample_data['trends'].copy()
    
    # Hareketli ortalama hesaplama
    df_trends['ma7'] = df_trends['climate change'].rolling(window=7).mean()
    assert not df_trends['ma7'].isnull().all(), "Hareketli ortalama hesaplanamadı"

def test_data_validation_rules(sample_data):
    """Veri doğrulama kurallarını test et"""
    # GDELT verisi doğrulama
    df_gdelt = sample_data['gdelt'].copy()
    
    # Başlık uzunluğu kontrolü
    assert df_gdelt['title'].str.len().min() > 0, "Boş başlık var"
    
    # Metin uzunluğu kontrolü
    assert df_gdelt['text'].str.len().min() > 0, "Boş metin var"
    
    # İklim verisi doğrulama
    df_climate = sample_data['climate'].copy()
    
    # Yıl sıralaması kontrolü
    assert df_climate['Year'].is_monotonic_increasing, "Yıllar sıralı değil"
    
    # Google Trends verisi doğrulama
    df_trends = sample_data['trends'].copy()
    
    # Tarih sıralaması kontrolü
    assert df_trends['date'].is_monotonic_increasing, "Tarihler sıralı değil" 