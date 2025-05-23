import pytest
import pandas as pd
import os
from datetime import datetime, timedelta
import sys

# Proje kök dizinini Python path'ine ekle
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_processing.pipeline import DataProcessingPipeline

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
def temp_dir(tmp_path):
    """Geçici dizin fixture'ı"""
    return str(tmp_path)

@pytest.fixture
def pipeline(temp_dir):
    """Pipeline fixture'ı"""
    log_path = os.path.join(temp_dir, "pipeline.log")
    return DataProcessingPipeline(log_path=log_path)

@pytest.fixture
def sample_data():
    """Örnek veri fixture'ı"""
    return create_sample_data()

def test_pipeline_initialization(pipeline):
    """Pipeline başlatma testi"""
    assert pipeline.processed_data == {}
    assert pipeline.logger is not None

def test_gdelt_processing(pipeline, sample_data):
    """GDELT veri işleme testi"""
    df = pipeline.process_gdelt_data(sample_data['gdelt'])
    
    # Tarih formatı kontrolü
    assert pd.api.types.is_datetime64_any_dtype(df['date'])
    
    # Metin temizliği kontrolü
    assert df['title'].str.islower().all()
    assert df['text'].str.islower().all()
    
    # Duplikasyon kontrolü
    assert not df.duplicated(subset=['title', 'text']).any()

def test_climate_processing(pipeline, sample_data):
    """İklim veri işleme testi"""
    df = pipeline.process_climate_data(sample_data['climate'])
    
    # Sayısal değer kontrolü
    assert pd.api.types.is_numeric_dtype(df['Mean'])
    
    # Sıralama kontrolü
    assert df['Year'].is_monotonic_increasing
    
    # Aykırı değer kontrolü
    mean = df['Mean'].mean()
    std = df['Mean'].std()
    assert (abs(df['Mean'] - mean) <= 3 * std).all()

def test_trends_processing(pipeline, sample_data):
    """Google Trends veri işleme testi"""
    df = pipeline.process_trends_data(sample_data['trends'])
    
    # Tarih formatı kontrolü
    assert pd.api.types.is_datetime64_any_dtype(df['date'])
    
    # Trend değeri kontrolü
    assert pd.api.types.is_numeric_dtype(df['climate change'])
    assert df['climate change'].min() >= 0
    assert df['climate change'].max() <= 100
    
    # Hareketli ortalama kontrolü
    assert 'ma7' in df.columns
    assert not df['ma7'].isnull().all()

def test_full_pipeline(pipeline, sample_data, temp_dir):
    """Tam pipeline testi"""
    # Pipeline'ı çalıştır
    processed_data = pipeline.run_pipeline(sample_data)
    
    # İşlenmiş verileri kontrol et
    assert 'gdelt' in processed_data
    assert 'climate' in processed_data
    assert 'trends' in processed_data
    
    # Verileri kaydet
    output_dir = os.path.join(temp_dir, "processed")
    pipeline.save_processed_data(output_dir)
    
    # Kaydedilen dosyaları kontrol et
    assert os.path.exists(os.path.join(output_dir, "processed_gdelt.csv"))
    assert os.path.exists(os.path.join(output_dir, "processed_climate.csv"))
    assert os.path.exists(os.path.join(output_dir, "processed_trends.csv")) 