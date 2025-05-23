import pytest
import pandas as pd
import os
from datetime import datetime, timedelta
import sys
import logging

# Proje kök dizinini Python path'ine ekle
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_ingestion.fetch_gdelt_news import fetch_gdelt_for_date
from data_ingestion.fetch_climate_data import fetch_and_save_climate_data

# Test için geçici dizin oluştur
@pytest.fixture
def temp_dir(tmp_path):
    return str(tmp_path)

# Test için geçici log dosyası oluştur
@pytest.fixture
def temp_log_file(tmp_path):
    log_file = tmp_path / "test.log"
    return str(log_file)

def test_gdelt_data_fetching(temp_dir, temp_log_file):
    """GDELT veri çekme işlemini test et"""
    # Son 7 günün verilerini çek
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    # Test için tek bir günün verisini çek
    test_date = start_date
    fetch_gdelt_for_date(
        test_date,
        target_dir=temp_dir,
        save_to_mongo=False,
        log_path=temp_log_file
    )
    
    # Veri dosyasının oluşturulduğunu kontrol et
    expected_file = os.path.join(temp_dir, f"{test_date.strftime('%Y%m%d')}.csv")
    assert os.path.exists(expected_file), "GDELT veri dosyası oluşturulmadı"
    
    # Dosyanın içeriğini kontrol et
    df = pd.read_csv(expected_file)
    assert not df.empty, "GDELT veri dosyası boş"
    assert len(df.columns) > 0, "GDELT veri dosyasında sütun yok"

def test_climate_data_fetching(temp_dir, temp_log_file):
    """İklim verisi çekme işlemini test et"""
    fetch_and_save_climate_data(
        target_dir=temp_dir,
        save_to_mongo=False,
        log_path=temp_log_file
    )
    
    # Veri dosyasının oluşturulduğunu kontrol et
    expected_file = os.path.join(temp_dir, "global_temp.csv")
    assert os.path.exists(expected_file), "İklim veri dosyası oluşturulmadı"
    
    # Dosyanın içeriğini kontrol et
    df = pd.read_csv(expected_file)
    assert not df.empty, "İklim veri dosyası boş"
    assert "Year" in df.columns, "İklim veri dosyasında Year sütunu yok"
    assert "Mean" in df.columns, "İklim veri dosyasında Mean sütunu yok"

def test_data_validation(temp_dir):
    """Veri doğrulama testleri"""
    # GDELT verisi için doğrulama
    gdelt_file = os.path.join(temp_dir, f"{datetime.now().strftime('%Y%m%d')}.csv")
    if os.path.exists(gdelt_file):
        df_gdelt = pd.read_csv(gdelt_file)
        assert df_gdelt.isnull().sum().sum() < len(df_gdelt) * 0.5, "GDELT verisinde çok fazla eksik değer var"
    
    # İklim verisi için doğrulama
    climate_file = os.path.join(temp_dir, "global_temp.csv")
    if os.path.exists(climate_file):
        df_climate = pd.read_csv(climate_file)
        assert df_climate["Year"].dtype in [int, float], "Year sütunu sayısal değil"
        assert df_climate["Mean"].dtype in [int, float], "Mean sütunu sayısal değil"
        assert df_climate["Year"].min() >= 1800, "Year değerleri mantıksız"
        assert df_climate["Year"].max() <= datetime.now().year, "Year değerleri mantıksız" 