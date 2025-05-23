import pytest
import os
import sys
import logging

# Proje kök dizinini Python path'ine ekle
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Test için loglama ayarları
@pytest.fixture(autouse=True)
def setup_logging():
    """Test için loglama ayarlarını yapılandır"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

# Test için geçici dizin oluştur
@pytest.fixture(scope="session")
def test_dir(tmp_path_factory):
    """Test için geçici dizin oluştur"""
    return tmp_path_factory.mktemp("test_data")

# Test için MongoDB bağlantı ayarları
@pytest.fixture(scope="session")
def mongo_uri():
    """Test için MongoDB bağlantı URI'si"""
    return "mongodb://localhost:27017"

# Test için Spark oturumu
@pytest.fixture(scope="session")
def spark_session():
    """Test için Spark oturumu oluştur"""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("ClimateWatchTest") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop() 