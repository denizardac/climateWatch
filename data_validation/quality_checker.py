import pandas as pd
import numpy as np
from typing import Dict, List, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataQualityChecker:
    def __init__(self, data: pd.DataFrame):
        self.data = data
        self.quality_metrics = {}
    
    def check_missing_values(self) -> Dict[str, float]:
        """Eksik değerleri kontrol eder ve yüzdelik oranlarını döndürür."""
        missing_percentages = (self.data.isnull().sum() / len(self.data)) * 100
        self.quality_metrics['missing_values'] = missing_percentages.to_dict()
        return self.quality_metrics['missing_values']
    
    def check_data_types(self) -> Dict[str, str]:
        """Veri tiplerini kontrol eder."""
        self.quality_metrics['data_types'] = self.data.dtypes.astype(str).to_dict()
        return self.quality_metrics['data_types']
    
    def check_duplicates(self) -> int:
        """Tekrar eden satırları kontrol eder."""
        duplicates = self.data.duplicated().sum()
        self.quality_metrics['duplicates'] = duplicates
        return duplicates
    
    def check_outliers(self, columns: List[str], method: str = 'iqr') -> Dict[str, Dict[str, Any]]:
        """Belirtilen kolonlardaki aykırı değerleri kontrol eder."""
        outliers = {}
        for col in columns:
            if method == 'iqr':
                Q1 = self.data[col].quantile(0.25)
                Q3 = self.data[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                outliers[col] = {
                    'count': ((self.data[col] < lower_bound) | (self.data[col] > upper_bound)).sum(),
                    'lower_bound': lower_bound,
                    'upper_bound': upper_bound
                }
        self.quality_metrics['outliers'] = outliers
        return outliers
    
    def generate_report(self) -> Dict[str, Any]:
        """Tüm kalite metriklerini içeren bir rapor oluşturur."""
        self.check_missing_values()
        self.check_data_types()
        self.check_duplicates()
        return self.quality_metrics

def validate_climate_data(data: pd.DataFrame) -> Dict[str, Any]:
    """İklim verisi için özel doğrulama fonksiyonu."""
    checker = DataQualityChecker(data)
    
    # Sıcaklık verisi için aykırı değer kontrolü
    if 'temperature' in data.columns:
        checker.check_outliers(['temperature'])
    
    # Tarih formatı kontrolü
    if 'date' in data.columns:
        try:
            pd.to_datetime(data['date'])
        except Exception as e:
            logger.error(f"Tarih formatı hatası: {e}")
    
    return checker.generate_report()

def validate_news_data(data: pd.DataFrame) -> Dict[str, Any]:
    """Haber verisi için özel doğrulama fonksiyonu."""
    checker = DataQualityChecker(data)
    
    # Metin alanları için uzunluk kontrolü
    text_columns = [col for col in data.columns if data[col].dtype == 'object']
    for col in text_columns:
        data[f'{col}_length'] = data[col].str.len()
        checker.check_outliers([f'{col}_length'])
    
    return checker.generate_report() 