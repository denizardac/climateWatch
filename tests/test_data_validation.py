import unittest
import pandas as pd
import numpy as np
from data_validation.quality_checker import DataQualityChecker, validate_climate_data, validate_news_data

class TestDataValidation(unittest.TestCase):
    def setUp(self):
        # Test verisi oluştur
        self.climate_data = pd.DataFrame({
            'date': pd.date_range(start='2020-01-01', periods=100),
            'temperature': np.random.normal(20, 5, 100),
            'humidity': np.random.normal(60, 10, 100),
            'pressure': np.random.normal(1013, 5, 100)
        })
        
        self.news_data = pd.DataFrame({
            'date': pd.date_range(start='2020-01-01', periods=100),
            'title': ['Climate News ' + str(i) for i in range(100)],
            'content': ['Content ' + str(i) for i in range(100)],
            'sentiment': np.random.choice(['positive', 'negative', 'neutral'], 100)
        })
    
    def test_missing_values(self):
        # Eksik değer testi
        checker = DataQualityChecker(self.climate_data)
        missing_values = checker.check_missing_values()
        self.assertEqual(len(missing_values), 4)  # 4 kolon için eksik değer kontrolü
        self.assertTrue(all(v == 0 for v in missing_values.values()))  # Hiç eksik değer olmamalı
    
    def test_data_types(self):
        # Veri tipi testi
        checker = DataQualityChecker(self.climate_data)
        data_types = checker.check_data_types()
        self.assertEqual(data_types['temperature'], 'float64')
        self.assertEqual(data_types['date'], 'datetime64[ns]')
    
    def test_duplicates(self):
        # Tekrar eden satır testi
        checker = DataQualityChecker(self.climate_data)
        duplicates = checker.check_duplicates()
        self.assertEqual(duplicates, 0)  # Tekrar eden satır olmamalı
    
    def test_outliers(self):
        # Aykırı değer testi
        checker = DataQualityChecker(self.climate_data)
        outliers = checker.check_outliers(['temperature'])
        self.assertIn('temperature', outliers)
        self.assertIn('count', outliers['temperature'])
    
    def test_climate_data_validation(self):
        # İklim verisi doğrulama testi
        report = validate_climate_data(self.climate_data)
        self.assertIn('missing_values', report)
        self.assertIn('data_types', report)
        self.assertIn('duplicates', report)
    
    def test_news_data_validation(self):
        # Haber verisi doğrulama testi
        report = validate_news_data(self.news_data)
        self.assertIn('missing_values', report)
        self.assertIn('data_types', report)
        self.assertIn('duplicates', report)

if __name__ == '__main__':
    unittest.main() 