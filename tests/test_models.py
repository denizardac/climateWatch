import unittest
import pandas as pd
import numpy as np
from models.train_model import ClimateModelTrainer, train_climate_trend_model, train_news_sentiment_model

class TestModels(unittest.TestCase):
    def setUp(self):
        # Test verisi oluştur
        self.climate_data = pd.DataFrame({
            'date': pd.date_range(start='2020-01-01', periods=100),
            'temperature': np.random.normal(20, 5, 100),
            'humidity': np.random.normal(60, 10, 100),
            'pressure': np.random.normal(1013, 5, 100),
            'target': np.random.normal(25, 3, 100)  # Hedef değişken
        })
        
        self.news_data = pd.DataFrame({
            'date': pd.date_range(start='2020-01-01', periods=100),
            'title': ['Climate News ' + str(i) for i in range(100)],
            'content': ['Content ' + str(i) for i in range(100)],
            'sentiment': np.random.choice(['positive', 'negative', 'neutral'], 100)
        })
    
    def test_regression_model(self):
        # Regresyon modeli testi
        trainer = ClimateModelTrainer(model_type='regression')
        X_train, X_test, y_train, y_test = trainer.prepare_data(
            self.climate_data, 'target'
        )
        
        trainer.train(X_train, y_train)
        metrics = trainer.evaluate(X_test, y_test)
        
        self.assertIn('mse', metrics)
        self.assertIn('rmse', metrics)
        self.assertIsInstance(metrics['mse'], float)
        self.assertIsInstance(metrics['rmse'], float)
    
    def test_classification_model(self):
        # Sınıflandırma modeli testi
        trainer = ClimateModelTrainer(model_type='classification')
        X_train, X_test, y_train, y_test = trainer.prepare_data(
            self.news_data, 'sentiment'
        )
        
        trainer.train(X_train, y_train)
        metrics = trainer.evaluate(X_test, y_test)
        
        self.assertIn('accuracy', metrics)
        self.assertIn('classification_report', metrics)
        self.assertIsInstance(metrics['accuracy'], float)
    
    def test_climate_trend_model(self):
        # İklim trendi modeli testi
        metrics = train_climate_trend_model(self.climate_data, 'target')
        
        self.assertIn('mse', metrics)
        self.assertIn('rmse', metrics)
        self.assertIsInstance(metrics['mse'], float)
        self.assertIsInstance(metrics['rmse'], float)
    
    def test_news_sentiment_model(self):
        # Haber duygu analizi modeli testi
        metrics = train_news_sentiment_model(self.news_data, 'sentiment')
        
        self.assertIn('accuracy', metrics)
        self.assertIn('classification_report', metrics)
        self.assertIsInstance(metrics['accuracy'], float)
    
    def test_model_save_load(self):
        # Model kaydetme ve yükleme testi
        trainer = ClimateModelTrainer(model_type='regression')
        X_train, X_test, y_train, y_test = trainer.prepare_data(
            self.climate_data, 'target'
        )
        
        trainer.train(X_train, y_train)
        trainer.save_model('test_model.joblib')
        
        new_trainer = ClimateModelTrainer(model_type='regression')
        new_trainer.load_model('test_model.joblib')
        
        # Yüklenen modelin tahminleri orijinal model ile aynı olmalı
        original_pred = trainer.model.predict(X_test)
        loaded_pred = new_trainer.model.predict(X_test)
        
        np.testing.assert_array_almost_equal(original_pred, loaded_pred)

if __name__ == '__main__':
    unittest.main() 