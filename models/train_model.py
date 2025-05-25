import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.metrics import mean_squared_error, accuracy_score, classification_report
import joblib
import logging
from typing import Tuple, Dict, Any
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClimateModelTrainer:
    def __init__(self, model_type: str = 'regression'):
        self.model_type = model_type
        self.model = None
        self.scaler = StandardScaler()
        
    def prepare_data(self, data: pd.DataFrame, target_column: str, 
                    test_size: float = 0.2) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """Veriyi eğitim ve test setlerine ayırır."""
        # Target dışındaki datetime ve object tipindeki sütunları çıkar
        X = data.drop(columns=[target_column])
        # Sadece sayısal ve kategorik (object) sütunlar kalsın, datetime'ları çıkar
        X = X.select_dtypes(exclude=["datetime", "datetime64[ns]", "timedelta", "timedelta64[ns]"])
        y = data[target_column]
        
        # Kategorik değişkenleri one-hot encoding ile dönüştür
        X = pd.get_dummies(X)
        
        # Veriyi ölçeklendir
        X_scaled = self.scaler.fit_transform(X)
        
        # Eğitim ve test setlerine ayır
        X_train, X_test, y_train, y_test = train_test_split(
            X_scaled, y, test_size=test_size, random_state=42
        )
        
        return X_train, X_test, y_train, y_test
    
    def train(self, X_train: np.ndarray, y_train: np.ndarray) -> None:
        """Modeli eğitir."""
        if self.model_type == 'regression':
            self.model = RandomForestRegressor(n_estimators=100, random_state=42)
        else:
            self.model = RandomForestClassifier(n_estimators=100, random_state=42)
        
        self.model.fit(X_train, y_train)
        logger.info("Model eğitimi tamamlandı.")
    
    def evaluate(self, X_test: np.ndarray, y_test: np.ndarray) -> Dict[str, Any]:
        """Modeli değerlendirir."""
        y_pred = self.model.predict(X_test)
        
        if self.model_type == 'regression':
            mse = mean_squared_error(y_test, y_pred)
            rmse = np.sqrt(mse)
            metrics = {
                'mse': mse,
                'rmse': rmse
            }
        else:
            accuracy = accuracy_score(y_test, y_pred)
            report = classification_report(y_test, y_pred, output_dict=True)
            metrics = {
                'accuracy': accuracy,
                'classification_report': report
            }
        
        return metrics
    
    def save_model(self, model_path: str) -> None:
        """Modeli kaydeder."""
        if not os.path.exists('models/saved'):
            os.makedirs('models/saved')
        
        model_file = os.path.join('models/saved', model_path)
        joblib.dump(self.model, model_file)
        logger.info(f"Model kaydedildi: {model_file}")
    
    def load_model(self, model_path: str) -> None:
        """Kaydedilmiş modeli yükler."""
        model_file = os.path.join('models/saved', model_path)
        self.model = joblib.load(model_file)
        logger.info(f"Model yüklendi: {model_file}")

def train_climate_trend_model(data: pd.DataFrame, target_column: str) -> Dict[str, Any]:
    """İklim trendi tahmin modeli eğitir."""
    trainer = ClimateModelTrainer(model_type='regression')
    
    # Veriyi hazırla
    X_train, X_test, y_train, y_test = trainer.prepare_data(
        data, target_column
    )
    
    # Modeli eğit
    trainer.train(X_train, y_train)
    
    # Modeli değerlendir
    metrics = trainer.evaluate(X_test, y_test)
    
    # Modeli kaydet
    trainer.save_model('climate_trend_model.joblib')
    
    return metrics

def train_news_sentiment_model(data: pd.DataFrame, target_column: str) -> Dict[str, Any]:
    """Haber duygu analizi modeli eğitir."""
    trainer = ClimateModelTrainer(model_type='classification')
    
    # Veriyi hazırla
    X_train, X_test, y_train, y_test = trainer.prepare_data(
        data, target_column
    )
    
    # Modeli eğit
    trainer.train(X_train, y_train)
    
    # Modeli değerlendir
    metrics = trainer.evaluate(X_test, y_test)
    
    # Modeli kaydet
    trainer.save_model('news_sentiment_model.joblib')
    
    return metrics 