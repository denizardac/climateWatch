import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
import os
import sys

# Proje kök dizinini Python path'ine ekle
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class DataProcessingPipeline:
    """Veri işleme pipeline'ı"""
    
    def __init__(self, log_path: Optional[str] = None):
        """Pipeline'ı başlat"""
        self.logger = self._setup_logging(log_path)
        self.processed_data = {}
    
    def _setup_logging(self, log_path: Optional[str]) -> logging.Logger:
        """Loglama ayarlarını yapılandır"""
        logger = logging.getLogger("DataProcessingPipeline")
        if log_path:
            handler = logging.FileHandler(log_path)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger
    
    def process_gdelt_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """GDELT verisini işle"""
        self.logger.info("GDELT verisi işleniyor...")
        
        # Tarih sütununu datetime'a çevir
        df['date'] = pd.to_datetime(df['date'])
        
        # Metin temizliği
        df['title'] = df['title'].str.lower()
        df['text'] = df['text'].str.lower()
        
        # Eksik değerleri temizle
        df = df.dropna(subset=['title', 'text'])
        
        # Duplikasyonları kaldır
        df = df.drop_duplicates(subset=['title', 'text'])
        
        self.logger.info(f"GDELT verisi işlendi. {len(df)} satır kaldı.")
        return df
    
    def process_climate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """İklim verisini işle"""
        self.logger.info("İklim verisi işleniyor...")
        
        # Sayısal değerleri kontrol et
        df['Mean'] = pd.to_numeric(df['Mean'], errors='coerce')
        
        # Aykırı değerleri temizle
        mean = df['Mean'].mean()
        std = df['Mean'].std()
        df = df[abs(df['Mean'] - mean) <= 3 * std]
        
        # Yılları sırala
        df = df.sort_values('Year')
        
        self.logger.info(f"İklim verisi işlendi. {len(df)} satır kaldı.")
        return df
    
    def process_trends_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Google Trends verisini işle"""
        self.logger.info("Google Trends verisi işleniyor...")
        
        # Tarih sütununu datetime'a çevir
        df['date'] = pd.to_datetime(df['date'])
        
        # Eksik değerleri temizle
        df = df.dropna(subset=['date', 'climate change'])
        
        # Duplikasyonları kaldır
        df = df.drop_duplicates(subset=['date'])
        
        # 7 günlük hareketli ortalama hesapla
        df['ma7'] = df['climate change'].rolling(window=7, min_periods=1).mean()
        
        self.logger.info(f"Google Trends verisi işlendi. {len(df)} satır kaldı.")
        return df
    
    def run_pipeline(self, data_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Tüm veri işleme pipeline'ını çalıştır"""
        self.logger.info("Veri işleme pipeline'ı başlatılıyor...")
        
        try:
            if 'gdelt' in data_dict:
                self.processed_data['gdelt'] = self.process_gdelt_data(data_dict['gdelt'])
            
            if 'climate' in data_dict:
                self.processed_data['climate'] = self.process_climate_data(data_dict['climate'])
            
            if 'trends' in data_dict:
                self.processed_data['trends'] = self.process_trends_data(data_dict['trends'])
            
            self.logger.info("Veri işleme pipeline'ı başarıyla tamamlandı.")
            return self.processed_data
            
        except Exception as e:
            self.logger.error(f"Pipeline çalıştırılırken hata oluştu: {str(e)}")
            raise
    
    def save_processed_data(self, output_dir: str):
        """İşlenmiş verileri kaydet"""
        self.logger.info(f"İşlenmiş veriler {output_dir} dizinine kaydediliyor...")
        
        os.makedirs(output_dir, exist_ok=True)
        
        for data_type, df in self.processed_data.items():
            output_file = os.path.join(output_dir, f"processed_{data_type}.csv")
            df.to_csv(output_file, index=False)
            self.logger.info(f"{data_type} verisi {output_file} dosyasına kaydedildi.") 