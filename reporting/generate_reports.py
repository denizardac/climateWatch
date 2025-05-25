import pandas as pd
import logging
from pathlib import Path
import yaml
from datetime import datetime

# Logging ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_config():
    """Konfigürasyon dosyasını yükler."""
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logging.error(f"Konfigürasyon yüklenirken hata: {str(e)}")
        return None

def ensure_directories():
    """Gerekli dizinlerin varlığını kontrol eder ve oluşturur."""
    dirs = ['reports', 'reports/daily', 'reports/weekly', 'reports/monthly']
    for dir_path in dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)

def generate_daily_report():
    """Günlük rapor oluşturur."""
    try:
        # GDELT makalelerini yükle
        articles_df = pd.read_csv('data_storage/processed/gdelt_articles_with_sentiment.csv')
        
        # Temel istatistikler
        total_articles = len(articles_df)
        avg_sentiment = articles_df['sentiment'].mean()
        
        # Rapor içeriği
        report = f"""Günlük İklim Haberleri Raporu
Tarih: {datetime.now().strftime('%Y-%m-%d')}

Toplam Makale Sayısı: {total_articles}
Ortalama Duygu Skoru: {avg_sentiment:.2f}

En Olumsuz 3 Haber:
{articles_df.nsmallest(3, 'sentiment')[['title', 'sentiment']].to_string()}

En Olumlu 3 Haber:
{articles_df.nlargest(3, 'sentiment')[['title', 'sentiment']].to_string()}
"""
        
        # Raporu kaydet
        report_path = f'reports/daily/report_{datetime.now().strftime("%Y%m%d")}.txt'
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
            
        logging.info(f"Günlük rapor oluşturuldu: {report_path}")
        return True
        
    except Exception as e:
        logging.error(f"Günlük rapor oluşturulurken hata: {str(e)}")
        return False

def main():
    """Ana fonksiyon."""
    logging.info("Raporlama başlatılıyor...")
    
    # Konfigürasyonu yükle
    config = load_config()
    if not config:
        return False
    
    # Dizinleri kontrol et
    ensure_directories()
    
    # Raporları oluştur
    success = generate_daily_report()
    
    if success:
        logging.info("Raporlama başarıyla tamamlandı")
    else:
        logging.error("Raporlama sırasında hatalar oluştu")
    
    return success

if __name__ == "__main__":
    main() 