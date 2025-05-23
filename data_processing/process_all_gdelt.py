import os
import pandas as pd
from country_code_mapper import CountryCodeMapper
import logging

# Logging ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def process_all_files():
    # Mapper'ı başlat
    mapper = CountryCodeMapper()
    
    # GDELT klasörünün yolu
    gdelt_dir = "data_storage/gdelt"
    
    # İşlenmiş dosyaların kaydedileceği klasör
    output_dir = "data_storage/gdelt_processed"
    os.makedirs(output_dir, exist_ok=True)
    
    # Sadece 2025 yılı dosyalarını işle
    csv_files = [f for f in os.listdir(gdelt_dir) if f.endswith('.csv') and f.startswith('2025')]
    
    # Her dosyayı işle
    for file_name in csv_files:
        input_path = os.path.join(gdelt_dir, file_name)
        output_path = os.path.join(output_dir, file_name)
        
        logger.info(f"İşleniyor: {file_name}")
        
        # Dosyayı işle
        df = mapper.process_gdelt_file(input_path)
        
        if not df.empty:
            # İşlenmiş veriyi kaydet
            df.to_csv(output_path, index=False)
            
            # İstatistikleri göster
            logger.info(f"Toplam kayıt: {len(df)}")
            logger.info(f"Ülke kodu olan Actor1: {df['Actor1CountryCode'].notna().sum()}")
            logger.info(f"Ülke kodu olan Actor2: {df['Actor2CountryCode'].notna().sum()}")
            
            # Eşleşmeyen ülke isimlerini göster
            unmatched = df[df['Actor1CountryCode'].isna()]['Actor1Name'].unique()
            if len(unmatched) > 0:
                logger.info(f"Eşleşmeyen ülke isimleri ({len(unmatched)}):")
                for name in unmatched[:5]:  # İlk 5 tanesini göster
                    logger.info(f"- {name}")
                if len(unmatched) > 5:
                    logger.info(f"... ve {len(unmatched)-5} tane daha")
        else:
            logger.error(f"Dosya işlenemedi: {file_name}")

if __name__ == "__main__":
    process_all_files() 