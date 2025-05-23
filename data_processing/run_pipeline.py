import os
import pandas as pd
from pipeline import DataProcessingPipeline

# Veri dosyalarının yolları
GDELT_DIR = 'data_storage/gdelt'
CLIMATE_FILE = 'data_storage/climate/global_temp.csv'
TRENDS_FILE = 'data_storage/trends/trends_climate_change_2023-01-01_2023-12-31.csv'

# GDELT dosyalarını okuyup birleştir
gdelt_dfs = []
for filename in os.listdir(GDELT_DIR):
    if filename.endswith('.csv'):
        filepath = os.path.join(GDELT_DIR, filename)
        # Sütun isimlerini manuel olarak belirle ve title sütununu string tipine dönüştür
        df = pd.read_csv(filepath, names=['date', 'source', 'title', 'text'], dtype={'title': str})
        gdelt_dfs.append(df)
gdelt_df = pd.concat(gdelt_dfs, ignore_index=True)

# İklim verisini oku
climate_df = pd.read_csv(CLIMATE_FILE)

# Google Trends verisini oku
trends_df = pd.read_csv(TRENDS_FILE)

# Pipeline'ı başlat
pipeline = DataProcessingPipeline(log_path='data_processing/pipeline.log')

# Verileri pipeline'a ver
data_dict = {
    'gdelt': gdelt_df,
    'climate': climate_df,
    'trends': trends_df
}

# Pipeline'ı çalıştır
processed_data = pipeline.run_pipeline(data_dict)

# İşlenmiş verileri kaydet
output_dir = 'data_storage/processed'
pipeline.save_processed_data(output_dir)

print("Pipeline başarıyla çalıştı ve işlenmiş veriler kaydedildi.") 