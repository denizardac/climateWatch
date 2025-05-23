import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
import logging

# Logging ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def analyze_countries():
    # İşlenmiş dosyaların klasörü
    processed_dir = "data_storage/gdelt_processed"
    
    # Tüm CSV dosyalarını birleştir
    all_data = []
    for file_name in os.listdir(processed_dir):
        if file_name.endswith('.csv'):
            file_path = os.path.join(processed_dir, file_name)
            df = pd.read_csv(file_path)
            all_data.append(df)
    
    if not all_data:
        logger.error("İşlenmiş dosya bulunamadı!")
        return
        
    # Tüm verileri birleştir
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # 1. En aktif ülkeler (Actor1 olarak)
    actor1_counts = combined_df['Actor1CountryCode'].value_counts().head(10)
    
    # 2. En çok bahsedilen ülkeler (Actor2 olarak)
    actor2_counts = combined_df['Actor2CountryCode'].value_counts().head(10)
    
    # 3. Ülke çiftleri analizi
    country_pairs = combined_df[['Actor1CountryCode', 'Actor2CountryCode']].dropna()
    country_pairs = country_pairs[country_pairs['Actor1CountryCode'] != country_pairs['Actor2CountryCode']]
    pair_counts = Counter(zip(country_pairs['Actor1CountryCode'], country_pairs['Actor2CountryCode']))
    top_pairs = dict(pair_counts.most_common(10))
    
    # Sonuçları göster
    print("\n1. En Aktif Ülkeler (Actor1):")
    for country, count in actor1_counts.items():
        print(f"{country}: {count} kez")
        
    print("\n2. En Çok Bahsedilen Ülkeler (Actor2):")
    for country, count in actor2_counts.items():
        print(f"{country}: {count} kez")
        
    print("\n3. En Sık İlişkili Ülke Çiftleri:")
    for (country1, country2), count in top_pairs.items():
        print(f"{country1} -> {country2}: {count} kez")
    
    # Görselleştirme
    plt.figure(figsize=(15, 10))
    
    # 1. En aktif ülkeler grafiği
    plt.subplot(2, 1, 1)
    actor1_counts.plot(kind='bar')
    plt.title('En Aktif Ülkeler (Actor1)')
    plt.xlabel('Ülke Kodu')
    plt.ylabel('Haber Sayısı')
    
    # 2. En çok bahsedilen ülkeler grafiği
    plt.subplot(2, 1, 2)
    actor2_counts.plot(kind='bar')
    plt.title('En Çok Bahsedilen Ülkeler (Actor2)')
    plt.xlabel('Ülke Kodu')
    plt.ylabel('Haber Sayısı')
    
    plt.tight_layout()
    plt.savefig('data_storage/analysis/country_analysis.png')
    logger.info("Grafik kaydedildi: data_storage/analysis/country_analysis.png")
    
    # Detaylı analiz için CSV dosyası oluştur
    analysis_df = pd.DataFrame({
        'Country': list(actor1_counts.index) + list(actor2_counts.index),
        'Count': list(actor1_counts.values) + list(actor2_counts.values),
        'Role': ['Actor1'] * len(actor1_counts) + ['Actor2'] * len(actor2_counts)
    })
    
    analysis_df.to_csv('data_storage/analysis/country_analysis.csv', index=False)
    logger.info("Analiz sonuçları kaydedildi: data_storage/analysis/country_analysis.csv")

if __name__ == "__main__":
    # Analiz klasörünü oluştur
    os.makedirs('data_storage/analysis', exist_ok=True)
    analyze_countries() 