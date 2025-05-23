import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta

def analyze_gdelt_data():
    """
    GDELT verilerini analiz eder ve görselleştirir.
    - Son 7 günün haber sayıları
    - Haberlerin ortalama tonu
    - Zaman içindeki değişimler
    """
    # GDELT verilerinin bulunduğu dizin
    gdelt_dir = 'data_storage/gdelt'
    
    # Tüm CSV dosyalarını oku
    all_data = []
    for file in os.listdir(gdelt_dir):
        if file.endswith('.csv'):
            file_path = os.path.join(gdelt_dir, file)
            df = pd.read_csv(file_path, low_memory=False)
            all_data.append(df)
    
    # Tüm verileri birleştir
    if not all_data:
        print("GDELT verisi bulunamadı!")
        return
    
    combined_data = pd.concat(all_data, ignore_index=True)
    
    # Tarih sütununu datetime'a çevir
    combined_data['date'] = pd.to_datetime(combined_data['date'], format='mixed', utc=True)
    
    # Son 7 günün verilerini filtrele
    end_date = combined_data['date'].max()
    start_date = end_date - timedelta(days=6)
    recent_data = combined_data[combined_data['date'].dt.date >= start_date.date()]
    
    # Günlük istatistikleri hesapla
    daily_stats = recent_data.groupby(recent_data['date'].dt.date).agg({
        'date': 'count',  # Haber sayısı
        'AvgTone': 'mean'  # Ortalama ton
    }).rename(columns={'date': 'news_count'})
    
    # Eksik günleri 0 ile doldur
    date_range = pd.date_range(start=start_date.date(), end=end_date.date(), freq='D')
    daily_stats = daily_stats.reindex(date_range, fill_value=0)
    
    # Görselleştirme
    plt.style.use('default')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    
    # Günlük haber sayıları
    bars = ax1.bar(daily_stats.index, daily_stats['news_count'], color='skyblue')
    ax1.set_title('Son 7 Günün İklim Değişikliği Haber Sayıları', pad=20, fontsize=12)
    ax1.set_xlabel('Tarih', fontsize=10)
    ax1.set_ylabel('Haber Sayısı', fontsize=10)
    ax1.tick_params(axis='x', rotation=45)
    
    # Bar üzerine değerleri yaz
    for bar in bars:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}',
                ha='center', va='bottom')
    
    # Ortalama ton
    ax2.plot(daily_stats.index, daily_stats['AvgTone'], marker='o', color='green', linestyle='-', linewidth=2)
    ax2.set_title('Son 7 Günün Haberlerin Ortalama Tonu', pad=20, fontsize=12)
    ax2.set_xlabel('Tarih', fontsize=10)
    ax2.set_ylabel('Ortalama Ton', fontsize=10)
    ax2.tick_params(axis='x', rotation=45)
    ax2.grid(True, linestyle='--', alpha=0.7)
    
    # Ton açıklaması
    ax2.axhline(y=0, color='r', linestyle='--', alpha=0.3)
    ax2.text(0.02, 0.98, 'Pozitif ton: Olumlu haberler\nNegatif ton: Olumsuz haberler', 
             transform=ax2.transAxes, verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    plt.tight_layout()
    
    # Grafikleri kaydet
    output_dir = 'data_analysis/visualizations'
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(os.path.join(output_dir, 'gdelt_analysis.png'), dpi=300, bbox_inches='tight')
    print(f"Grafikler kaydedildi: {os.path.join(output_dir, 'gdelt_analysis.png')}")
    
    # İstatistikleri yazdır
    print("\nSon 7 Günün İstatistikleri:")
    print(daily_stats)
    
    # Genel istatistikler
    print("\nGenel İstatistikler:")
    print(f"Toplam haber sayısı: {len(recent_data)}")
    print(f"Ortalama günlük haber sayısı: {daily_stats['news_count'].mean():.1f}")
    print(f"En çok haber olan gün: {daily_stats['news_count'].idxmax()} ({daily_stats['news_count'].max()} haber)")
    print(f"En az haber olan gün: {daily_stats['news_count'].idxmin()} ({daily_stats['news_count'].min()} haber)")
    print(f"Genel ortalama ton: {recent_data['AvgTone'].mean():.2f}")

if __name__ == '__main__':
    analyze_gdelt_data() 