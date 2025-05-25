"""
Küresel İklim Gündemi Büyük Veri Projesi: Tüm Veri Kaynakları Analizi
Bu script, MongoDB'deki tüm ana koleksiyonlardan (iklim, gdelt, disasters, policies, summits, trends, open_datasets, kaggle_data) verileri çekip,
temel özetler, trendler ve koleksiyonlar arası karşılaştırmalı analizler/görseller üretir.
Sunum ve raporlar için özetler ve görseller analysis_results klasörüne kaydedilir.
"""
import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
from collections import Counter

os.makedirs("analysis_results", exist_ok=True)

client = MongoClient("mongodb://localhost:27017/")
db = client["climatewatch"]

# 1. Koleksiyonları ve isimlerini tanımla
data_sources = {
    "climate_data": "İklim Verisi (Sıcaklık, CO2 vb.)",
    "gdelt_events": "GDELT Haber Eventleri",
    "disaster_events": "Doğal Afetler",
    "policy_changes": "İklim Politikası Değişiklikleri",
    "climate_summits": "İklim Zirveleri",
    "google_trends": "Google Trends",
    "open_datasets": "Açık Veri Setleri",
    "kaggle_data": "Kaggle Veri Setleri"
}

summary = {}

for col, label in data_sources.items():
    print(f"\n--- {label} ---")
    collection = db[col]
    cursor = collection.find({}, {"_id": 0})
    df = pd.DataFrame(list(cursor))
    summary[col] = df
    print(f"Toplam kayıt: {len(df)}")
    if len(df) == 0:
        continue
    # Temel istatistikler
    print(df.head())
    print(df.describe(include='all'))
    # Yıllık trend (eğer tarih varsa)
    for date_col in ["date", "year", "Date", "DATE"]:
        if date_col in df.columns:
            try:
                years = pd.to_datetime(df[date_col], errors='coerce').dt.year
                year_counts = years.value_counts().sort_index()
                plt.figure(figsize=(8,4))
                sns.lineplot(x=year_counts.index, y=year_counts.values, marker="o")
                plt.title(f"{label} - Yıllara Göre Kayıt Sayısı")
                plt.xlabel("Yıl")
                plt.ylabel("Kayıt Sayısı")
                plt.tight_layout()
                plt.savefig(f"analysis_results/{col}_yearly_trend.png")
                plt.close()
                print(f"Yıllık trend grafiği kaydedildi: analysis_results/{col}_yearly_trend.png")
            except Exception as e:
                print(f"Yıllık trend grafiği oluşturulamadı: {e}")
            break
    # En çok geçen kelimeler (haber/gdelt için)
    if col == "gdelt_events" and "27" in df.columns and "28" in df.columns:
        all_text = df["27"].astype(str).str.cat(df["28"].astype(str), sep=" ").str.lower()
        words = " ".join(all_text).split()
        most_common = Counter(words).most_common(30)
        print("En çok geçen 30 kelime:")
        for word, count in most_common:
            print(f"{word}: {count}")
        try:
            from wordcloud import WordCloud
            wordcloud = WordCloud(width=800, height=400, background_color='white').generate(" ".join(words))
            plt.figure(figsize=(12,6))
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.axis('off')
            plt.title("GDELT Eventlerinde En Çok Geçen Kelimeler")
            plt.tight_layout()
            plt.savefig("analysis_results/gdelt_wordcloud.png")
            plt.close()
            print("Kelime bulutu kaydedildi: analysis_results/gdelt_wordcloud.png")
        except ImportError:
            print("wordcloud kütüphanesi yüklü değil, kelime bulutu oluşturulamadı.")

# 2. Karşılaştırmalı Analiz: Medya vs. İklim
if len(summary["gdelt_events"]) > 0 and len(summary["climate_data"]) > 0:
    print("\n--- Medya İlgisi ve İklim Göstergeleri Korelasyonu ---")
    # Yıllık haber event sayısı
    gdelt = summary["gdelt_events"]
    if "date" in gdelt.columns:
        gdelt["year"] = pd.to_datetime(gdelt["date"], errors='coerce').dt.year
        gdelt_yearly = gdelt.groupby("year").size().reset_index(name="gdelt_event_count")
    else:
        gdelt_yearly = pd.DataFrame()
    # Yıllık ortalama sıcaklık (örnek: 'Mean' veya 'temperature' kolonu varsa)
    climate = summary["climate_data"]
    temp_col = None
    for c in ["Mean", "temperature", "mean", "Average", "avg"]:
        if c in climate.columns:
            temp_col = c
            break
    if temp_col and "Year" in climate.columns:
        climate_yearly = climate[["Year", temp_col]].rename(columns={"Year": "year"})
    elif temp_col and "year" in climate.columns:
        climate_yearly = climate[["year", temp_col]]
    else:
        climate_yearly = pd.DataFrame()
    # Birleştir ve korelasyon grafiği çiz
    if not gdelt_yearly.empty and not climate_yearly.empty:
        merged = pd.merge(gdelt_yearly, climate_yearly, on="year", how="inner")
        plt.figure(figsize=(10,5))
        ax1 = plt.gca()
        ax2 = ax1.twinx()
        ax1.plot(merged["year"], merged["gdelt_event_count"], color="tab:blue", marker="o", label="Haber Event Sayısı")
        ax2.plot(merged["year"], merged[temp_col], color="tab:red", marker="s", label="Ortalama Sıcaklık")
        ax1.set_xlabel("Yıl")
        ax1.set_ylabel("Haber Event Sayısı", color="tab:blue")
        ax2.set_ylabel(f"{temp_col}", color="tab:red")
        plt.title("Yıllık Medya İlgisi ve Ortalama Sıcaklık Karşılaştırması")
        plt.tight_layout()
        plt.savefig("analysis_results/media_vs_climate_correlation.png")
        plt.close()
        print("Medya ve iklim korelasyon grafiği kaydedildi: analysis_results/media_vs_climate_correlation.png")
        # Korelasyon katsayısı
        corr = merged["gdelt_event_count"].corr(merged[temp_col])
        print(f"Korelasyon katsayısı: {corr:.3f}")

print("\nTüm analizler tamamlandı. Sonuç görselleri 'analysis_results' klasörüne kaydedildi.") 