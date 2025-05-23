import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from scipy.signal import detrend
from statsmodels.tsa.seasonal import seasonal_decompose
import logging
import os
from nlp_processor import NLPProcessor
from datetime import datetime
from collections import Counter

# Logging ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TimeSeriesAnalyzer:
    def __init__(self):
        self.data_dir = "data_storage"
        self.output_dir = "analysis_results"
        self.nlp_processor = NLPProcessor()
        os.makedirs(self.output_dir, exist_ok=True)
        
    def load_data(self):
        """
        Tüm veri setlerini yükler ve birleştirir
        """
        try:
            # GDELT haber verilerini yükle
            news_files = [f for f in os.listdir(os.path.join(self.data_dir, "news")) if f.startswith("gdelt_")]
            news_dfs = []
            for file in news_files:
                df = pd.read_csv(os.path.join(self.data_dir, "news", file))
                news_dfs.append(df)
            news_df = pd.concat(news_dfs, ignore_index=True)
            news_df['date'] = pd.to_datetime(news_df['date'])
            
            # NASA sıcaklık verilerini yükle
            temp_df = pd.read_csv(os.path.join(self.data_dir, "climate", "nasa_temperature.csv"))
            if 'Year' in temp_df.columns:
                temp_df = temp_df.rename(columns={'Year': 'year'})
            if 'Temperature Anomaly' in temp_df.columns:
                temp_df = temp_df.rename(columns={'Temperature Anomaly': 'Temperature_Anomaly'})
            
            # NOAA CO2 verilerini yükle
            co2_df = pd.read_csv(os.path.join(self.data_dir, "climate", "noaa_co2.csv"))
            if 'Year' in co2_df.columns:
                co2_df = co2_df.rename(columns={'Year': 'year'})
            if 'CO2 (ppm)' in co2_df.columns:
                co2_df = co2_df.rename(columns={'CO2 (ppm)': 'CO2'})
            
            # Sütun isimlerini kontrol et ve logla
            logger.info(f"NASA sıcaklık verisi sütunları: {temp_df.columns.tolist()}")
            logger.info(f"NOAA CO2 verisi sütunları: {co2_df.columns.tolist()}")
            
            return news_df, temp_df, co2_df
            
        except Exception as e:
            logger.error(f"Veri yükleme hatası: {str(e)}")
            return None, None, None
            
    def clean_data(self, df):
        """
        Veri setini temizler
        """
        if df is None:
            return None
            
        # Sonsuz değerleri NaN ile değiştir
        df = df.replace([np.inf, -np.inf], np.nan)
        
        # Eksik değerleri doldur
        df = df.ffill()  # ileri doldurma
        df = df.bfill()  # geri doldurma
        
        # Hala eksik değer varsa 0 ile doldur
        df = df.fillna(0)
        
        # Sütun bazlı temizleme
        for col in df.columns:
            if col != 'year':  # yıl sütununu atla
                # Aykırı değerleri temizle
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                # Aykırı değerleri sınırlara çek
                df[col] = df[col].clip(lower=lower_bound, upper=upper_bound)
        
        return df
            
    def prepare_news_data(self, news_df):
        """
        Haber verilerini hazırlar
        """
        try:
            # Tarih sütununu datetime'a çevir
            news_df['date'] = pd.to_datetime(news_df['date'])
            
            # Yıllık istatistikleri hesapla
            yearly_stats = news_df.groupby(news_df['date'].dt.year).agg({
                'article_count': 'sum',  # Haber sayısı
                'avg_tone': 'mean'  # Ortalama ton
            })
            
            # Index'i sütun haline getir
            yearly_stats = yearly_stats.reset_index()
            yearly_stats = yearly_stats.rename(columns={'date': 'year'})
            
            # Eksik yılları doldur
            yearly_stats = yearly_stats.set_index('year')
            yearly_stats = yearly_stats.reindex(range(yearly_stats.index.min(), yearly_stats.index.max() + 1))
            yearly_stats = yearly_stats.ffill()  # ileri doldurma
            yearly_stats = yearly_stats.bfill()  # geri doldurma
            yearly_stats = yearly_stats.reset_index()
            
            logger.info(f"Temizlenen haber verisi boyutu: {yearly_stats.shape}")
            logger.info(f"Yıl aralığı: {yearly_stats['year'].min()} - {yearly_stats['year'].max()}")
            
            return yearly_stats
            
        except Exception as e:
            logger.error(f"Haber verisi hazırlama hatası: {str(e)}")
            return None
        
    def analyze_news_content(self, news_df):
        """
        Haber içeriklerini kelime bazlı analiz eder
        """
        if news_df is None or news_df.empty:
            logger.error("Haber verisi bulunamadı veya boş")
            return None
            
        try:
            # NLP işlemcisini kullan
            nlp_output_dir = os.path.join(self.output_dir, 'nlp_analysis')
            os.makedirs(nlp_output_dir, exist_ok=True)
            
            # Haber verilerini işle
            processed_news = self.nlp_processor.process_news_data(news_df, nlp_output_dir)
            
            if processed_news is not None:
                # Yıllık istatistikleri hesapla
                yearly_stats = processed_news.groupby('year').agg({
                    'sentiment': ['mean', 'std'],
                    'keywords': lambda x: list(set([k for sublist in x for k in sublist]))
                }).reset_index()
                
                # Sütun isimlerini düzelt
                yearly_stats.columns = [
                    'year',
                    'avg_sentiment',
                    'sentiment_std',
                    'keywords'
                ]
                
                # Sonuçları kaydet
                yearly_stats.to_csv(os.path.join(nlp_output_dir, 'yearly_nlp_stats.csv'), index=False)
                
                # Duygu analizi grafiği
                plt.figure(figsize=(12, 6))
                plt.plot(yearly_stats['year'], yearly_stats['avg_sentiment'], 'b-', label='Ortalama Duygu')
                plt.fill_between(
                    yearly_stats['year'],
                    yearly_stats['avg_sentiment'] - yearly_stats['sentiment_std'],
                    yearly_stats['avg_sentiment'] + yearly_stats['sentiment_std'],
                    alpha=0.2
                )
                plt.title('Yıllara Göre Haber Duygu Analizi')
                plt.xlabel('Yıl')
                plt.ylabel('Duygu Skoru')
                plt.legend()
                plt.savefig(os.path.join(nlp_output_dir, 'sentiment_trend.png'))
                plt.close()
                
                # Anahtar kelime analizi
                all_keywords = []
                for keywords in yearly_stats['keywords']:
                    all_keywords.extend(keywords)
                
                keyword_freq = Counter(all_keywords)
                top_keywords = pd.DataFrame(
                    keyword_freq.most_common(20),
                    columns=['keyword', 'frequency']
                )
                
                # Anahtar kelime grafiği
                plt.figure(figsize=(12, 6))
                sns.barplot(data=top_keywords, x='frequency', y='keyword')
                plt.title('En Sık Kullanılan 20 Anahtar Kelime')
                plt.xlabel('Kullanım Sıklığı')
                plt.ylabel('Anahtar Kelime')
                plt.tight_layout()
                plt.savefig(os.path.join(nlp_output_dir, 'top_keywords.png'))
                plt.close()
                
                # Sonuçları kaydet
                top_keywords.to_csv(os.path.join(nlp_output_dir, 'top_keywords.csv'), index=False)
                
                logger.info("NLP analizi tamamlandı ve sonuçlar kaydedildi.")
                return yearly_stats
                
        except Exception as e:
            logger.error(f"NLP analizi hatası: {str(e)}")
            return None
        
    def merge_datasets(self, news_stats, temp_df, co2_df):
        """
        Tüm veri setlerini birleştirir
        """
        if news_stats is None or temp_df is None or co2_df is None:
            logger.error("Birleştirilecek veri setlerinden biri eksik")
            return None
            
        # Veri setlerini temizle
        news_stats = self.clean_data(news_stats)
        temp_df = self.clean_data(temp_df)
        co2_df = self.clean_data(co2_df)
            
        # Tüm veri setlerini yıl bazında birleştir
        merged_df = pd.merge(news_stats, temp_df, on='year', how='outer')
        merged_df = pd.merge(merged_df, co2_df, on='year', how='outer')
        
        # Eksik değerleri doldur
        merged_df = merged_df.sort_values('year')
        merged_df = self.clean_data(merged_df)
        
        # Sütun isimlerini kontrol et
        required_columns = ['year', 'article_count', 'avg_tone', 'Temperature_Anomaly', 'CO2']
        missing_columns = [col for col in required_columns if col not in merged_df.columns]
        if missing_columns:
            logger.error(f"Eksik sütunlar: {missing_columns}")
            logger.info(f"Mevcut sütunlar: {merged_df.columns.tolist()}")
            return None
        
        return merged_df
        
    def calculate_correlations(self, merged_df, period=None):
        """
        Değişkenler arasındaki korelasyonları hesaplar
        """
        if merged_df is None:
            logger.error("Korelasyon hesaplanacak veri seti eksik")
            return None, None
            
        # Veriyi temizle
        merged_df = self.clean_data(merged_df)
        
        # Dönem filtreleme
        if period:
            start_year, end_year = period
            merged_df = merged_df[(merged_df['year'] >= start_year) & (merged_df['year'] <= end_year)]
            
        # Korelasyon matrisi
        corr_matrix = merged_df[['article_count', 'avg_tone', 'Temperature_Anomaly', 'CO2']].corr()
        
        # Korelasyon p-değerleri
        p_values = pd.DataFrame(index=corr_matrix.index, columns=corr_matrix.columns)
        for col1 in corr_matrix.columns:
            for col2 in corr_matrix.columns:
                try:
                    # Sadece her iki sütunda da sıfırdan farklı değerler varsa korelasyon hesapla
                    mask = (merged_df[col1] != 0) & (merged_df[col2] != 0)
                    if mask.sum() > 1:  # En az 2 veri noktası olmalı
                        corr, p_val = stats.pearsonr(merged_df.loc[mask, col1], merged_df.loc[mask, col2])
                        p_values.loc[col1, col2] = p_val
                    else:
                        p_values.loc[col1, col2] = np.nan
                except Exception as e:
                    logger.warning(f"Korelasyon hesaplama hatası ({col1}, {col2}): {str(e)}")
                    p_values.loc[col1, col2] = np.nan
                    
        p_values = p_values.astype(float)
        
        return corr_matrix, p_values
        
    def plot_time_series(self, merged_df):
        """
        Zaman serisi grafiklerini çizer
        """
        if merged_df is None:
            logger.error("Grafik çizilecek veri seti eksik")
            return
            
        # Veriyi temizle
        merged_df = self.clean_data(merged_df)
            
        # Grafik ayarları
        plt.style.use('seaborn-v0_8')
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('İklim Değişikliği ve Medya Analizi', fontsize=16)
        
        # Haber sayısı
        axes[0,0].plot(merged_df['year'], merged_df['article_count'], 'b-')
        axes[0,0].set_title('Yıllık Haber Sayısı')
        axes[0,0].set_xlabel('Yıl')
        axes[0,0].set_ylabel('Haber Sayısı')
        
        # Haber tonu
        axes[0,1].plot(merged_df['year'], merged_df['avg_tone'], 'g-')
        axes[0,1].set_title('Ortalama Haber Tonu')
        axes[0,1].set_xlabel('Yıl')
        axes[0,1].set_ylabel('Ton')
        
        # Sıcaklık anomalisi
        axes[1,0].plot(merged_df['year'], merged_df['Temperature_Anomaly'], 'r-')
        axes[1,0].set_title('Sıcaklık Anomalisi')
        axes[1,0].set_xlabel('Yıl')
        axes[1,0].set_ylabel('Anomali (°C)')
        
        # CO2 seviyesi
        axes[1,1].plot(merged_df['year'], merged_df['CO2'], 'k-')
        axes[1,1].set_title('CO2 Seviyesi')
        axes[1,1].set_xlabel('Yıl')
        axes[1,1].set_ylabel('CO2 (ppm)')
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'time_series.png'))
        plt.close()
        
    def plot_correlation_heatmap(self, corr_matrix, p_values, period=None):
        """
        Korelasyon ısı haritasını çizer
        """
        if corr_matrix is None or p_values is None:
            logger.error("Korelasyon matrisi veya p-değerleri eksik")
            return
            
        # NaN değerleri 0 ile doldur
        corr_matrix = corr_matrix.fillna(0)
        p_values = p_values.fillna(1)  # p-değeri olmayan korelasyonları maskele
            
        plt.figure(figsize=(10, 8))
        mask = p_values > 0.05  # p > 0.05 olan korelasyonları maskele
        
        title = 'Değişkenler Arası Korelasyonlar (p < 0.05)'
        if period:
            title += f' ({period[0]}-{period[1]})'
        
        sns.heatmap(corr_matrix, 
                   annot=True, 
                   cmap='coolwarm', 
                   center=0,
                   mask=mask,
                   fmt='.2f')
        
        plt.title(title)
        plt.tight_layout()
        
        filename = 'correlation_heatmap'
        if period:
            filename += f'_{period[0]}_{period[1]}'
        plt.savefig(os.path.join(self.output_dir, f'{filename}.png'))
        plt.close()
        
    def analyze_periods(self, merged_df):
        """
        Farklı zaman dilimlerinde analiz yapar
        """
        if merged_df is None:
            return
            
        # Tüm yılları al
        years = sorted(merged_df['year'].unique())
        
        # 5 yıllık dönemler oluştur
        periods = []
        for i in range(0, len(years), 5):
            if i + 4 < len(years):
                periods.append((years[i], years[i+4]))
                
        # Her dönem için analiz yap
        for period in periods:
            start_year, end_year = period
            logger.info(f"\nDönem analizi: {start_year}-{end_year}")
            
            # Korelasyonları hesapla
            corr_matrix, p_values = self.calculate_correlations(merged_df, period)
            
            # Korelasyon ısı haritasını çiz
            self.plot_correlation_heatmap(corr_matrix, p_values, period)
            
            # Sonuçları kaydet
            period_dir = os.path.join(self.output_dir, f'period_{start_year}_{end_year}')
            os.makedirs(period_dir, exist_ok=True)
            
            # NaN değerleri temizle
            corr_matrix = corr_matrix.fillna(0)
            p_values = p_values.fillna(1)  # p-değeri olmayan korelasyonları maskele
            
            corr_matrix.to_csv(os.path.join(period_dir, 'correlation_matrix.csv'))
            p_values.to_csv(os.path.join(period_dir, 'p_values.csv'))
            
            # Özet istatistikleri göster
            print(f"\n{start_year}-{end_year} Dönemi Korelasyon Matrisi:")
            print(corr_matrix)
            print(f"\n{start_year}-{end_year} Dönemi P-değerleri:")
            print(p_values)
        
    def analyze_seasonality(self, merged_df):
        """
        Mevsimsel etkileri analiz eder
        """
        if merged_df is None:
            return
            
        # Veriyi temizle
        merged_df = self.clean_data(merged_df)
        
        # Mevsimsel analiz için veriyi hazırla
        seasonal_data = merged_df[['year', 'Temperature_Anomaly', 'CO2', 'article_count', 'avg_tone']].copy()
        
        # Her değişken için mevsimsel analiz
        variables = ['Temperature_Anomaly', 'CO2', 'article_count', 'avg_tone']
        seasonal_results = {}
        
        for var in variables:
            try:
                # Mevsimsel ayrıştırma
                decomposition = seasonal_decompose(seasonal_data[var], period=5)
                
                # Trend ve mevsimsel bileşenleri kaydet
                seasonal_data[f'{var}_trend'] = decomposition.trend
                seasonal_data[f'{var}_seasonal'] = decomposition.seasonal
                seasonal_data[f'{var}_resid'] = decomposition.resid
                
                # Mevsimsellik gücü
                seasonal_strength = 1 - (np.var(decomposition.resid) / 
                                      np.var(decomposition.seasonal + decomposition.resid))
                
                seasonal_results[var] = {
                    'seasonal_strength': seasonal_strength,
                    'trend_strength': 1 - (np.var(decomposition.resid) / 
                                         np.var(decomposition.trend + decomposition.resid))
                }
                
            except Exception as e:
                logger.warning(f"Mevsimsel analiz hatası ({var}): {str(e)}")
                seasonal_results[var] = {
                    'seasonal_strength': np.nan,
                    'trend_strength': np.nan
                }
        
        # Mevsimsel grafikleri çiz
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('Değişkenlerin Mevsimsel Analizi', fontsize=16)
        
        for i, var in enumerate(variables):
            row = i // 2
            col = i % 2
            
            # Orijinal veri
            axes[row, col].plot(seasonal_data['year'], seasonal_data[var], 
                              label='Orijinal', alpha=0.5)
            
            # Trend
            axes[row, col].plot(seasonal_data['year'], seasonal_data[f'{var}_trend'], 
                              label='Trend', color='red')
            
            # Mevsimsel bileşen
            axes[row, col].plot(seasonal_data['year'], seasonal_data[f'{var}_seasonal'], 
                              label='Mevsimsel', color='green', alpha=0.5)
            
            # Mevsimsellik gücü
            strength = seasonal_results[var]['seasonal_strength']
            title = f'{var}\n'
            title += f'Mevsimsellik Gücü: {strength:.4f}'
            
            axes[row, col].set_title(title)
            axes[row, col].set_xlabel('Yıl')
            axes[row, col].legend()
            
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'seasonal_analysis.png'))
        plt.close()
        
        # Mevsimsel sonuçları kaydet
        seasonal_summary = pd.DataFrame(seasonal_results).T
        seasonal_summary.to_csv(os.path.join(self.output_dir, 'seasonal_analysis.csv'))
        
        return seasonal_summary
        
    def run_analysis(self):
        """
        Tüm analizleri çalıştırır
        """
        # Verileri yükle
        news_df, temp_df, co2_df = self.load_data()
        if news_df is None:
            logger.error("Veri yükleme başarısız")
            return
            
        # Haber verilerini hazırla
        news_stats = self.prepare_news_data(news_df)
        if news_stats is None:
            logger.error("Haber verisi hazırlama başarısız")
            return
            
        # Haber içeriklerini analiz et
        nlp_stats = self.analyze_news_content(news_df)
        if nlp_stats is not None:
            logger.info("NLP analizi başarıyla tamamlandı")
            
        # Veri setlerini birleştir
        merged_df = self.merge_datasets(news_stats, temp_df, co2_df)
        if merged_df is None:
            logger.error("Veri setleri birleştirme başarısız")
            return
            
        # Tüm dönem için korelasyonları hesapla
        corr_matrix, p_values = self.calculate_correlations(merged_df)
        if corr_matrix is None:
            logger.error("Korelasyon hesaplama başarısız")
            return
            
        # Grafikleri çiz
        self.plot_time_series(merged_df)
        self.plot_correlation_heatmap(corr_matrix, p_values)
        
        # Alt dönem analizlerini yap
        self.analyze_periods(merged_df)
        
        # Mevsimsel analiz yap
        seasonal_results = self.analyze_seasonality(merged_df)
        
        # Sonuçları kaydet
        merged_df.to_csv(os.path.join(self.output_dir, 'merged_data.csv'), index=False)
        corr_matrix.to_csv(os.path.join(self.output_dir, 'correlation_matrix.csv'))
        p_values.to_csv(os.path.join(self.output_dir, 'p_values.csv'))
        
        logger.info("Analiz tamamlandı ve sonuçlar kaydedildi.")
        
        # Özet istatistikleri göster
        print("\nTüm Dönem Özet İstatistikler:")
        print("\nKorelasyon Matrisi:")
        print(corr_matrix)
        print("\nP-değerleri:")
        print(p_values)
        print("\nMevsimsel Analiz Sonuçları:")
        print(seasonal_results)
        if nlp_stats is not None:
            print("\nNLP Analizi Sonuçları:")
            print(nlp_stats)

def main():
    analyzer = TimeSeriesAnalyzer()
    analyzer.run_analysis()

if __name__ == "__main__":
    main() 