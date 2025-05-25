import os
import pandas as pd
import requests
from data_processing.nlp_pipeline import batch_sentiment, get_sentiment
import logging
from bs4 import BeautifulSoup
from datetime import datetime
import re
import time
from urllib.parse import urlparse
from transformers import pipeline
from typing import List, Dict

# Loglama ayarlarını güncelle
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/nlp_processing.log'
)
logger = logging.getLogger(__name__)

def extract_article_content(url: str) -> str:
    """
    URL'den makale içeriğini çeker
    """
    try:
        # User-Agent ekle
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        logger.info(f"Makale içeriği çekiliyor: {url}")
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        # HTML'i parse et
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Gereksiz elementleri kaldır
        for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
            element.decompose()
        
        # Makale içeriğini bul
        # Önce article tag'ini dene
        article = soup.find('article')
        if article:
            text = article.get_text(separator=' ', strip=True)
            logger.info(f"Makale içeriği article tag'inden alındı: {len(text)} karakter")
        else:
            # main tag'ini dene
            main = soup.find('main')
            if main:
                text = main.get_text(separator=' ', strip=True)
                logger.info(f"Makale içeriği main tag'inden alındı: {len(text)} karakter")
            else:
                # body tag'ini kullan
                text = soup.body.get_text(separator=' ', strip=True)
                logger.info(f"Makale içeriği body tag'inden alındı: {len(text)} karakter")
        
        # Metni temizle
        text = re.sub(r'\s+', ' ', text)  # Fazla boşlukları temizle
        text = text.strip()
        
        # İçerik uzunluğunu kontrol et
        if len(text) < 100:  # Çok kısa içerik
            logger.warning(f"Makale içeriği çok kısa ({len(text)} karakter): {url}")
            return ""
            
        logger.info(f"Makale içeriği başarıyla çekildi: {len(text)} karakter")
        return text
        
    except Exception as e:
        logger.error(f"Makale içeriği çekilemedi ({url}): {str(e)}")
        return ""

def fetch_gdelt_articles(query, start_date, end_date):
    """
    GDELT DOC 2.0 API'den makale verilerini çeker ve içeriklerini alır
    """
    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    
    params = {
        "query": query,
        "mode": "artlist",
        "format": "json",
        "maxrecords": 25
    }
    
    try:
        logger.info(f"GDELT API'ye istek yapılıyor: {query}")
        logger.info(f"API URL: {url}")
        logger.info(f"Parametreler: {params}")
        
        response = requests.get(url, params=params)
        logger.info(f"API Yanıt Kodu: {response.status_code}")
        response.raise_for_status()
        
        data = response.json()
        logger.info("JSON yanıtı başarıyla parse edildi")
        
        articles_data = data.get('articles', [])
        if not articles_data:
            logger.warning("Hiç makale bulunamadı!")
            return None
        
        articles = []
        for article in articles_data:
            article_url = article.get('url', '')
            if not article_url:
                continue
                
            # Makale içeriğini çek
            logger.info(f"Makale içeriği çekiliyor: {article_url}")
            content = extract_article_content(article_url)
            
            # Her istek arasında biraz bekle
            time.sleep(1)
            
            articles.append({
                'title': article.get('title', ''),
                'url': article_url,
                'source': article.get('domain', ''),
                'date': article.get('seendate', ''),
                'text': content
            })
        
        logger.info(f"{len(articles)} makale başarıyla çekildi")
        return pd.DataFrame(articles)
    except Exception as e:
        logger.error(f"API isteği başarısız: {str(e)}")
        logger.error(f"Hata detayı: {type(e).__name__}")
        return None

def chunk_text(text: str, max_length: int = 400) -> List[str]:
    """
    Metni belirli uzunlukta parçalara böler.
    
    Args:
        text (str): Bölünecek metin
        max_length (int): Maksimum parça uzunluğu
        
    Returns:
        List[str]: Metin parçaları
    """
    # Metni cümlelere böl
    sentences = re.split(r'[.!?]+', text)
    chunks = []
    current_chunk = []
    current_length = 0
    
    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue
            
        # Cümle çok uzunsa, kelimelere böl
        if len(sentence) > max_length:
            words = sentence.split()
            temp_chunk = []
            temp_length = 0
            
            for word in words:
                if temp_length + len(word) + 1 > max_length:
                    if temp_chunk:
                        chunks.append(' '.join(temp_chunk))
                    temp_chunk = [word]
                    temp_length = len(word)
                else:
                    temp_chunk.append(word)
                    temp_length += len(word) + 1
            
            if temp_chunk:
                chunks.append(' '.join(temp_chunk))
            continue
            
        if current_length + len(sentence) > max_length:
            if current_chunk:
                chunks.append(' '.join(current_chunk))
            current_chunk = [sentence]
            current_length = len(sentence)
        else:
            current_chunk.append(sentence)
            current_length += len(sentence)
    
    if current_chunk:
        chunks.append(' '.join(current_chunk))
    
    return chunks

def analyze_sentiment(text: str, sentiment_analyzer) -> float:
    """
    Metnin duygu analizini yapar.
    
    Args:
        text (str): Analiz edilecek metin
        sentiment_analyzer: Duygu analizi modeli
        
    Returns:
        float: Duygu skoru (-1 ile 1 arası)
    """
    try:
        # Metni parçalara böl
        chunks = chunk_text(text)
        
        # Her parça için duygu analizi yap
        sentiments = []
        for chunk in chunks:
            if len(chunk.strip()) > 0:
                try:
                    result = sentiment_analyzer(chunk)[0]
                    sentiments.append(result['score'] if result['label'] == 'POSITIVE' else -result['score'])
                except Exception as e:
                    logging.warning(f"Parça analizi hatası: {str(e)}")
                    continue
        
        # Parçaların ortalamasını al
        if sentiments:
            return sum(sentiments) / len(sentiments)
        else:
            logging.warning("Hiç sentiment değeri hesaplanamadı")
            return 0.0
        
    except Exception as e:
        logging.error(f"Duygu analizi hatası: {str(e)}")
        return 0.0

def process_gdelt_data(articles_df):
    """
    Makalelere duygu analizi uygular
    """
    # Boş metinleri kontrol et ve başlığı kullan
    empty_texts = articles_df['text'].isna() | (articles_df['text'] == '')
    if empty_texts.any():
        logger.warning(f"{empty_texts.sum()} makalenin metni boş! Başlıklar kullanılacak.")
        # Boş metinler için başlığı kullan
        articles_df.loc[empty_texts, 'text'] = articles_df.loc[empty_texts, 'title']
        # Boş metinlerin kaynaklarını logla
        empty_sources = articles_df.loc[empty_texts, 'source'].unique()
        logger.info(f"Boş metin içeren kaynaklar: {', '.join(empty_sources)}")
    
    # Duygu analizi uygula
    logger.info("Duygu analizi başlatılıyor...")
    
    # Duygu analizi modelini yükle
    logging.info("Duygu analizi modeli yükleniyor...")
    sentiment_analyzer = pipeline("sentiment-analysis", model="nlptown/bert-base-multilingual-uncased-sentiment")
    logging.info("Duygu analizi modeli başarıyla yüklendi")
    
    # Her makale için ayrı ayrı duygu analizi yap ve logla
    sentiments = []
    for idx, row in articles_df.iterrows():
        try:
            sentiment = analyze_sentiment(row['text'], sentiment_analyzer)
            logger.info(f"Makale {idx+1}: {row['title']} - Sentiment: {sentiment}")
            sentiments.append(sentiment)
        except Exception as e:
            logger.error(f"Makale {idx+1} için duygu analizi hatası: {str(e)}")
            sentiments.append(0.0)
    
    articles_df["sentiment"] = sentiments
    
    # Sentiment değerlerini kontrol et ve logla
    zero_sentiments = (articles_df["sentiment"] == 0.0).sum()
    if zero_sentiments > 0:
        logger.warning(f"{zero_sentiments} makalenin sentiment değeri 0.0!")
        # Hangi makalelerin sentiment değeri 0.0, logla
        zero_sentiment_articles = articles_df[articles_df["sentiment"] == 0.0]
        for _, article in zero_sentiment_articles.iterrows():
            logger.info(f"0.0 sentiment: {article['title']} ({article['source']})")
    
    # Sentiment istatistiklerini logla
    avg_sentiment = articles_df["sentiment"].mean()
    min_sentiment = articles_df["sentiment"].min()
    max_sentiment = articles_df["sentiment"].max()
    logger.info(f"Sentiment istatistikleri - Ortalama: {avg_sentiment:.3f}, Min: {min_sentiment:.3f}, Max: {max_sentiment:.3f}")
    
    return articles_df

def main():
    # GDELT'ten makaleleri çek
    query = "climate change"
    start_date = "2023-01-01"
    end_date = "2023-12-31"
    
    logger.info("Program başlatılıyor...")
    articles_df = fetch_gdelt_articles(query, start_date, end_date)
    
    if articles_df is not None:
        logger.info("Duygu analizi başlatılıyor...")
        articles_df = process_gdelt_data(articles_df)
        # Sonuçları kaydet
        output_path = "data_storage/processed/gdelt_articles_with_sentiment.csv"
        logger.info(f"Makaleler {output_path} dosyasına kaydediliyor...")
        articles_df.to_csv(output_path, index=False)
        logger.info(f"Makaleler başarıyla kaydedildi: {output_path}")
    else:
        logger.error("GDELT'ten veri çekilemedi!")

if __name__ == "__main__":
    main() 