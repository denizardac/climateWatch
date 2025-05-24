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

# Loglama ayarlarını güncelle
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
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
    
    # Duygu analizi uygula
    logger.info("Duygu analizi başlatılıyor...")
    
    # Her makale için ayrı ayrı duygu analizi yap ve logla
    sentiments = []
    for idx, row in articles_df.iterrows():
        try:
            sentiment = get_sentiment(row['text'])
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