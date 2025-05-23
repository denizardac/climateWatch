import pandas as pd
import numpy as np
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from textblob import TextBlob
import spacy
from gensim import corpora, models
import logging
import os
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from collections import Counter

# Logging ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# NLTK gerekli dosyaları indir
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('averaged_perceptron_tagger')

class NLPProcessor:
    def __init__(self):
        # spaCy modelini yükle
        try:
            self.nlp = spacy.load('en_core_web_sm')
        except:
            logger.info("spaCy modeli bulunamadı, indiriliyor...")
            os.system('python -m spacy download en_core_web_sm')
            self.nlp = spacy.load('en_core_web_sm')
            
        self.lemmatizer = WordNetLemmatizer()
        self.stop_words = set(stopwords.words('english'))
        
    def process_news_data(self, news_df, output_dir):
        """
        Haber verilerini NLP teknikleriyle işler
        
        Args:
            news_df (pd.DataFrame): Haber verileri
            output_dir (str): Çıktı dizini
            
        Returns:
            pd.DataFrame: İşlenmiş veriler
        """
        try:
            # Metin temizleme
            news_df['cleaned_text'] = news_df['date'].astype(str)
            
            # Duygu analizi
            news_df['sentiment'] = news_df['cleaned_text'].apply(self.analyze_sentiment)
            
            # Anahtar kelime çıkarma
            news_df['keywords'] = news_df['cleaned_text'].apply(self.extract_keywords)
            
            # Yıl sütunu ekle
            news_df['year'] = pd.to_datetime(news_df['date']).dt.year
            
            # Kelime bulutu oluştur
            self.create_wordcloud(news_df['cleaned_text'].str.cat(sep=' '), output_dir)
            
            # Sonuçları kaydet
            news_df.to_csv(os.path.join(output_dir, 'processed_news.csv'), index=False)
            
            return news_df
            
        except Exception as e:
            logging.error(f"NLP işleme hatası: {str(e)}")
            return None
            
    def clean_text(self, text):
        """
        Metni temizler
        """
        if pd.isna(text):
            return ""
            
        # Küçük harfe çevir
        text = text.lower()
        
        # Tokenize
        tokens = word_tokenize(text)
        
        # Stop words ve noktalama işaretlerini kaldır
        tokens = [token for token in tokens if token.isalnum() and token not in self.stop_words]
        
        # Lemmatization
        tokens = [self.lemmatizer.lemmatize(token) for token in tokens]
        
        return ' '.join(tokens)
        
    def analyze_sentiment(self, text):
        """
        Metnin duygu analizini yapar
        """
        if not text:
            return 0
            
        blob = TextBlob(text)
        return blob.sentiment.polarity
        
    def extract_keywords(self, text):
        """
        Metinden anahtar kelimeleri çıkarır
        """
        if not text:
            return []
            
        # Tokenize ve temizle
        tokens = word_tokenize(text.lower())
        tokens = [token for token in tokens if token.isalnum() and token not in self.stop_words]
        
        # Kelime frekanslarını hesapla
        freq_dist = Counter(tokens)
        
        # En sık kullanılan 5 kelimeyi döndür
        return [word for word, _ in freq_dist.most_common(5)]
        
    def create_wordcloud(self, text, output_dir):
        """
        Kelime bulutu oluşturur
        """
        if not text:
            return
            
        wordcloud = WordCloud(
            width=800,
            height=400,
            background_color='white',
            max_words=100
        ).generate(text)
        
        plt.figure(figsize=(10, 5))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        plt.savefig(os.path.join(output_dir, 'wordcloud.png'))
        plt.close()

def main():
    # NLP işlemcisini başlat
    processor = NLPProcessor()
    
    # Örnek metin
    text = "Climate change is a pressing global issue that requires immediate action. Rising temperatures and extreme weather events are becoming more frequent."
    
    # Temizleme
    cleaned_text = processor.clean_text(text)
    print("\nTemizlenmiş metin:")
    print(cleaned_text)
    
    # Duygu analizi
    sentiment = processor.analyze_sentiment(text)
    print("\nDuygu analizi skoru:", sentiment)
    
    # Anahtar kelimeler
    keywords = processor.extract_keywords(text)
    print("\nAnahtar kelimeler:", keywords)
    
    # Konu modelleme
    texts = [
        "Climate change is causing global warming",
        "Renewable energy is the solution",
        "Carbon emissions must be reduced",
        "Extreme weather events are increasing",
        "Sustainable development is crucial"
    ]
    
    lda_model, dictionary, corpus = processor.perform_topic_modeling(texts)
    print("\nKonular:")
    for i in range(lda_model.num_topics):
        print(f"\nKonu {i+1}:")
        print(lda_model.print_topic(i))

if __name__ == "__main__":
    main() 