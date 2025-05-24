import logging
from transformers import pipeline
import numpy as np

# Loglama ayarlarını güncelle
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Duygu analizi modelini yükle
try:
    sentiment_analyzer = pipeline("sentiment-analysis", model="nlptown/bert-base-multilingual-uncased-sentiment")
    logger.info("Duygu analizi modeli başarıyla yüklendi")
except Exception as e:
    logger.error(f"Model yüklenirken hata oluştu: {str(e)}")
    raise

def split_text(text: str, max_length: int = 500) -> list:
    """
    Metni belirtilen maksimum uzunluğa göre parçalara böler
    """
    if not text:
        return []
        
    # Metni kelimelere ayır
    words = text.split()
    chunks = []
    current_chunk = []
    current_length = 0
    
    for word in words:
        # Her kelime yaklaşık 4 token olarak hesaplanıyor
        word_length = len(word.split()) * 4
        
        if current_length + word_length > max_length:
            # Mevcut parçayı kaydet
            chunks.append(' '.join(current_chunk))
            current_chunk = [word]
            current_length = word_length
        else:
            current_chunk.append(word)
            current_length += word_length
    
    # Son parçayı ekle
    if current_chunk:
        chunks.append(' '.join(current_chunk))
    
    return chunks

def get_sentiment(text: str) -> float:
    """
    Tek bir metin için duygu analizi yapar
    """
    if not text:
        return 0.0
        
    try:
        # Metni parçalara böl
        chunks = split_text(text)
        if not chunks:
            return 0.0
            
        # Her parça için duygu analizi yap
        sentiments = []
        for chunk in chunks:
            result = sentiment_analyzer(chunk)[0]
            # 1-5 arası skoru -1 ile 1 arasına normalize et
            score = (int(result['label'].split()[0]) - 3) / 2
            sentiments.append(score)
        
        # Parçaların ortalamasını al
        return np.mean(sentiments)
        
    except Exception as e:
        logger.error(f"Duygu analizi hatası: {str(e)}")
        return 0.0

def batch_sentiment(texts: list) -> list:
    """
    Metin listesi için toplu duygu analizi yapar
    """
    return [get_sentiment(text) for text in texts] 