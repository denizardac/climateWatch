from textblob import TextBlob
from typing import List, Optional

def get_sentiment(text: Optional[str]) -> Optional[float]:
    """
    Verilen metnin duygu skorunu döndürür. İngilizce için uygundur.
    """
    if text:
        blob = TextBlob(text)
        return blob.sentiment.polarity
    return None

def batch_sentiment(texts: List[Optional[str]]) -> List[Optional[float]]:
    """
    Birden fazla metin için duygu skorlarını toplu olarak döndürür.
    """
    return [get_sentiment(t) for t in texts] 