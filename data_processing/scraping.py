import requests
from bs4 import BeautifulSoup
import time
import logging
from typing import List, Dict, Optional

logging.basicConfig(level=logging.INFO)


def get_article_text(url: str, max_length: int = 2000, timeout: int = 10) -> Optional[str]:
    """
    Verilen URL'den haber metnini çeker. Hatalara karşı dayanıklıdır.
    """
    try:
        response = requests.get(url, timeout=timeout)
        if response.status_code != 200:
            logging.warning(f"URL {url} - Status code: {response.status_code}")
            return None
        soup = BeautifulSoup(response.content, "html.parser")
        paragraphs = soup.find_all("p")
        text = " ".join([p.get_text() for p in paragraphs])
        return text[:max_length] if text else None
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")
        return None

def batch_scrape(urls: List[str], sleep_time: float = 1.0, max_length: int = 2000) -> List[Dict[str, Optional[str]]]:
    """
    Birden fazla URL'den haber metni çeker. Her istek arasında bekleme ekler.
    """
    articles = []
    for url in urls:
        text = get_article_text(url, max_length=max_length)
        articles.append({"url": url, "text": text})
        time.sleep(sleep_time)
    return articles 