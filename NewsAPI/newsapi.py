import requests
import pandas as pd
import os
from datetime import datetime, timedelta

API_KEY = '0ab7c37685cb4c978166623751d88b06'
QUERY = 'climate NOT finance'
PAGE_SIZE = 100
BASE_URL = 'https://newsapi.org/v2/everything'

# Manuel tarih aralığı tanımı
FROM_DATE = '2025-04-11'  # Başlangıç tarihi (YYYY-MM-DD)
TO_DATE = '2025-04-15'    # Bitiş tarihi (YYYY-MM-DD)

# Kayıt klasörü
SAVE_DIR = 'NewsAPI'
os.makedirs(SAVE_DIR, exist_ok=True)

all_articles = []

for page in range(1, 6):
    params = {
        'q': QUERY,
        'from': FROM_DATE,
        'to': TO_DATE,
        'language': 'en',
        'sortBy': 'relevancy',
        'pageSize': PAGE_SIZE,
        'page': page,
        'apiKey': API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()

    if data.get('status') != 'ok':
        print("Hata oluştu:", data)
        break

    articles = data.get('articles', [])
    if not articles:
        break

    all_articles.extend(articles)

df = pd.DataFrame(all_articles)

if not df.empty:
    df['source'] = df['source'].apply(lambda x: x['name'])
    df = df[['source', 'author', 'title', 'description', 'content', 'publishedAt', 'url']]
    csv_path = os.path.join(SAVE_DIR, 'climate_change_news_recent.csv')
    df.to_csv(csv_path, index=False)
    print(f"{len(df)} haber başarıyla kaydedildi: {csv_path}")
else:
    print("Hiç veri bulunamadı.")