import os
import pandas as pd
from data_processing.scraping import batch_scrape
from data_processing.nlp_pipeline import batch_sentiment
import logging

logging.basicConfig(level=logging.INFO)

def main():
    # 1. GDELT veya işlenmiş CSV'den URL'leri oku
    input_paths = [
        "data_storage/processed/combined_data.csv",  # işlenmiş veri
        "data_storage/gdelt/20230101.csv"           # örnek GDELT dosyası (gerekirse)
    ]
    urls = set()
    for path in input_paths:
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                if "SOURCEURL" in df.columns:
                    urls.update(df["SOURCEURL"].dropna().unique().tolist())
            except Exception as e:
                logging.warning(f"{path} okunamadı: {e}")
    urls = list(urls)
    if not urls:
        logging.error("Hiç URL bulunamadı!")
        return
    logging.info(f"Toplam {len(urls)} URL bulundu. İlk 10: {urls[:10]}")

    # 2. Scraping ve NLP pipeline'ı çalıştır
    articles = batch_scrape(urls, sleep_time=0.5)
    articles_df = pd.DataFrame(articles)
    articles_df["sentiment"] = batch_sentiment(articles_df["text"].tolist())

    # 3. Sonuçları kaydet
    output_path = "data_storage/processed/articles_with_sentiment.csv"
    articles_df.to_csv(output_path, index=False)
    logging.info(f"Sonuçlar {output_path} dosyasına kaydedildi.")

if __name__ == "__main__":
    main() 