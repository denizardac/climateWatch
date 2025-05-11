import requests
import zipfile
import io
import pandas as pd
from datetime import datetime, timedelta
import urllib3
import os

urllib3.disable_warnings()

def download_gdelt_csv(date: str):
    url = f"https://data.gdeltproject.org/gdeltv2/{date}000000.gkg.csv.zip"
    try:
        response = requests.get(url, verify=False, timeout=20)
        if response.status_code != 200:
            print(f"Dosya bulunamadı veya indirilemedi: {url}")
            return pd.DataFrame()
    except Exception as e:
        print(f"Hata oluştu: {e}")
        return pd.DataFrame()

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        filename = z.namelist()[0]
        with z.open(filename) as f:
            colnames = [
                "GKGRECORDID", "DATE", "SourceCollectionIdentifier", "SourceCommonName",
                "DocumentIdentifier", "Counts", "V2Counts", "Themes", "V2Themes",
                "Locations", "V2Locations", "Persons", "V2Persons", "Organizations",
                "V2Organizations", "V2Tone", "Dates", "GCAM", "SharingImage", "RelatedImages",
                "SocialImageEmbeds", "SocialVideoEmbeds", "Quotations", "AllNames", "Amounts",
                "TranslationInfo", "Extras"
            ]
            try:
                df = pd.read_csv(f, sep='\t', names=colnames, quoting=3, low_memory=False, encoding='utf-8')
            except UnicodeDecodeError:
                print(f"UTF-8 ile okunamıyor, atlanıyor: {date}")
                return pd.DataFrame()

    df = df[df['V2Themes'].str.contains('ENV_CLIMATECHANGE', na=False)]
    df = df[['DATE', 'DocumentIdentifier', 'V2Themes', 'V2Tone', 'V2Locations', 'V2Organizations']]
    return df

#  BAŞLANGIÇ / BİTİŞ TARİHİNİ BURADA BELİRLE
start_date = datetime.strptime("2024-11-01", "%Y-%m-%d")
end_date = datetime.strptime("2024-11-02", "%Y-%m-%d")

#  Verileri İndir ve Birleştir
all_dfs = []
current_date = end_date

while current_date >= start_date:
    date_str = current_date.strftime('%Y%m%d')
    print(f"İndiriliyor: {date_str}")
    df_day = download_gdelt_csv(date_str)
    if not df_day.empty:
        all_dfs.append(df_day)
    current_date -= timedelta(days=1)

#  CSV olarak kaydet
script_dir = os.path.dirname(os.path.abspath(__file__))

if all_dfs:
    result_df = pd.concat(all_dfs, ignore_index=True)
    out_file = os.path.join(script_dir, f"gdelt_climate_news_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.csv")
    result_df.to_csv(out_file, index=False)
    print(f"{len(result_df)} haber kaydedildi: {out_file}")
else:
    print("İklim temalı haber bulunamadı.")