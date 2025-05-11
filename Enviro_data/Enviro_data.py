# climate_data/fetch_gistemp.py

import pandas as pd
import requests
from io import StringIO
import os

# Manuel tarih aralığı tanımı
START_YEAR = 1970
END_YEAR = 2020

def fetch_gistemp_global_temperature_anomalies():
    url = "https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception("Veri alınamadı: HTTP {}".format(response.status_code))

    content = response.content.decode("utf-8")
    df = pd.read_csv(StringIO(content), skiprows=1)
    df.columns = df.columns.str.strip()
    df = df.drop(columns=[col for col in df.columns if 'J-D' not in col and 'Year' not in col])
    df = df.rename(columns={'Year': 'year', 'J-D': 'temp_anomaly'})
    df['temp_anomaly'] = pd.to_numeric(df['temp_anomaly'], errors='coerce')
    df = df.dropna(subset=['temp_anomaly'])

    df = df[(df['year'] >= START_YEAR) & (df['year'] <= END_YEAR)]
    return df[['year', 'temp_anomaly']]

def fetch_co2_emissions():
    url = "https://datahub.io/core/co2-fossil-global/r/global.csv"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("CO2 verisi alınamadı: HTTP {}".format(response.status_code))

    df = pd.read_csv(StringIO(response.content.decode("utf-8")))
    df = df.rename(columns={'Year': 'year', 'Total': 'co2_emissions'})
    df = df[['year', 'co2_emissions']]
    df = df[(df['year'] >= START_YEAR) & (df['year'] <= END_YEAR)]
    return df.dropna()

def fetch_sea_level_data():
    url = "https://datahub.io/core/sea-level-rise/r/epa-sea-level.csv"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("Deniz seviyesi verisi alınamadı: HTTP {}".format(response.status_code))

    df = pd.read_csv(StringIO(response.content.decode("utf-8")))
    df = df.rename(columns={'Year': 'year', 'CSIRO Adjusted Sea Level': 'sea_level'})
    df = df[['year', 'sea_level']]
    df = df[(df['year'] >= START_YEAR) & (df['year'] <= END_YEAR)]
    return df.dropna()

def merge_datasets(temp_df, co2_df, sea_level_df):
    merged = pd.merge(temp_df, co2_df, on='year', how='outer')
    merged = pd.merge(merged, sea_level_df, on='year', how='outer')
    return merged.sort_values(by='year')

if __name__ == "__main__":
    temp_df = fetch_gistemp_global_temperature_anomalies()
    co2_df = fetch_co2_emissions()
    sea_df = fetch_sea_level_data()
    merged_df = merge_datasets(temp_df, co2_df, sea_df)
    output_path = os.path.join("Enviro_data", f"merged_climate_data_{START_YEAR}_{END_YEAR}.csv")
    merged_df.to_csv(output_path, index=False)
    print(f"Veriler {output_path} dosyasına yazıldı.")