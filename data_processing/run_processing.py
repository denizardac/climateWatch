import os
import logging
from data_processing.clean_gdelt import clean_gdelt
from data_processing.clean_climate import clean_climate
from data_processing.clean_disasters import clean_disasters
from data_processing.clean_policies import clean_policies
from data_processing.clean_summits import clean_summits
from data_processing.clean_trends import clean_trends
from data_processing.clean_open_datasets import clean_open_datasets
from data_processing.clean_kaggle import clean_kaggle
from data_processing.clean_pdf import clean_pdf

def run_all_processing():
    try:
        clean_gdelt()
    except Exception as e:
        print(f"GDELT temizlik hatası: {e}")
    try:
        clean_climate()
    except Exception as e:
        print(f"İklim temizlik hatası: {e}")
    try:
        clean_disasters()
    except Exception as e:
        print(f"Afet temizlik hatası: {e}")
    try:
        clean_policies()
    except Exception as e:
        print(f"Politika temizlik hatası: {e}")
    try:
        clean_summits()
    except Exception as e:
        print(f"Zirve temizlik hatası: {e}")
    try:
        clean_trends()
    except Exception as e:
        print(f"Trends temizlik hatası: {e}")
    try:
        clean_open_datasets()
    except Exception as e:
        print(f"Open datasets temizlik hatası: {e}")
    try:
        clean_kaggle()
    except Exception as e:
        print(f"Kaggle temizlik hatası: {e}")
    try:
        clean_pdf()
    except Exception as e:
        print(f"PDF temizlik hatası: {e}")

if __name__ == '__main__':
    run_all_processing() 