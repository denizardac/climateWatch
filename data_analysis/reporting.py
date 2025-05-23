import os
import pandas as pd
import matplotlib.pyplot as plt
import logging

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def report_summary(input_file, output_dir, log_path):
    ensure_dir(output_dir)
    ensure_dir(os.path.dirname(log_path))
    logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    try:
        df = pd.read_csv(input_file)
        summary = df.describe(include='all')
        summary_path = os.path.join(output_dir, 'summary.csv')
        summary.to_csv(summary_path)
        logging.info(f"Özet tablo kaydedildi: {summary_path}")
        # Basit histogram/grafik örneği
        for col in df.select_dtypes(include=['float', 'int']).columns:
            plt.figure()
            df[col].hist(bins=30)
            plt.title(f"{col} Dağılımı")
            plt.xlabel(col)
            plt.ylabel('Frekans')
            fig_path = os.path.join(output_dir, f"hist_{col}.png")
            plt.savefig(fig_path)
            plt.close()
            logging.info(f"Histogram kaydedildi: {fig_path}")
    except Exception as e:
        logging.error(f"Raporlama hatası: {e}")

if __name__ == '__main__':
    # Örnek: GDELT raporu
    report_summary('data_storage/processed/cleaned_gdelt.csv', 'analysis_results/gdelt', 'logs/report_gdelt.log')
    # Diğer kaynaklar için de benzer şekilde çağrılabilir 