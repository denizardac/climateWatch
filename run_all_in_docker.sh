#!/bin/bash

# Log ve çıktı kontrol fonksiyonu
date_str() { date '+%Y-%m-%d %H:%M:%S'; }
log_and_check_output() {
  step_name="$1"
  output_file="$2"
  log_file="$3"
  echo "[$(date_str)] $step_name tamamlandı." | tee -a "$log_file"
  if [ -f "$output_file" ]; then
    lines=$(wc -l < "$output_file")
    size=$(stat -c %s "$output_file")
    echo "Çıktı dosyası: $output_file | Satır: $lines | Boyut: $size bytes" | tee -a "$log_file"
  else
    echo "[UYARI] Çıktı dosyası bulunamadı: $output_file" | tee -a "$log_file"
  fi
}

# 1. Proje dizinine geç
cd /home/jovyan/climateWatch 2>/dev/null || cd /climateWatch 2>/dev/null || cd /workspace/climateWatch 2>/dev/null || cd /  # fallback

# 2. Gerekli klasörleri oluştur
mkdir -p data_storage/gdelt data_storage/climate data_storage/disasters data_storage/policies data_storage/summits data_storage/processed logs analysis_results

# 3. Gerekli Python paketlerini yükle
# pip install --user pymongo pyspark kaggle  # Artık gerek yok, Dockerfile requirements.txt ile kuruyor

# 4. Config dosyasındaki MongoDB ve Spark ayarlarını kontrol et
sed -i 's/host: localhost/host: mongodb/' config.yaml
echo "MongoDB host:"
grep "host:" config.yaml | grep mongodb
echo "Spark master:"
grep "master:" config.yaml | grep spark

# 5. Ingestion pipeline'ı çalıştır
step_log="logs/ingestion.log"
echo "[$(date_str)] Ingestion başlatılıyor..." | tee "$step_log"
export PYTHONPATH=.
python3 -m data_ingestion.run_ingestion 2>&1 | tee -a "$step_log"
log_and_check_output "Ingestion" "data_storage/gdelt/$(ls data_storage/gdelt | head -n1)" "$step_log"

# Kaggle ve UNFCCC COP işlemleri için kullanıcıya yol gösterici mesajlar
if [ ! -f "data_storage/summits/unfccc_cop_summits.csv" ]; then
  echo "[UYARI] UNFCCC COP zirveleri CSV dosyası eksik! Lütfen https://unfccc.int/process-and-meetings/conferences/past-conferences linkinden indirip data_storage/summits/unfccc_cop_summits.csv olarak kaydedin."
fi
if [ ! -f "~/.kaggle/kaggle.json" ]; then
  echo "[UYARI] Kaggle API anahtarı eksik! Lütfen https://www.kaggle.com/docs/api adresinden anahtarınızı alın ve ~/.kaggle/kaggle.json olarak ekleyin."
else
  echo "Kaggle veri seti indirmek ister misiniz? (e/h)"
  read kaggle_choice
  if [ "$kaggle_choice" = "e" ]; then
    echo "Lütfen indirmek istediğiniz Kaggle dataset adını girin (örn: zynicide/wine-reviews):"
    read kaggle_dataset
    kaggle datasets download -d $kaggle_dataset -p data_storage/kaggle/
    echo "Kaggle veri seti indirildi."
  fi
fi

# 6. Processing pipeline'ı çalıştır
step_log="logs/processing.log"
echo "[$(date_str)] Processing başlatılıyor..." | tee "$step_log"
export PYTHONPATH=.
python3 -m data_processing.run_processing 2>&1 | tee -a "$step_log"
log_and_check_output "Processing" "data_storage/processed/cleaned_gdelt.csv" "$step_log"

# 6.5 Scraping + NLP pipeline'ı çalıştır
step_log="logs/scraping_nlp.log"
echo "[$(date_str)] Scraping ve NLP başlatılıyor..." | tee "$step_log"
export PYTHONPATH=.
python3 -m data_processing.run_scraping_nlp 2>&1 | tee -a "$step_log"
log_and_check_output "Scraping+NLP" "data_storage/processed/articles_with_sentiment.csv" "$step_log"

# 7. Raporlama adımı
step_log="logs/reporting.log"
echo "[$(date_str)] Raporlama başlatılıyor..." | tee "$step_log"
export PYTHONPATH=.
python3 -m data_analysis.reporting 2>&1 | tee -a "$step_log"
log_and_check_output "Raporlama" "analysis_results/gdelt/summary.csv" "$step_log"

# 8. Sonuçları göster
echo "GDELT dosyaları:"
ls data_storage/gdelt

echo "Log dosyaları:"
ls logs/ 