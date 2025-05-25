#!/bin/bash# Log ve çıktı kontrol fonksiyonudate_str() { date '+%Y-%m-%d %H:%M:%S'; }log_and_check_output() {  step_name="$1"  output_file="$2"  log_file="$3"  echo "[$(date_str)] $step_name tamamlandı." | tee -a "$log_file"  if [ -f "$output_file" ]; then    lines=$(wc -l < "$output_file")    size=$(stat -c %s "$output_file")    echo "Çıktı dosyası: $output_file | Satır: $lines | Boyut: $size bytes" | tee -a "$log_file"  else    echo "[UYARI] Çıktı dosyası bulunamadı: $output_file" | tee -a "$log_file"  fi}# Proje dizinine geçcd /home/jovyan/work# PYTHONPATH ayarlaexport PYTHONPATH=/home/jovyan/work:$PYTHONPATH# Gerekli klasörleri oluşturmkdir -p data_storage/gdeltmkdir -p data_storage/processedmkdir -p data_storage/summitsmkdir -p analysis_results/gdeltmkdir -p logs# Config dosyasını kontrol etif [ ! -f config.yaml ]; then    echo "HATA: config.yaml dosyası bulunamadı!"    exit 1fi# MongoDB ve Spark ayarlarını kontrol etMONGO_HOST=$(grep "mongodb_host" config.yaml | cut -d'"' -f2)SPARK_MASTER=$(grep "spark_master" config.yaml | cut -d'"' -f2)echo "MongoDB host: $MONGO_HOST"echo "Spark master: $SPARK_MASTER"# Ingestionecho "[$(date_str)] Ingestion başlatılıyor..."python3 -m data_ingestion.run_ingestionecho "[$(date_str)] Ingestion tamamlandı."# Ingestion çıktılarını kontrol etif [ ! -d "data_storage/gdelt" ] || [ -z "$(ls -A data_storage/gdelt)" ]; then    echo "[UYARI] Çıktı dosyası bulunamadı: data_storage/gdelt/"fi# UNFCCC COP zirveleri kontrolüif [ ! -f "data_storage/summits/unfccc_cop_summits.csv" ]; then    echo "[UYARI] UNFCCC COP zirveleri CSV dosyası eksik! Lütfen https://unfccc.int/process-and-meetings/conferences/past-conferences linkinden indirip data_storage/summits/unfccc_cop_summits.csv olarak kaydedin."fi# Kaggle API anahtarı kontrolüif [ ! -f "/home/jovyan/.kaggle/kaggle.json" ]; then    echo "[UYARI] Kaggle API anahtarı eksik! Lütfen https://www.kaggle.com/docs/api adresinden anahtarınızı alın ve ~/.kaggle/kaggle.json olarak ekleyin."fi# Processingecho "[$(date_str)] Processing başlatılıyor..."python3 -m data_processing.run_processingecho "[$(date_str)] Processing tamamlandı."# Processing çıktılarını kontrol etif [ ! -f "data_storage/processed/cleaned_gdelt.csv" ]; then    echo "[UYARI] Çıktı dosyası bulunamadı: data_storage/processed/cleaned_gdelt.csv"fi# Scraping ve NLPecho "[$(date_str)] Scraping ve NLP başlatılıyor..."python3 -m data_processing.run_scraping_nlpecho "[$(date_str)] Scraping+NLP tamamlandı."# Scraping ve NLP çıktılarını kontrol etif [ ! -f "data_storage/processed/articles_with_sentiment.csv" ]; then    echo "[UYARI] Çıktı dosyası bulunamadı: data_storage/processed/articles_with_sentiment.csv"fi# Raporlamaecho "[$(date_str)] Raporlama başlatılıyor..."python3 -m data_analysis.reportingecho "[$(date_str)] Raporlama tamamlandı."# Raporlama çıktılarını kontrol etif [ ! -f "analysis_results/gdelt/summary.csv" ]; then    echo "[UYARI] Çıktı dosyası bulunamadı: analysis_results/gdelt/summary.csv"fi# Sonuçları gösterecho "GDELT dosyaları:"ls -l data_storage/gdelt/echo "Log dosyaları:"ls -l logs/ #!/bin/bash

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

# Proje dizinine geç
cd /home/jovyan/work

# PYTHONPATH ayarla
export PYTHONPATH=/home/jovyan/work:$PYTHONPATH

# Gerekli klasörleri oluştur
mkdir -p data_storage/gdelt
mkdir -p data_storage/processed
mkdir -p data_storage/summits
mkdir -p analysis_results/gdelt
mkdir -p logs

# Config dosyasını kontrol et
if [ ! -f config.yaml ]; then
    echo "HATA: config.yaml dosyası bulunamadı!"
    exit 1
fi

# MongoDB ve Spark ayarlarını kontrol et
MONGO_HOST=$(grep "mongodb_host" config.yaml | cut -d'"' -f2)
SPARK_MASTER=$(grep "spark_master" config.yaml | cut -d'"' -f2)

echo "MongoDB host: $MONGO_HOST"
echo "Spark master: $SPARK_MASTER"

# Ingestion
echo "[$(date_str)] Ingestion başlatılıyor..."
python3 -m data_ingestion.run_ingestion
echo "[$(date_str)] Ingestion tamamlandı."

# Ingestion çıktılarını kontrol et
if [ ! -d "data_storage/gdelt" ] || [ -z "$(ls -A data_storage/gdelt)" ]; then
    echo "[UYARI] Çıktı dosyası bulunamadı: data_storage/gdelt/"
fi

# UNFCCC COP zirveleri kontrolü
if [ ! -f "data_storage/summits/unfccc_cop_summits.csv" ]; then
    echo "[UYARI] UNFCCC COP zirveleri CSV dosyası eksik! Lütfen https://unfccc.int/process-and-meetings/conferences/past-conferences linkinden indirip data_storage/summits/unfccc_cop_summits.csv olarak kaydedin."
fi

# Kaggle API anahtarı kontrolü
if [ ! -f "/home/jovyan/.kaggle/kaggle.json" ]; then
    echo "[UYARI] Kaggle API anahtarı eksik! Lütfen https://www.kaggle.com/docs/api adresinden anahtarınızı alın ve ~/.kaggle/kaggle.json olarak ekleyin."
fi

# Processing
echo "[$(date_str)] Processing başlatılıyor..."
python3 -m data_processing.run_processing
echo "[$(date_str)] Processing tamamlandı."

# Processing çıktılarını kontrol et
if [ ! -f "data_storage/processed/cleaned_gdelt.csv" ]; then
    echo "[UYARI] Çıktı dosyası bulunamadı: data_storage/processed/cleaned_gdelt.csv"
fi

# Scraping ve NLP
echo "[$(date_str)] Scraping ve NLP başlatılıyor..."
python3 -m data_processing.run_scraping_nlp
echo "[$(date_str)] Scraping+NLP tamamlandı."

# Scraping ve NLP çıktılarını kontrol et
if [ ! -f "data_storage/processed/articles_with_sentiment.csv" ]; then
    echo "[UYARI] Çıktı dosyası bulunamadı: data_storage/processed/articles_with_sentiment.csv"
fi

# Raporlama
echo "[$(date_str)] Raporlama başlatılıyor..."
python3 -m data_analysis.reporting
echo "[$(date_str)] Raporlama tamamlandı."

# Raporlama çıktılarını kontrol et
if [ ! -f "analysis_results/gdelt/summary.csv" ]; then
    echo "[UYARI] Çıktı dosyası bulunamadı: analysis_results/gdelt/summary.csv"
fi

# Sonuçları göster
echo "GDELT dosyaları:"
ls -l data_storage/gdelt/

echo "Log dosyaları:"
ls -l logs/ 