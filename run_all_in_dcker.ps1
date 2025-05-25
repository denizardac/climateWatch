# PowerShell script for running the ClimateWatch pipeline with logging and output checks

function Log-And-CheckOutput {
    param(
        [string]$StepName,
        [string]$OutputFile,
        [string]$LogFile
    )
    $dateStr = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    "$dateStr $StepName tamamlandı." | Tee-Object -FilePath $LogFile -Append
    if (Test-Path $OutputFile) {
        $lines = (Get-Content $OutputFile | Measure-Object -Line).Lines
        $size = (Get-Item $OutputFile).Length
        "Çıktı dosyası: $OutputFile | Satır: $lines | Boyut: $size bytes" | Tee-Object -FilePath $LogFile -Append
    } else {
        "[UYARI] Çıktı dosyası bulunamadı: $OutputFile" | Tee-Object -FilePath $LogFile -Append
    }
}

$LogFile = "logs/pipeline_log.txt"

# Gerekli klasörleri oluştur
New-Item -ItemType Directory -Force -Path "data_storage/gdelt" | Out-Null
New-Item -ItemType Directory -Force -Path "data_storage/processed" | Out-Null
New-Item -ItemType Directory -Force -Path "data_storage/summits" | Out-Null
New-Item -ItemType Directory -Force -Path "analysis_results/gdelt" | Out-Null
New-Item -ItemType Directory -Force -Path "logs" | Out-Null

# Config dosyasını kontrol et
if (!(Test-Path "config.yaml")) {
    "HATA: config.yaml dosyası bulunamadı!" | Tee-Object -FilePath $LogFile -Append
    exit 1
}

# Ingestion
"Ingestion başlatılıyor..." | Tee-Object -FilePath $LogFile -Append
python -m data_ingestion.run_ingestion
Log-And-CheckOutput "Ingestion" "data_storage/gdelt" $LogFile

# UNFCCC COP zirveleri kontrolü
if (!(Test-Path "data_storage/summits/unfccc_cop_summits.csv")) {
    "[UYARI] UNFCCC COP zirveleri CSV dosyası eksik! Lütfen https://unfccc.int/process-and-meetings/conferences/past-conferences linkinden indirip data_storage/summits/unfccc_cop_summits.csv olarak kaydedin." | Tee-Object -FilePath $LogFile -Append
}

# Processing
"Processing başlatılıyor..." | Tee-Object -FilePath $LogFile -Append
python -m data_processing.run_processing
Log-And-CheckOutput "Processing" "data_storage/processed/cleaned_gdelt.csv" $LogFile

# Scraping ve NLP
"Scraping ve NLP başlatılıyor..." | Tee-Object -FilePath $LogFile -Append
python -m data_processing.run_scraping_nlp
Log-And-CheckOutput "Scraping+NLP" "data_storage/processed/articles_with_sentiment.csv" $LogFile

# Raporlama
"Raporlama başlatılıyor..." | Tee-Object -FilePath $LogFile -Append
python -m data_analysis.reporting
Log-And-CheckOutput "Raporlama" "analysis_results/gdelt/summary.csv" $LogFile

# Sonuçları göster
"GDELT dosyaları:" | Tee-Object -FilePath $LogFile -Append
Get-ChildItem -Path "data_storage/gdelt" | Out-String | Tee-Object -FilePath $LogFile -Append

"Log dosyaları:" | Tee-Object -FilePath $LogFile -Append
Get-ChildItem -Path "logs" | Out-String | Tee-Object -FilePath $LogFile -Append 