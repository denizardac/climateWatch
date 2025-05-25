# Test pipeline script for ClimateWatch
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
$PSDefaultParameterValues['*:Encoding'] = 'utf8'

# Python modül yolunu ayarla
$env:PYTHONPATH = $PWD.Path

$LogFile = "logs/test_pipeline_log.txt"

# Log klasörünü oluştur
New-Item -ItemType Directory -Force -Path "logs" | Out-Null

# Test veri klasörlerini temizle
Remove-Item -Path "data_storage/raw/*" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -Path "data_storage/processed/*" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -Path "data_storage/analysis/*" -Recurse -Force -ErrorAction SilentlyContinue

# Test klasörlerini oluştur
New-Item -ItemType Directory -Force -Path "data_storage/raw" | Out-Null
New-Item -ItemType Directory -Force -Path "data_storage/processed" | Out-Null
New-Item -ItemType Directory -Force -Path "data_storage/analysis" | Out-Null

Write-Host "Test pipeline başlatılıyor..." -ForegroundColor Green
Write-Host "PYTHONPATH: $env:PYTHONPATH" -ForegroundColor Yellow
Write-Host "Log dosyası: $LogFile" -ForegroundColor Yellow

# Docker servislerini kontrol et
Write-Host "`nDocker servisleri kontrol ediliyor..." -ForegroundColor Cyan

# Docker'ın çalışıp çalışmadığını kontrol et
$dockerStatus = docker info 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "Docker servisi çalışmıyor! Lütfen Docker Desktop'ı başlatın." -ForegroundColor Red
    exit 1
}

# Tüm gerekli servisleri kontrol et ve başlat
$services = @("mongodb", "spark", "kafka", "hdfs-namenode")
$allServicesRunning = $true

foreach ($service in $services) {
    $status = docker ps --filter "name=$service" --format "{{.Status}}"
    if ($status) {
        Write-Host "$service çalışıyor: $status" -ForegroundColor Green
    } else {
        Write-Host "$service çalışmıyor! Başlatılıyor..." -ForegroundColor Yellow
        docker-compose up -d $service
        Start-Sleep -Seconds 10
        
        # Servisin başlatılıp başlatılmadığını kontrol et
        $status = docker ps --filter "name=$service" --format "{{.Status}}"
        if ($status) {
            Write-Host "$service başarıyla başlatıldı: $status" -ForegroundColor Green
        } else {
            Write-Host "$service başlatılamadı!" -ForegroundColor Red
            $allServicesRunning = $false
        }
    }
}

if (-not $allServicesRunning) {
    Write-Host "`nBazı servisler başlatılamadı. Lütfen docker-compose.yml dosyasını kontrol edin." -ForegroundColor Red
    Write-Host "Tüm servisleri başlatmak için: docker-compose up -d" -ForegroundColor Yellow
    exit 1
}

# Test fonksiyonları
function Test-Ingestion {
    Write-Host "`n=== Veri Toplama Testi ===`n"
    Write-Host "Veri toplama testi başlatılıyor..."
    try {
        python data_ingestion/run_ingestion.py
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Veri toplama başarılı!"
            return $true
        } else {
            Write-Host "Veri toplama başarısız!"
            return $false
        }
    } catch {
        Write-Host "Veri toplama hatası: $_"
        return $false
    }
}

function Test-Processing {
    Write-Host "`n=== Veri İşleme Testi ===`n"
    Write-Host "Veri işleme testi başlatılıyor..."
    try {
        python data_processing/run_processing.py
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Veri işleme başarılı!"
            return $true
        } else {
            Write-Host "Veri işleme başarısız!"
            return $false
        }
    } catch {
        Write-Host "Veri işleme hatası: $_"
        return $false
    }
}

function Test-NLP {
    Write-Host "`n=== NLP İşleme Testi ===`n"
    Write-Host "NLP işleme testi başlatılıyor..."
    try {
        python data_processing/run_scraping_nlp.py
        if ($LASTEXITCODE -eq 0) {
            Write-Host "NLP işleme başarılı!"
            return $true
        } else {
            Write-Host "NLP işleme başarısız!"
            return $false
        }
    } catch {
        Write-Host "NLP işleme hatası: $_"
        return $false
    }
}

function Test-Reporting {
    Write-Host "`n=== Raporlama Testi ===`n"
    Write-Host "Raporlama testi başlatılıyor..."
    try {
        python reporting/generate_reports.py
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Raporlama başarılı!"
            return $true
        } else {
            Write-Host "Raporlama başarısız!"
            return $false
        }
    } catch {
        Write-Host "Raporlama hatası: $_"
        return $false
    }
}

# Ana test döngüsü
$tests = @(
    @{Name="Veri Toplama"; Test={Test-Ingestion}},
    @{Name="Veri İşleme"; Test={Test-Processing}},
    @{Name="NLP İşleme"; Test={Test-NLP}},
    @{Name="Raporlama"; Test={Test-Reporting}}
)

$success = $true
foreach ($test in $tests) {
    Write-Host "`n=== $($test.Name) Testi ===" -ForegroundColor Cyan
    if (-not (& $test.Test)) {
        $success = $false
        Write-Host "`n$($test.Name) testi başarısız oldu. Diğer testler durduruluyor..." -ForegroundColor Red
        break
    }
}

# Sonuç raporu
Write-Host "`n=== Test Sonuçları ===" -ForegroundColor Cyan
if ($success) {
    Write-Host "Tüm testler başarıyla tamamlandı!" -ForegroundColor Green
} else {
    Write-Host "Bazı testler başarısız oldu. Lütfen hata mesajlarını kontrol edin." -ForegroundColor Red
}

# Sonuçları göster
Write-Host "`nTest sonuçları:" -ForegroundColor Magenta
Write-Host "`nGDELT dosyaları:" -ForegroundColor Yellow
Get-ChildItem -Path "data_storage/raw" -Filter "*.csv" | Format-Table Name, Length, LastWriteTime

Write-Host "`nİşlenmiş dosyalar:" -ForegroundColor Yellow
Get-ChildItem -Path "data_storage/processed" -Filter "*.csv" | Format-Table Name, Length, LastWriteTime

Write-Host "`nAnaliz sonuçları:" -ForegroundColor Yellow
Get-ChildItem -Path "data_storage/analysis" -Filter "*.csv" | Format-Table Name, Length, LastWriteTime

Write-Host "`nTest tamamlandı!" -ForegroundColor Green 