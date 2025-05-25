# UTF-8 encoding ayarla
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

# Docker servislerini kontrol et
Write-Host "Docker servisleri kontrol ediliyor..." -ForegroundColor Yellow

# Docker'ın çalışıp çalışmadığını kontrol et
$dockerStatus = docker info 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "Docker servisi çalışmıyor! Lütfen Docker Desktop'ı başlatın." -ForegroundColor Red
    exit 1
}

# Tüm gerekli servisleri kontrol et
$services = @(
    @{Name="mongodb"; Port="27017"},
    @{Name="spark"; Port="7077"},
    @{Name="kafka"; Port="9092"},
    @{Name="hdfs-namenode"; Port="9000"}
)

$allServicesRunning = $true
foreach ($service in $services) {
    $status = docker ps --filter "name=$($service.Name)" --format "{{.Status}}"
    if (-not $status) {
        Write-Host "$($service.Name) container'ı çalışmıyor! Başlatılıyor..." -ForegroundColor Yellow
        docker-compose up -d $($service.Name)
        Start-Sleep -Seconds 10
        
        # Servisin başlatılıp başlatılmadığını kontrol et
        $status = docker ps --filter "name=$($service.Name)" --format "{{.Status}}"
        if (-not $status) {
            Write-Host "$($service.Name) container'ı başlatılamadı!" -ForegroundColor Red
            $allServicesRunning = $false
        }
    }
}

if (-not $allServicesRunning) {
    Write-Host "`nBazı servisler başlatılamadı. Lütfen docker-compose.yml dosyasını kontrol edin." -ForegroundColor Red
    exit 1
}

# Servislerin durumunu göster
Write-Host "`nServis Durumları:" -ForegroundColor Green
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

Write-Host "`nTüm servisler hazır!" -ForegroundColor Green 