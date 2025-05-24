# Download winutils.exe and hadoop.dll for Windows
$hadoopBinDir = ".\hadoop\bin"
New-Item -ItemType Directory -Force -Path $hadoopBinDir

# Download URLs for Hadoop 3.3.1 binaries
$winutilsUrl = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.1/bin/winutils.exe"
$hadoopDllUrl = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.1/bin/hadoop.dll"

# Download files
Write-Host "Downloading Hadoop binaries..."
Invoke-WebRequest -Uri $winutilsUrl -OutFile "$hadoopBinDir\winutils.exe"
Invoke-WebRequest -Uri $hadoopDllUrl -OutFile "$hadoopBinDir\hadoop.dll"

Write-Host "Hadoop binaries downloaded successfully!"
Write-Host "HADOOP_HOME is set to: $env:HADOOP_HOME" 