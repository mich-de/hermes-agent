# Riavvio pulito di Hermes: chiude TUTTE le istanze hermes.py e ne lancia una sola.
# Uso: pwsh -NoProfile -File C:\hermes-agent\restart.ps1 [-Delay 15]

param([int]$Delay = 0)

$ErrorActionPreference = "SilentlyContinue"
$root = "C:\hermes-agent"
$pythonw = "C:\Users\mich-de\AppData\Local\Programs\Python\Python312-arm64\pythonw.exe"

if ($Delay -gt 0) { Start-Sleep -Seconds $Delay }

$killed = @()
Get-CimInstance Win32_Process |
    Where-Object { $_.Name -match '^pythonw?\.exe$' -and $_.CommandLine -like '*hermes.py*' } |
    ForEach-Object {
        $killed += $_.ProcessId
        Stop-Process -Id $_.ProcessId -Force
    }

Start-Sleep -Seconds 2
Start-Process -FilePath $pythonw -ArgumentList (Join-Path $root "hermes.py") -WorkingDirectory $root -WindowStyle Hidden
Write-Output ("chiusi: " + ($killed -join ", ") + " — avviata nuova istanza")
