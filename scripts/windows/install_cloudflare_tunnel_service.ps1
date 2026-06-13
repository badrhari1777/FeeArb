$ErrorActionPreference = "Stop"

$repoRoot = "C:\Projects\FeeArb"
$cloudflaredExe = "C:\Program Files (x86)\cloudflared\cloudflared.exe"
$tunnelId = "b848b4d9-bb7c-47ff-858b-ef025d2d469f"
$sourceConfig = Join-Path $repoRoot "config\cloudflared\feearb.yml"
$sourceCredentials = "C:\Users\Pavel\.cloudflared\$tunnelId.json"
$sourceCert = "C:\Users\Pavel\.cloudflared\cert.pem"
$systemDir = "C:\Windows\System32\config\systemprofile\.cloudflared"
$systemConfig = Join-Path $systemDir "config.yml"

if (!(Test-Path $cloudflaredExe)) { throw "Missing cloudflared.exe at $cloudflaredExe" }
if (!(Test-Path $sourceConfig)) { throw "Missing tunnel config at $sourceConfig" }
if (!(Test-Path $sourceCredentials)) { throw "Missing tunnel credentials at $sourceCredentials" }

New-Item -ItemType Directory -Path $systemDir -Force | Out-Null
Copy-Item -LiteralPath $sourceConfig -Destination $systemConfig -Force
Copy-Item -LiteralPath $sourceCredentials -Destination (Join-Path $systemDir "$tunnelId.json") -Force
if (Test-Path $sourceCert) {
    Copy-Item -LiteralPath $sourceCert -Destination (Join-Path $systemDir "cert.pem") -Force
}

$service = Get-Service -Name "Cloudflared" -ErrorAction SilentlyContinue
if (-not $service) {
    & $cloudflaredExe service install
    $service = Get-Service -Name "Cloudflared"
}

if ($service.Status -ne "Stopped") {
    Stop-Service -Name "Cloudflared" -Force
    $service.WaitForStatus("Stopped", [TimeSpan]::FromSeconds(30))
}

$imagePath = "`"$cloudflaredExe`" --config=$systemConfig tunnel run"
Set-ItemProperty -LiteralPath "HKLM:\SYSTEM\CurrentControlSet\Services\Cloudflared" -Name ImagePath -Value $imagePath
Set-Service -Name "Cloudflared" -StartupType Automatic
Start-Service -Name "Cloudflared"
$service.WaitForStatus("Running", [TimeSpan]::FromSeconds(30))

Write-Output "OK: Cloudflared service is running and set to Automatic"
