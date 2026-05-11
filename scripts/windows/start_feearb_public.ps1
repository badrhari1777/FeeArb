$ErrorActionPreference = "Stop"

$repoRoot = "C:\Projects\FeeArb"
$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
$uvicornLauncher = Join-Path $repoRoot "scripts\windows\run_feearb_uvicorn.py"
$tailscaleExe = "C:\Program Files\Tailscale\tailscale.exe"
$caddyExe = "C:\Tools\caddy\caddy.exe"
$caddyfile = "C:\Tools\caddy\Caddyfile"
$feeArbApiUrl = "http://127.0.0.1:8000/api/settings"

function Test-ListeningPort {
    param(
        [int]$Port,
        [string]$Address = ""
    )
    if ($Address) {
        $conns = Get-NetTCPConnection -ErrorAction SilentlyContinue -LocalAddress $Address -LocalPort $Port -State Listen
    } else {
        $conns = Get-NetTCPConnection -ErrorAction SilentlyContinue -LocalPort $Port -State Listen
    }
    return (@($conns).Count -gt 0)
}

function Test-FeeArbApiReady {
    try {
        $resp = Invoke-WebRequest -UseBasicParsing -Uri $feeArbApiUrl -TimeoutSec 5
        return ($null -ne $resp -and $resp.StatusCode -eq 200)
    } catch {
        return $false
    }
}

function Get-FeeArbUvicornProcesses {
    return @(
        Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object {
                $_.CommandLine -match "run_feearb_uvicorn\.py|webapp\.app:app"
            }
    )
}

function Start-CaddyIfNeeded {
    if (Test-ListeningPort -Port 18080) {
        return
    }
    if (!(Test-Path $caddyExe)) { throw "Missing caddy.exe at $caddyExe" }
    if (!(Test-Path $caddyfile)) { throw "Missing Caddyfile at $caddyfile" }

    Start-Process -FilePath $caddyExe -WorkingDirectory (Split-Path $caddyExe) -ArgumentList @(
        "run",
        "--config", $caddyfile,
        "--adapter", "caddyfile"
    ) -WindowStyle Hidden

    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Seconds 1
        if (Test-ListeningPort -Port 18080) { return }
    }
    throw "Caddy did not start listening on port 18080"
}

function Start-FeeArbIfNeeded {
    if (Test-FeeArbApiReady) {
        return
    }
    if (!(Test-Path $venvPython)) { throw "Missing python.exe at $venvPython" }
    if (!(Test-Path $uvicornLauncher)) { throw "Missing uvicorn launcher at $uvicornLauncher" }

    $existing = @(Get-FeeArbUvicornProcesses)
    if ($existing.Count -gt 0 -and (Test-ListeningPort -Port 8000)) {
        throw ("FeeArb listener exists on 127.0.0.1:8000 but API is not healthy; existing PID(s): {0}" -f (($existing | Select-Object -ExpandProperty ProcessId) -join ", "))
    }

    $proc = Start-Process -FilePath $venvPython -WorkingDirectory $repoRoot -ArgumentList @(
        $uvicornLauncher
    ) -WindowStyle Hidden -PassThru

    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Seconds 1
        $current = Get-Process -Id $proc.Id -ErrorAction SilentlyContinue
        if (-not $current) {
            throw "FeeArb (uvicorn) exited before becoming healthy"
        }
        if (Test-FeeArbApiReady) { return }
    }
    throw "FeeArb (uvicorn) did not become healthy on /api/settings"
}

function Ensure-Funnel {
    if (!(Test-Path $tailscaleExe)) { throw "Missing tailscale.exe at $tailscaleExe" }

    # Tailscale service might still be coming up right after boot; retry for ~60s.
    for ($i = 0; $i -lt 20; $i++) {
        try {
            $status = & $tailscaleExe status 2>$null
            if ($LASTEXITCODE -eq 0 -and $status) { break }
        } catch {}
        Start-Sleep -Seconds 3
    }

    # Idempotent: if already configured, this should be a no-op.
    & $tailscaleExe funnel --yes --bg --https=443 127.0.0.1:18080 | Out-Null
}

Start-CaddyIfNeeded
Start-FeeArbIfNeeded
Ensure-Funnel

Write-Output "OK: FeeArb on 127.0.0.1:8000; Caddy on 127.0.0.1:18080; Funnel enforced"
