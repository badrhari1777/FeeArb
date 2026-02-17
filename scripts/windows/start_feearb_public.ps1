$ErrorActionPreference = "Stop"

$repoRoot = "C:\Projects\FeeArb"
$venvUvicorn = Join-Path $repoRoot ".venv\Scripts\uvicorn.exe"
$tailscaleExe = "C:\Program Files\Tailscale\tailscale.exe"
$caddyExe = "C:\Tools\caddy\caddy.exe"
$caddyfile = "C:\Tools\caddy\Caddyfile"

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
    if (Test-ListeningPort -Port 8000) {
        return
    }
    if (!(Test-Path $venvUvicorn)) { throw "Missing uvicorn.exe at $venvUvicorn" }

    Start-Process -FilePath $venvUvicorn -WorkingDirectory $repoRoot -ArgumentList @(
        "webapp.app:app",
        "--host", "127.0.0.1",
        "--port", "8000"
    ) -WindowStyle Hidden

    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Seconds 1
        if (Test-ListeningPort -Port 8000) { return }
    }
    throw "FeeArb (uvicorn) did not start listening on port 8000"
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
