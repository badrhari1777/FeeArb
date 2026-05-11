$ErrorActionPreference = "Stop"

$targetAddress = "127.0.0.1"
$targetPort = 8000

function Test-IsFeeArbUvicornProcess {
    param(
        [int]$ProcessId
    )
    $proc = Get-CimInstance Win32_Process -Filter "ProcessId=$ProcessId" -ErrorAction SilentlyContinue
    if (-not $proc) {
        return $false
    }

    $cmd = ""
    if ($null -ne $proc.CommandLine) {
        $cmd = [string]$proc.CommandLine
    }
    $name = ""
    if ($null -ne $proc.Name) {
        $name = [string]$proc.Name
    }
    $looksLikeUvicorn = ($cmd -match "(uvicorn(?:\.exe)?|-m\s+uvicorn|run_feearb_uvicorn\.py)") -or ($name -match "python|uvicorn")
    $isFeeArbApp = $cmd -match "run_feearb_uvicorn\.py|webapp\.app:app"

    return ($looksLikeUvicorn -and $isFeeArbApp)
}

function Get-FeeArbUvicornPids {
    return @(
        Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object { Test-IsFeeArbUvicornProcess -ProcessId $_.ProcessId } |
            Select-Object -ExpandProperty ProcessId -Unique
    )
}

$pids = @(Get-FeeArbUvicornPids)
$listenerPids = @(
    Get-NetTCPConnection -ErrorAction SilentlyContinue -LocalAddress $targetAddress -LocalPort $targetPort -State Listen |
        Select-Object -ExpandProperty OwningProcess -Unique
)
if (($pids.Count -eq 0) -and ($listenerPids.Count -eq 0)) {
    Write-Output "No FeeArb uvicorn process found"
    exit 0
}

$stopped = @()
$skipped = @()

foreach ($procId in $pids) {
    if ($procId -eq $PID) {
        $skipped += $procId
        continue
    }
    Stop-Process -Id $procId -Force -ErrorAction Stop
    $stopped += $procId
}

if ($stopped.Count -gt 0) {
    Write-Output ("Stopped FeeArb uvicorn PID(s): " + ($stopped -join ", "))
}

if ($skipped.Count -gt 0) {
    Write-Warning ("Skipped PID(s): {0}" -f ($skipped -join ", "))
}

Start-Sleep -Seconds 1

$remaining = @(
    Get-NetTCPConnection -ErrorAction SilentlyContinue -LocalAddress $targetAddress -LocalPort $targetPort -State Listen |
        Select-Object -ExpandProperty OwningProcess -Unique
)
if ($remaining.Count -gt 0) {
    Write-Warning ("Port still listening on {0}:{1}, PID(s): {2}" -f $targetAddress, $targetPort, ($remaining -join ", "))
} else {
    Write-Output ("Port {0}:{1} is free" -f $targetAddress, $targetPort)
}
