$ErrorActionPreference = "Stop"

$targetAddress = "127.0.0.1"
$targetPort = 8000

function Get-ListenerPids {
    param(
        [string]$Address,
        [int]$Port
    )
    $conns = Get-NetTCPConnection -ErrorAction SilentlyContinue -LocalAddress $Address -LocalPort $Port -State Listen
    return @($conns | Select-Object -ExpandProperty OwningProcess -Unique)
}

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
    $looksLikeUvicorn = ($cmd -match "(uvicorn(?:\.exe)?|-m\s+uvicorn)") -or ($name -match "python|uvicorn")
    $isFeeArbApp = $cmd -match "webapp\.app:app"

    return ($looksLikeUvicorn -and $isFeeArbApp)
}

$pids = Get-ListenerPids -Address $targetAddress -Port $targetPort
if (-not $pids -or $pids.Count -eq 0) {
    Write-Output "No listener on $targetAddress`:$targetPort"
    exit 0
}

$stopped = @()
$skipped = @()

foreach ($procId in $pids) {
    if (Test-IsFeeArbUvicornProcess -ProcessId $procId) {
        Stop-Process -Id $procId -Force -ErrorAction Stop
        $stopped += $procId
    } else {
        $skipped += $procId
    }
}

if ($stopped.Count -gt 0) {
    Write-Output ("Stopped FeeArb uvicorn PID(s): " + ($stopped -join ", "))
}

if ($skipped.Count -gt 0) {
    Write-Warning ("Skipped non-uvicorn PID(s) on {0}:{1}: {2}" -f $targetAddress, $targetPort, ($skipped -join ", "))
}

$remaining = Get-ListenerPids -Address $targetAddress -Port $targetPort
if ($remaining.Count -gt 0) {
    Write-Warning ("Port still listening on {0}:{1}, PID(s): {2}" -f $targetAddress, $targetPort, ($remaining -join ", "))
} else {
    Write-Output ("Port {0}:{1} is free" -f $targetAddress, $targetPort)
}
