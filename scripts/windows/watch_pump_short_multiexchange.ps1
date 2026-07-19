param(
    [string]$StartDate = "2024-01-01",
    [double]$SleepSec = 0.05,
    [string]$OutputRoot = "data\research\pump_short_multiexchange_2024_clean",
    [int]$IntervalSec = 3600,
    [string]$Exchanges = "binance,bybit,okx,bitget,mexc,kucoin",
    [switch]$RefreshAll
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = Resolve-Path -LiteralPath (Join-Path $ScriptDir "..\..")
$Python = Join-Path $RootDir ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $Python)) {
    $Python = "python"
}

$LogDir = Join-Path $RootDir "logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$WatchLog = Join-Path $LogDir "pump_short_multiexchange_watch.log"

function Write-WatchLog {
    param([string]$Message)
    $stamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    Add-Content -LiteralPath $WatchLog -Encoding UTF8 -Value "[$stamp] $Message"
}

function Get-ActiveResearchProcesses {
    $currentPid = $PID
    return Get-CimInstance Win32_Process |
        Where-Object {
            $_.ProcessId -ne $currentPid -and (
                $_.CommandLine -like "*pump_short_run_multiexchange.py*" -or
                $_.CommandLine -like "*pump_short_collect_exchange.py*" -or
                $_.CommandLine -like "*pump_short_cross_exchange_research.py*"
            )
        } |
        Select-Object ProcessId, CommandLine
}

function Build-RunArgs {
    $args = @(
        "scripts\pump_short_run_multiexchange.py",
        "--start", $StartDate,
        "--sleep-sec", [string]$SleepSec,
        "--output-root", $OutputRoot,
        "--exchanges"
    )
    $exchangeItems = $Exchanges.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_.Length -gt 0 -and $_ -ne "bingx" }
    foreach ($exchange in $exchangeItems) {
        $args += $exchange
    }
    if ($RefreshAll) {
        # RefreshAll is intentionally expensive: it recollects samples instead of resume-only updates.
        # The Python orchestrator does not expose this yet, so keep the flag documented in the watch log.
        Write-WatchLog "RefreshAll requested, but current orchestrator is resume-first; existing symbols will not be fully recollected."
    }
    return $args
}

Write-WatchLog "watchdog started root=$RootDir start=$StartDate output_root=$OutputRoot interval_sec=$IntervalSec exchanges=$Exchanges"

while ($true) {
    $active = @(Get-ActiveResearchProcesses)
    if ($active.Count -gt 0) {
        $summary = ($active | ForEach-Object { "pid=$($_.ProcessId)" }) -join ", "
        Write-WatchLog "research process already active; sleeping interval. $summary"
        Start-Sleep -Seconds $IntervalSec
        continue
    }

    $runStamp = (Get-Date).ToString("yyyyMMdd-HHmmss")
    $stdout = Join-Path $LogDir "pump_short_multiexchange_cycle_$runStamp.out.log"
    $stderr = Join-Path $LogDir "pump_short_multiexchange_cycle_$runStamp.err.log"
    $argList = Build-RunArgs
    Write-WatchLog "starting update cycle: $Python $($argList -join ' ')"
    $proc = Start-Process -FilePath $Python `
        -ArgumentList $argList `
        -WorkingDirectory $RootDir `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -PassThru

    while (-not $proc.HasExited) {
        Start-Sleep -Seconds $IntervalSec
        try {
            $proc.Refresh()
        } catch {
            Write-WatchLog "process refresh failed: $($_.Exception.Message)"
            break
        }
        if (-not $proc.HasExited) {
            Write-WatchLog "update cycle still running pid=$($proc.Id) stdout=$stdout stderr=$stderr"
        }
    }

    try {
        $proc.WaitForExit()
        $proc.Refresh()
    } catch {
        Write-WatchLog "process wait/refresh failed after exit: $($_.Exception.Message)"
    }
    $exitCode = $proc.ExitCode
    if ($null -eq $exitCode) {
        $exitCode = -999
    }
    Write-WatchLog "update cycle exited code=$exitCode stdout=$stdout stderr=$stderr"
    Write-WatchLog "sleeping $IntervalSec sec before next cycle"
    Start-Sleep -Seconds $IntervalSec
}
