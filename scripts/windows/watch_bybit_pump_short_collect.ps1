param(
    [int]$LookbackDays = 90,
    [double]$SleepSec = 2.0,
    [string]$OutputDir = "data\research\bybit_pump_short",
    [int]$CheckIntervalSec = 3600,
    [int]$RestartDelaySec = 3600,
    [string]$Symbols = "",
    [int]$MaxSymbols = 0
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
$WatchLog = Join-Path $LogDir "bybit_pump_short_watch.log"

function Write-WatchLog {
    param([string]$Message)
    $stamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    Add-Content -LiteralPath $WatchLog -Encoding UTF8 -Value "[$stamp] $Message"
}

function Build-Args {
    $args = @(
        "scripts\bybit_pump_short_collect.py",
        "--lookback-days", [string]$LookbackDays,
        "--sleep-sec", [string]$SleepSec,
        "--output-dir", $OutputDir
    )
    if ($Symbols.Trim().Length -gt 0) {
        $args += @("--symbols", $Symbols.Trim())
    }
    if ($MaxSymbols -gt 0) {
        $args += @("--max-symbols", [string]$MaxSymbols)
    }
    return $args
}

Write-WatchLog "watchdog started root=$RootDir lookback_days=$LookbackDays sleep_sec=$SleepSec output_dir=$OutputDir symbols=$Symbols max_symbols=$MaxSymbols"

while ($true) {
    $runStamp = (Get-Date).ToString("yyyyMMdd-HHmmss")
    $stdout = Join-Path $LogDir "bybit_pump_short_collect_$runStamp.out.log"
    $stderr = Join-Path $LogDir "bybit_pump_short_collect_$runStamp.err.log"
    $argList = Build-Args
    Write-WatchLog "starting collector: $Python $($argList -join ' ')"
    $proc = Start-Process -FilePath $Python `
        -ArgumentList $argList `
        -WorkingDirectory $RootDir `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -PassThru

    while (-not $proc.HasExited) {
        Start-Sleep -Seconds $CheckIntervalSec
        try {
            $proc.Refresh()
        } catch {
            Write-WatchLog "process refresh failed: $($_.Exception.Message)"
            break
        }
        if (-not $proc.HasExited) {
            Write-WatchLog "collector still running pid=$($proc.Id) stdout=$stdout stderr=$stderr"
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
        $stdoutTail = ""
        if (Test-Path -LiteralPath $stdout) {
            $stdoutTail = (Get-Content -LiteralPath $stdout -Tail 5) -join "`n"
        }
        if ($stdoutTail -match "collection complete" -and $stdoutTail -notmatch "failed=[1-9]") {
            $exitCode = 0
        } else {
            $exitCode = -999
        }
    }
    Write-WatchLog "collector exited code=$exitCode stdout=$stdout stderr=$stderr"
    if ($exitCode -eq 0) {
        Write-WatchLog "collector completed successfully; watchdog exiting"
        break
    }

    Write-WatchLog "collector failed; sleeping $RestartDelaySec sec before resume retry"
    Start-Sleep -Seconds $RestartDelaySec
}
