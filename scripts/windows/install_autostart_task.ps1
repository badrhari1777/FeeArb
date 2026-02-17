$ErrorActionPreference = "Stop"

$taskName = "FeeArb Public UI (Tailscale Funnel)"
$scriptPath = "C:\Projects\FeeArb\scripts\windows\start_feearb_public.ps1"

if (!(Test-Path $scriptPath)) {
    throw "Missing script: $scriptPath"
}

$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`""
$trigger = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -RunLevel Highest

$task = New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings -Principal $principal

try {
    Register-ScheduledTask -TaskName $taskName -InputObject $task -Force | Out-Null
} catch {
    throw "Failed to register task '$taskName': $($_.Exception.Message)"
}

Write-Output "Installed scheduled task: $taskName"

