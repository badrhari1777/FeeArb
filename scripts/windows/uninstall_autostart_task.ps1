$ErrorActionPreference = "Stop"

$taskName = "FeeArb Public UI (Tailscale Funnel)"

try {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction Stop | Out-Null
    Write-Output "Removed scheduled task: $taskName"
} catch {
    Write-Output "Task not removed (maybe not installed): $taskName"
}

