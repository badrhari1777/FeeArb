# Tailscale Funnel Autostart (Windows)

Goal
- Expose FeeArb UI on the public internet via Tailscale Funnel.
- Protect it with BasicAuth (Caddy).
- Make it survive PC reboots without manual steps.

Assumptions
- FeeArb FastAPI runs on `http://127.0.0.1:8000`.
- Caddy runs on `http://127.0.0.1:18080` and reverse-proxies to `127.0.0.1:8000`.
- Tailscale Funnel proxies `https://<machine>.<tailnet>.ts.net/` -> `http://127.0.0.1:18080/`.

## 1) Verify the current setup

On the Windows PC:

```powershell
& "C:\Program Files\Tailscale\tailscale.exe" status
& "C:\Program Files\Tailscale\tailscale.exe" funnel status
```

Confirm:
- `Funnel on` is shown, and it proxies to `http://127.0.0.1:18080`.

Confirm local ports:

```powershell
Get-NetTCPConnection -LocalAddress 127.0.0.1 -LocalPort 8000,18080 -State Listen
```

## 2) Recommended Caddy binding (LAN safety)

Prefer binding Caddy to loopback only, to avoid exposure on your local network:
- Use `127.0.0.1:18080 { ... }` instead of `:18080 { ... }` in `C:\Tools\caddy\Caddyfile`.

## 3) Install the autostart scheduled task

This repo ships scripts:
- `scripts/windows/start_feearb_public.ps1`
- `scripts/windows/install_autostart_task.ps1`
- `scripts/windows/uninstall_autostart_task.ps1`

Install (run PowerShell as Administrator):

```powershell
Set-ExecutionPolicy -Scope Process Bypass -Force
& "C:\Projects\FeeArb\scripts\windows\install_autostart_task.ps1"
```

Validate:

```powershell
Get-ScheduledTask -TaskName "FeeArb Public UI (Tailscale Funnel)" | Select-Object TaskName,State
Get-ScheduledTaskInfo -TaskName "FeeArb Public UI (Tailscale Funnel)" | Select-Object LastRunTime,LastTaskResult
```

Task behavior installed by the script:
- Triggers: `At startup` and `At logon`.
- Recovery: restarts up to 10 times with 1-minute interval if the start script fails.

## 4) Test reboot behavior

1) Reboot the PC.
2) Wait ~1-2 minutes.
3) From any device, open the Funnel URL:
   - `https://<machine>.<tailnet>.ts.net`
4) Ensure BasicAuth prompt appears and the UI loads normally.

## 5) Do you need to "log in" every time?

- Tailscale: no manual login is needed after reboot as long as the Tailscale Windows service stays signed-in.
- BasicAuth: the browser may ask again if you use incognito, clear credentials, or change the password.

## 6) Uninstall

```powershell
& "C:\Projects\FeeArb\scripts\windows\uninstall_autostart_task.ps1"
```
