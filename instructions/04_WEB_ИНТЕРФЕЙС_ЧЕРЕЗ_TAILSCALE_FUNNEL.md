# Публичный веб-интерфейс FeeArb через Tailscale Funnel

Эта схема предназначена для веб-интерфейса в браузере и отличается от приватного backend Android-приложения.

## Текущая схема

```text
Публичный HTTPS Funnel
https://desktop-0tl9bsa.tail3830e2.ts.net/
        |
        v
Caddy + BasicAuth на 127.0.0.1:18080
        |
        v
FeeArb на 127.0.0.1:8000
```

Для Android-приложения используйте не этот адрес, а приватный URL с портом `8443`.

## Проверка

```powershell
& "C:\Program Files\Tailscale\tailscale.exe" status
& "C:\Program Files\Tailscale\tailscale.exe" funnel status
Get-NetTCPConnection -LocalAddress 127.0.0.1 -LocalPort 8000,18080 -State Listen
```

## Автозапуск

Скрипты проекта:

```text
scripts/windows/start_feearb_public.ps1
scripts/windows/install_autostart_task.ps1
scripts/windows/uninstall_autostart_task.ps1
```

Установка задачи автозапуска из PowerShell с правами администратора:

```powershell
Set-ExecutionPolicy -Scope Process Bypass -Force
& "C:\Projects\FeeArb\scripts\windows\install_autostart_task.ps1"
```

Проверка задачи:

```powershell
Get-ScheduledTask -TaskName "FeeArb Public UI (Tailscale Funnel)" | Select-Object TaskName,State
Get-ScheduledTaskInfo -TaskName "FeeArb Public UI (Tailscale Funnel)" | Select-Object LastRunTime,LastTaskResult
```

Удаление задачи:

```powershell
& "C:\Projects\FeeArb\scripts\windows\uninstall_autostart_task.ps1"
```

## Различие адресов

```text
https://desktop-0tl9bsa.tail3830e2.ts.net/
Публичный Funnel, браузер, BasicAuth.

https://desktop-0tl9bsa.tail3830e2.ts.net:8443/
Приватный Tailscale Serve, Android backend, только tailnet.
```
