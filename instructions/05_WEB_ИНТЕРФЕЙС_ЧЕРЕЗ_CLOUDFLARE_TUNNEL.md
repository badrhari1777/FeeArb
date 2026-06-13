# FeeArb Android и веб-интерфейс через Cloudflare Tunnel

## Временный публичный резерв для Android

Если `https://app.feearb.ru/` возвращает `530` или `502`, в Android временно
укажите:

```text
https://desktop-0tl9bsa.tail3830e2.ts.net:10000/
```

В поле `Remote access token` остается тот же токен Cloudflare. Tailscale VPN
на телефоне для этого адреса не требуется. Порт `10000` является публичным
Funnel, а Caddy отклоняет запросы без правильного токена с HTTP `401`.

Приватный резерв через установленный на телефоне Tailscale остается прежним:

```text
https://desktop-0tl9bsa.tail3830e2.ts.net:8443/
```

Для приватного `:8443` поле токена не обязательно.

Целевая схема:

```text
FeeArb Android -> постоянный HTTPS-домен Cloudflare
               -> Cloudflare Tunnel
               -> FeeArb на 127.0.0.1:8000
```

Tailscale остается резервным способом доступа из файла
`01_ANDROID_ТЕЛЕФОН_ЧЕРЕЗ_TAILSCALE.md`.

## Текущее состояние

- `cloudflared 2026.6.0` установлен на компьютере.
- Android-приложение поддерживает поле `Remote access token`.
- Backend требует `X-FeeArb-Token` для запросов, пришедших через Cloudflare.
- Локальные и Tailscale-запросы продолжают работать без токена.
- Токен хранится локально в `state/remote_access_token.txt`.
- Именованный tunnel `feearb` создан.
- Постоянный адрес настроен:

```text
https://app.feearb.ru/
```

- Конфигурация: `config/cloudflared/feearb.yml`.
- Windows-служба `Cloudflared` имеет тип запуска `Automatic`.
- Общий стартовый скрипт FeeArb проверяет, что служба запущена.
- Готовая APK:

```text
android-app/app/build/outputs/apk/debug/FeeArb-cloudflare-ready-2026-06-10.apk
```

Сразу после регистрации `.ru` публичные DNS могут временно возвращать
`NXDOMAIN`. Нужно дождаться публикации делегирования родительской зоной `.ru`.

## Требования для постоянного адреса

- Домен подключен к Cloudflare.
- FeeArb работает на `http://127.0.0.1:8000`.
- `cloudflared.exe` установлен в `C:\Program Files (x86)\cloudflared\`.

## Настройка

Авторизация:

```powershell
& "C:\Program Files (x86)\cloudflared\cloudflared.exe" tunnel login
```

Создание туннеля:

```powershell
& "C:\Program Files (x86)\cloudflared\cloudflared.exe" tunnel create feearb
```

Создание DNS-маршрута:

```powershell
& "C:\Program Files (x86)\cloudflared\cloudflared.exe" tunnel route dns feearb app.feearb.ru
```

Файл проекта `config/cloudflared/feearb.yml`:

```yaml
tunnel: b848b4d9-bb7c-47ff-858b-ef025d2d469f
credentials-file: C:\Users\Pavel\.cloudflared\b848b4d9-bb7c-47ff-858b-ef025d2d469f.json
protocol: http2

ingress:
  - hostname: app.feearb.ru
    service: http://127.0.0.1:8000
  - service: http_status:404
```

Запуск:

```powershell
& "C:\Program Files (x86)\cloudflared\cloudflared.exe" tunnel --protocol http2 run feearb
```

Установка или восстановление Windows-службы из PowerShell администратора:

```powershell
& "C:\Projects\FeeArb\scripts\windows\install_cloudflare_tunnel_service.ps1"
```

Скрипт копирует config и credentials в профиль `LocalSystem`, устанавливает
правильный `ImagePath` службы и включает автоматический запуск.

## Настройка Android

После создания постоянного hostname:

1. Установите новую APK.
2. Откройте `Settings`.
3. В `Backend base URL` укажите `https://app.feearb.ru/`.
4. В `Remote access token` укажите значение из
   `state/remote_access_token.txt`.
5. Нажмите `Apply`.
6. Проверьте `Balances -> Refresh`.

Не публикуйте и не отправляйте remote token посторонним.

## Проверка защиты

```powershell
$token = (Get-Content C:\Projects\FeeArb\state\remote_access_token.txt -Raw).Trim()
Invoke-WebRequest https://app.feearb.ru/api/mobile/positions
Invoke-WebRequest https://app.feearb.ru/api/mobile/positions `
  -Headers @{"X-FeeArb-Token" = $token}
```

Первый запрос без токена должен вернуть `401`. Второй должен вернуть `200`.

## Важно

### Ошибка 502 или 530

Если `app.feearb.ru` возвращает `502/530`, сначала проверьте:

```powershell
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8000/api/settings
Get-Service cloudflared
```

Если локальный backend отвечает `200`, а `cloudflared` имеет статус `Running`,
ошибка находится в соединении Cloudflare edge/tunnel, а не в приложении.
Для работы с телефона временно используйте приватный Tailscale Serve:

```text
https://desktop-0tl9bsa.tail3830e2.ts.net:8443/
```

На 2026-06-11 диагностировались обрывы edge control stream cloudflared при
исправном origin. Переключение HTTP/2/QUIC и версия cloudflared не устранили
сбой, поэтому конфигурация была возвращена к штатной: HTTP/2 и
`127.0.0.1:8000`.

- Quick Tunnel на `trycloudflare.com` не используется для live-работы:
  адрес меняется и нет гарантии доступности.
- Для постоянной эксплуатации нужен именованный tunnel и собственный hostname.
- Не отключайте Tailscale до полной проверки Cloudflare с телефона.
- FeeArb содержит endpoints реальной торговли, поэтому туннель нельзя
  публиковать без remote token.
