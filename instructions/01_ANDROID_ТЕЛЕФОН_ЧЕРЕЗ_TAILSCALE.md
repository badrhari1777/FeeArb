# Android-приложение FeeArb через интернет и Tailscale

## Что указать в приложении

В FeeArb на телефоне откройте:

```text
Settings -> Backend base URL
```

Укажите полный адрес, обязательно с `https://`, портом `8443` и завершающим `/`:

```text
https://desktop-0tl9bsa.tail3830e2.ts.net:8443/
```

Нажмите `Apply`, затем откройте `Balances` или `Positions` и нажмите `Refresh`.

Если HTTPS-адрес не открывается на конкретном Android-телефоне, используйте
резервный приватный адрес:

```text
http://desktop-0tl9bsa:8080/
```

Он также доступен только внутри tailnet и проксируется на тот же FeeArb
backend. Android-приложение FeeArb разрешает этот HTTP-вариант.

## Настройка телефона

1. Установите официальное приложение `Tailscale` из Google Play.
2. Войдите в тот же аккаунт Tailscale, который используется на компьютере.
3. Разрешите Android создать VPN-подключение.
4. Убедитесь, что в Tailscale отображается статус `Connected`.
5. Оставляйте Tailscale подключенным во время работы FeeArb.
6. Откройте FeeArb и задайте backend URL, указанный выше.

Телефон может находиться в любой сети: мобильный интернет, чужой Wi-Fi или домашний Wi-Fi. Общая локальная сеть с компьютером не требуется.

## Что должно работать на компьютере

Для доступа с телефона одновременно должны быть запущены:

- компьютер;
- Windows-служба Tailscale;
- backend FeeArb на `127.0.0.1:8000`;
- приватный Tailscale Serve на HTTPS-порту `8443`.

Проверка backend на компьютере:

```powershell
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8000/api/mobile/positions
```

Проверка Tailscale:

```powershell
& "C:\Program Files\Tailscale\tailscale.exe" status
& "C:\Program Files\Tailscale\tailscale.exe" serve status
```

В выводе должна присутствовать приватная схема:

```text
https://desktop-0tl9bsa.tail3830e2.ts.net:8443 (tailnet only)
|-- / proxy http://127.0.0.1:8000
```

Также может быть включен резервный HTTP-маршрут:

```text
http://desktop-0tl9bsa:8080 (tailnet only)
|-- / proxy http://127.0.0.1:8000
```

Если Serve пропал, восстановите его:

```powershell
& "C:\Program Files\Tailscale\tailscale.exe" serve --bg --https=8443 http://127.0.0.1:8000
& "C:\Program Files\Tailscale\tailscale.exe" serve --bg --http=8080 http://127.0.0.1:8000
```

## Быстрая проверка с телефона

При подключенном Tailscale откройте в браузере телефона:

```text
https://desktop-0tl9bsa.tail3830e2.ts.net:8443/api/mobile/positions
```

Если отображается JSON, сеть настроена правильно. После этого тот же базовый адрес будет работать в FeeArb.

Если HTTPS не работает, проверьте резервный URL:

```text
http://desktop-0tl9bsa:8080/api/mobile/positions
```

## Если приложение не подключается

Проверяйте по порядку:

1. На телефоне есть интернет.
2. Tailscale на телефоне показывает `Connected`.
3. Телефон и компьютер вошли в один Tailscale-аккаунт/tailnet.
4. Компьютер включен и не находится в спящем режиме.
5. Backend отвечает локально на порту `8000`.
6. `tailscale serve status` показывает маршрут `:8443 -> 127.0.0.1:8000`.
7. В FeeArb адрес введен без пробелов и с `/` в конце.
8. В настройках Tailscale на Android разрешено использование Tailscale DNS.
9. На телефоне не включен другой VPN, Samsung Secure Wi-Fi или блокирующий VPN-фильтр.

После изменения URL нажмите `Apply`. Если сохранено старое значение, очистите данные приложения или переустановите APK и задайте URL заново.

## Безопасность

- Не открывайте порт `8000` через роутер.
- Не используйте Tailscale Funnel для backend нативного приложения.
- Не указывайте публичный адрес `https://desktop-0tl9bsa.tail3830e2.ts.net/` без `:8443`.
- Адрес `:8443` доступен только устройствам внутри вашего tailnet.
- FeeArb содержит live-операции. Первый тест проводите через `Refresh`, просмотр данных и `Dry Run`, без подтверждения `Execute`.

## Проверено

На 10 июня 2026 года:

- Tailscale Serve `:8443` активен;
- маршрут ведет на `127.0.0.1:8000`;
- `GET /api/mobile/positions` через приватный HTTPS-адрес возвращает HTTP `200`.
