# Cloudflare Tunnel + Access (Windows) Setup

Goal
- Expose the local FastAPI UI securely on a domain without opening ports.
- Keep the app running on this PC.

Prereqs
- Domain added to Cloudflare (NS switched at registrar).
- Local UI running at `http://127.0.0.1:8000`.

Step 1: Install cloudflared
1) Download:
   - https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/
2) Place `cloudflared.exe`, e.g.:
   - `C:\Tools\cloudflared\cloudflared.exe`

Step 2: Login (creates local cert)
```powershell
C:\Tools\cloudflared\cloudflared.exe tunnel login
```
Browser opens → log in → pick domain.

Step 3: Create tunnel
```powershell
C:\Tools\cloudflared\cloudflared.exe tunnel create feearb
```
Note:
- Tunnel UUID
- JSON credentials path

Step 4: Route DNS to tunnel
```powershell
C:\Tools\cloudflared\cloudflared.exe tunnel route dns feearb trade.yourdomain.com
```
Replace `trade.yourdomain.com`.

Step 5: Create config
Create:
`%USERPROFILE%\.cloudflared\config.yml`

Example:
```yaml
tunnel: <UUID>
credentials-file: C:\Users\<USER>\.cloudflared\<UUID>.json

ingress:
  - hostname: trade.yourdomain.com
    service: http://127.0.0.1:8000
  - service: http_status:404
```

Step 6: Run tunnel
```powershell
C:\Tools\cloudflared\cloudflared.exe tunnel run feearb
```

Step 7: Protect with Cloudflare Access
1) Cloudflare → Zero Trust
2) Access → Applications → Add Application
3) Type: Self-hosted
4) Domain: `trade.yourdomain.com`
5) Policy: Allow only your email
6) Login method: Email OTP or Google OAuth

Step 8: Verify
Open from phone:
`https://trade.yourdomain.com`
Should see Access login.

Step 9: Install as Windows service
```powershell
C:\Tools\cloudflared\cloudflared.exe service install
```

Quick test (no domain)
```powershell
C:\Tools\cloudflared\cloudflared.exe tunnel --url http://127.0.0.1:8000
```
Cloudflare returns a temporary URL.
