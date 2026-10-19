# 💰 CCF — Coin Cash Flow

> **Mode produksi saat ini: GitHub Actions Scheduled Cron (batch).**
> Entry point: `python main.py` — screening 1× → kirim alert Telegram → exit.
> Subscriber disimpan di **`subscribers.json`** yang di-commit balik ke repo
> oleh workflow (`subscribers_db.py`). Mode lama Hugging Face Spaces 24/7
> (`app.py` + SQLite) dipertahankan sebagai opsi, tidak dipakai cron.

## 🗓️ Setup GitHub Actions (utama)

1. **Push repo** berisi: `main.py`, `screener.py`, `config.py`,
   `subscribers_db.py`, `subscribers.json`, `requirements.txt`,
   `.github/workflows/ccf-schedule.yml`.
2. **Secrets & Variables** → *Settings ▸ Secrets and variables ▸ Actions*:
   - **Secret** `TELEGRAM_BOT_TOKEN` (Repository secrets)
   - **Variable** `TELEGRAM_ADMIN_ID` (Repository variables, opsional)
3. **Izinkan push** → *Settings ▸ Actions ▸ General ▸ Workflow permissions*
   pilih **"Read and write permissions"** (wajib agar `subscribers.json`
   bisa di-commit balik; tanpa ini data tetap jalan tapi hilang tiap run).
4. Cron berjalan tiap 30 menit (`*/30 * * * *`); tes manual dengan tombol
   **Run workflow** (workflow_dispatch). Deadline internal run = 105 dtk
   (`RUN_TIMEOUT_SECONDS`), target < 2 menit tercapai karena seluruh I/O
   async + pengiriman pesan paralel (httpx HTTP/2, semaphore 8).

### Cara menambah subscriber aktif (tanpa server bot)
Edit `subscribers.json` langsung / via PR:
```json
{ "users": { "123456789": { "telegram_id": 123456789, "username": "alice",
  "status": "active", "expiry_date": "2026-11-17 23:00:00" } } }
```
User harus pernah menekan /start pada bot polling (agar chat terbuka), atau
kirim broadcast manual. `expiry_date` format string UTC `YYYY-MM-DD HH:MM:SS`.

---


Bot Telegram untuk screening coin crypto di DEX: mendeteksi **Whale Buy** 🐋 dan
**Retail FOMO** 🔥, disaring dengan filter **anti-rug-pull** (GoPlus Security).

## Arsitektur

| File | Tanggung jawab |
|---|---|
| `app.py` | Entry point HF Spaces: Gradio heartbeat + Telegram bot + background screener loop |
| `config.py` | Secrets (`TELEGRAM_BOT_TOKEN`, `TELEGRAM_ADMIN_ID`) & semua threshold (override via env) |
| `database.py` | SQLite `data/ccf_users.db` — tabel `subscribers`, CRUD & cek langganan aktif. Mode tangguh bila disk read-only (fallback in-memory + peringatan log) |
| `screener.py` | Otak screening (100% async aiohttp): DexScreener/GeckoTerminal fetch → GoPlus anti-rug → deteksi Whale/FOMO |
| `requirements.txt` | Dependensi (gradio, python-telegram-bot v21, aiohttp, httpx, …) |

## Deploy ke Hugging Face Spaces

### 1. Susun repository (struktur file minimal)

```
ccf/                    <- repo Space-mu
├── app.py              <- WAJIB di root; HF menjalankan `python app.py`
├── config.py
├── database.py
├── screener.py
├── requirements.txt    <- dibaca otomatis oleh HF Spaces
└── README.md           <- opsional (jadi deskripsi Space)
```

Semua file `.py` harus berada di **root repo** (bukan subfolder), karena
`app.py` meng-import `config/database/screener` sebagai modul sibling.
Jangan upload folder `data/`, `__pycache__/`, atau `.env` (lihat `.gitignore`).

Cara upload:
```bash
git clone https://huggingface.co/spaces/<username>/ccf
cp app.py config.py database.py screener.py requirements.txt README.md <repo>/
cd <repo> && git add -A && git commit -m "CCF v1" && git push
```
(atau drag-and-drop lewat tombol **"Files → Add files"** di web UI HF.)

Space wajib dibuat dengan: **SDK = Gradio**, **Hardware = CPU Basic**,
dan file `app.py` memanggil `demo.launch()` tanpa parameter `server_name`
yang eksplisit (HF sudah mengatur proxy).

### 2. Isi Repository Secrets

Buka tab **Settings → Variables and secrets → New secret**, lalu tambahkan:

| Secret name | Value | Keterangan |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | `123456:ABC-DEF...` | Token dari @BotFather |
| `TELEGRAM_ADMIN_ID` | `987654321` | Chat ID kamu (cek via @userinfobot) |

Penting:
- Nama secret HARUS sama persis dengan yang dibaca `os.environ.get()` di
  `config.py` (huruf besar semua).
- Tandai **Sensitive** agar nilainya ter-mask di log.
- Setelah menambah secret, klik **Restart this Space** (secret hanya dimuat
  saat container start).
- Opsional override threshold juga lewat secret/env, mis. `LP_MIN_USD=8000`.

> Tanpa token, Space tetap hidup (Gradio saja) dan log akan menagih Secret —
> screener berjalan bahkan saat token invalid, jadi mudah mengetes sebelum beli bot token asli.

### 3. Persistensi data (penting!)

Filesystem HF Spaces bersifat **ephemeral**: folder `data/` ter-reset setiap
restart/redeploy, dan pada beberapa konfigurasi bisa read-only. Modul
`database.py` sudah menangani ini secara anggun:
- Jika path tidak bisa ditulis → mode in-memory + warning di log (bot tidak crash).
- Untuk produksi, aktifkan **Settings → Storage → Persistent Storage**
  (minimal 1 GB) lalu set secret/env `DATA_DIR=/persistent/data` sehingga
  `ccf_users.db` awet antar-restart.

## Perintah Bot

| Command | Deskripsi |
|---|---|
| `/start` | Daftar ke database (status `inactive`) |
| `/status` | Cek status & expiry langganan |
| `/help` | Bantuan |
| `/admin_approve <telegram_id> [hari]` | Aktifkan user (khusus ADMIN_ID, default 30 hari) |

## Threshold (dapat dioverride via Secrets/env)

- LP minimum: `LP_MIN_USD=5000`
- Whale: buy > `WHALE_TX_USD=300` **dan** > `WHALE_TX_LP_PCT_MIN=1%` dari LP
- Retail FOMO (window 3 menit): > `FOMO_UNIQUE_BUY_WALLETS_MIN=15` wallet buy,
  vol buy > `FOMO_BUY_VOLUME_RATIO_MIN=3x` vol sell, harga naik > `FOMO_PRICE_CHANGE_MIN_PCT=5%`
- Anti-rug: buang honeypot, tax > `RUG_MAX_TAX_PERCENT=10%`, LP < minimum
- Chains: `CHAINS_WATCHED=ethereum,bsc,solana,base` | interval: `POLL_INTERVAL_SECONDS=60`

## Run lokal (development)

```bash
pip install -r requirements.txt
cp .env.example .env   # isi token, atau export manual
python app.py          # Gradio di http://localhost:7860 + bot polling
```

⚠️ *Disclaimer: bukan saran finansial. DYOR.*
