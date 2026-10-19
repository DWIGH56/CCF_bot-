"""
CCF (Coin Cash Flow) - Batch Runner untuk GitHub Actions Cron Job
=================================================================

Entry point ketika bot dijalankan sebagai **scheduled job** (bukan server
24/7). Desain: sekali jalan -> screening sekali -> kirim alert -> exit.

Alur satu run:
    1. screen_coins()          (screener.py — async, aiohttp, paralel)
    2. get_all_active_users()  (subscribers_db.py — JSON di root repo)
    3. broadcast_alerts()      (kirim HTML per coin ke tiap user aktif,
                                via httpx HTTP/2 PARALEL ke Bot API Telegram)
    4. deactivate_expired_users() + save subscribers.json
       (workflow YAML yang meng-commit file ini balik ke repo)
    5. sys.exit(0)             — tidak ada loop, tidak ada Gradio.

Kenapa httpx dan bukan python-telegram-bot?
-------------------------------------------
PTB v20+ dirancang untuk *long-running polling bot*. Untuk one-shot send,
Bot API cukup dipanggil langsung via POST https://api.telegram.org/bot<token>/sendMessage.
httpx dipakai dengan **HTTP/2 + connection pool** sehingga N pesan terkirim
paralel (dibatasi semaphore) — kunci pencapaian target <2 menit per run.
Rate-limit tetap dihormati: max 20 pesan/menit per chat tujuan (Telegram
membatasi ~30 msg/menit per group, 20/msg/detik global); kita pakai jitter +
retry dengan backoff pada 429 (`retry_after`).

Secrets via environment variables (GitHub Actions Secrets):
    TELEGRAM_BOT_TOKEN, TELEGRAM_ADMIN_ID (opsional utk run ini)
Config threshold tetap dari config.py (bisa dioverride via vars).
"""

from __future__ import annotations

import asyncio
import html
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone

# --- .env saat development lokal (di CI pakai Secrets, no-op aman) ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import httpx

import subscribers_db as sdb
from config import CONFIG
from screener import ALERT_RETAIL_FOMO, ALERT_WHALE_BUY, screen_coins

# ---------------------------------------------------------------------------
# Logging & konstanta run
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO if not CONFIG.SECRETS.DEBUG else logging.DEBUG,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("ccf.main")

# Target durasi total run (detik). Seluruh fase diberi budget agar job selalu
# selesai < 2 menit; kalau lewat, sisa pekerjaan di-skip rapi lalu exit.
RUN_TIMEOUT_SECONDS: int = int(os.environ.get("RUN_TIMEOUT_SECONDS", "105"))

# Paralelisme pengiriman pesan (Bot API aman pada concurrency kecil).
SEND_CONCURRENCY: int = int(os.environ.get("SEND_CONCURRENCY", "8"))

_EMOJI_SIGNAL = {ALERT_WHALE_BUY: "🐋", ALERT_RETAIL_FOMO: "🔥"}


# ---------------------------------------------------------------------------
# Format pesan alert (identik dgn versi app.py — siap Telegram HTML)
# ---------------------------------------------------------------------------
def format_alert_message(coin: dict) -> str:
    """Ubah satu hasil screener.py menjadi pesan Telegram HTML yang rapi."""
    t = CONFIG.THRESHOLDS
    head_lines = []
    for s in coin.get("signals", []):
        emoji = _EMOJI_SIGNAL.get(s, "📡")
        head_lines.append(f"{emoji} <b>{html.escape(str(s))}</b>")

    name = html.escape(coin.get("name") or coin["symbol"])
    sym = html.escape(coin["symbol"])
    chain = html.escape(coin["chain"].upper())
    ca = coin["token_address"]
    url = coin.get("url") or f"https://dexscreener.com/{coin['chain']}/{ca}"
    sec = coin.get("security", {})

    whale = coin.get("whale", {})
    fomo = coin.get("retail_fomo", {})
    extra = []
    if whale:
        extra.append(
            f"🐋 Whale: buy terbesar ${whale.get('tx_usd', 0):,.0f} "
            f"({whale.get('lp_pct', 0):.1f}% LP, window "
            f"{whale.get('window', '?')})")
    if fomo:
        extra.append(
            f"🔥 FOMO: ~{fomo.get('unique_buy_wallets~', 0)} wallet buy, "
            f"buy/sell x{fomo.get('buy_sell_ratio', 0):.1f}, "
            f"harga {fomo.get('price_change_m5_pct', 0):+.1f}% (5m)")

    lines = [
        f"🚨 <b>CCF SIGNAL — {name} ({sym})</b>",
        *head_lines,
        "",
        f"🔗 Chain : <b>{chain}</b>",
        f"🆔 CA  : <code>{ca}</code>",
        f"💵 Harga: ${coin['price_usd']:.8f} "
        f"(5m {coin.get('price_change_m5', 0):+.1f}% | "
        f"1h {coin.get('price_change_h1', 0):+.1f}%)",
        f"💧 LP  : ${coin['lp_usd']:,.0f}  |  Vol 24h: "
        f"${coin.get('volume_24h_usd', 0):,.0f}",
        f"🛡️ Rug-check: OK (tax buy {sec.get('buy_tax_pct') or 0:.0f}% / "
        f"sell {sec.get('sell_tax_pct') or 0:.0f}%, "
        f"holders {sec.get('holder_count') or '?'})",
        *extra,
        f"⭐ Skor: {coin.get('score', 0)}/100  |  "
        f"{coin.get('screened_at', '')} UTC",
        "",
        f'👉 <a href="{url}">Buka di DexScreener</a>',
        f"<i>Bukan saran finansial. DYOR. Batas: LP≥${t.LP_MIN_USD:,.0f}, "
        f"whale>${t.WHALE_TX_USD:,.0f}</i>",
    ]
    return "\n".join(lines)[:4096]


# ---------------------------------------------------------------------------
# Pengiriman Telegram via httpx (async, HTTP/2, retry/backoff)
# ---------------------------------------------------------------------------
class TelegramSender:
    def __init__(self, token: str, client: httpx.AsyncClient) -> None:
        self.base = f"https://api.telegram.org/bot{token}"
        self.client = client

    async def send_message(self, chat_id: int, text: str,
                           retries: int = 2) -> bool:
        """POST sendMessage. Handle 429 (retry_after) & 5xx dengan backoff.
        Return True bila terkirim."""
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        for attempt in range(retries + 1):
            try:
                resp = await self.client.post(
                    f"{self.base}/sendMessage", json=payload, timeout=15.0)
                if resp.status_code == 200 and resp.json().get("ok"):
                    return True
                body = resp.json() if resp.content else {}
                # 429 too many requests -> hormati retry_after
                if resp.status_code == 429:
                    wait = float(body.get("parameters", {})
                                 .get("retry_after", 3)) + random.uniform(0, 1)
                    logger.warning("Rate-limit %s: tunggu %.1fs", chat_id, wait)
                    await asyncio.sleep(min(wait, 20.0))
                    continue
                # user diblokir / chat tidak ada -> tidak ada gunanya retry
                desc = str(body.get("description", ""))
                if any(k in desc for k in
                       ("bot was blocked", "chat not found",
                        "user is deactivated")):
                    logger.info("Skip %s permanen: %s", chat_id, desc)
                    return False
                logger.warning("Kirim ke %s gagal (%s): %s",
                               chat_id, resp.status_code, desc)
            except (httpx.HTTPError, ValueError) as exc:
                logger.warning("Kirim ke %s error: %s", chat_id, exc)
            await asyncio.sleep(1.5 * (attempt + 1))  # backoff linear sederhana
        return False


async def broadcast_alerts(client: httpx.AsyncClient, token: str,
                           coins: list[dict], active_users: list[dict]) -> int:
    """Kirim semua (user × coin) secara paralel dengan semaphore.

    Urutan prioritas: user menerima sinyal skor-tertinggi dulu. Kegagalan
    per-pesan hanya di-log — broadcast tidak pernah crash.
    """
    if not coins or not active_users:
        return 0

    sender = TelegramSender(token, client)
    sem = asyncio.Semaphore(SEND_CONCURRENCY)
    texts = [format_alert_message(c) for c in coins]  # format sekali utk semua

    async def _one(uid: int, idx: int) -> bool:
        async with sem:
            # jitter ringan antar pesan pertama per-user agar tidak menabrak
            # limit global 20 msg/menit per chat yg sama
            await asyncio.sleep(idx * 0.15 + random.uniform(0, 0.3))
            return await sender.send_message(uid, texts[idx])

    tasks = [_one(int(u["telegram_id"]), i)
             for u in active_users for i in range(len(texts))]
    results = await asyncio.gather(*tasks)
    sent = sum(1 for r in results if r)
    logger.info("Broadcast selesai: %d/%d pesan terkirim ke %d user aktif",
                sent, len(tasks), len(active_users))
    return sent


# ---------------------------------------------------------------------------
# Satu siklus batch (dipanggil dengan asyncio.wait_for -> hard deadline)
# ---------------------------------------------------------------------------
async def run_once() -> int:
    started = time.monotonic()
    token = CONFIG.SECRETS.TELEGRAM_BOT_TOKEN

    # 1) Housekeeping subscriber (expiry) — sebelum baca daftar aktif.
    expired = await asyncio.to_thread(sdb.deactivate_expired_users)
    if expired:
        logger.info("Housekeeping: %d user kedaluwarsa -> inactive", expired)

    # 2) Screening sekali jalan (batch).
    logger.info("Mulai screening | chains=%s", CONFIG.BOT.CHAINS_WATCHED)
    coins = await screen_coins()
    elapsed = time.monotonic() - started
    logger.info("Screening selesai dlm %.1fs — %d sinyal lolos",
                elapsed, len(coins))

    # 3) Ambil user aktif (JSON store) lalu kirim alert.
    active_users = await asyncio.to_thread(sdb.get_all_active_users)
    sent = 0
    if not coins:
        logger.info("Tidak ada sinyal — tidak mengirim apa pun.")
    elif not token:
        logger.error("TELEGRAM_BOT_TOKEN kosong — alert TIDAK dikirim. "
                     "Set Secrets/Variables di workflow.")
    elif not active_users:
        logger.info("%d sinyal tapi tidak ada subscriber aktif — skip.",
                    len(coins))
    else:
        async with httpx.AsyncClient(http2=True,
                                     limits=httpx.Limits(
                                         max_connections=SEND_CONCURRENCY)) as client:
            sent = await broadcast_alerts(client, token, coins, active_users)

    # 4) Ringkasan akhir (keluaran stdout — muncul di log Actions).
    total = time.monotonic() - started
    logger.info("Run selesai dalam %.1fs | sinyal=%d | user_aktif=%d "
                "| pesan=%d | subscribers_file=%s",
                total, len(coins), len(active_users), sent,
                sdb.SUBSCRIBERS_FILE)
    return 0 if total < RUN_TIMEOUT_SECONDS else 0


def main() -> None:
    logger.info("=== CCF batch run (GitHub Actions mode) ===")
    logger.info("DB mode: JSON (subscribers.json) — bukan SQLite/Gradio")
    try:
        # Hard deadline: apapun yang terjadi, proses selesai < RUN_TIMEOUT.
        code = asyncio.run(asyncio.wait_for(run_once(),
                                            timeout=RUN_TIMEOUT_SECONDS))
    except asyncio.TimeoutError:
        logger.error("Run melebihi batas %ds — dihentikan rapi (exit 0).",
                     RUN_TIMEOUT_SECONDS)
        code = 0
    except Exception:
        logger.exception("Kegagalan tak terduga — exit 1 agar Actions terlihat.")
        code = 1
    sys.exit(code)


if __name__ == "__main__":
    main()
