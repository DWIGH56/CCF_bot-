"""
CCF (Coin Cash Flow) - Entry Point Hugging Face Spaces [MODE WARISAN/24-7]
===========================================================================

⚠️ CATATAN ARSITEKTUR (sejak migrasi ke GitHub Actions Cron):
    Produksi utama CCF sekarang berjalan BATCH via `main.py` yang dipicu
    scheduled workflow `.github/workflows/ccf-schedule.yml`. Subscriber
    disimpan di `subscribers.json` (lihat `subscribers_db.py`).
    File app.py ini DIPERTAHANKAN hanya bila suatu saat ingin kembali
    deploy sebagai Space 24/7 — tidak dipakai oleh cron job dan Gradio /
    python-telegram-bot sengaja dikomentari dari requirements.txt.

File ini dijalankan oleh HF Spaces (`python app.py`). Tanggung jawab:

1. Gradio UI  : heartbeat sederhana "🟢 CCF Bot is Running..." + timestamp
                 yang di-refresh tiap 10 detik. WAJIB ada agar Space tidak
                 dianggap idle/tidur oleh HF.
2. Telegram
   Bot       : python-telegram-bot v20+ (async), berjalan di event loop
                 terpisah (thread sendiri) sehingga tidak memblokir Gradio.
                 Command: /start, /status, /help, /admin_approve (ADMIN only).
3. Background : loop screener.py via asyncio.create_task — tiap siklus
                 memanggil screen_coins(), lalu broadcast hasil ke semua
                 subscriber berstatus 'active' (dengan cooldown anti-spam).

Kedua server (Gradio + Telegram polling) hidup berdampingan:
    gradio launch(prevent_thread_lock=True)  ->  main thread bebas
    bot + screener berjalan di event loop asyncio pada thread daemon khusus.

Secrets dibaca dari Hugging Face Spaces Secrets via config.py:
    TELEGRAM_BOT_TOKEN, TELEGRAM_ADMIN_ID
"""

from __future__ import annotations

import asyncio
import html
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Optional

# --- .env saat development lokal (di HF Spaces pakai Secrets, no-op aman) ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import gradio as gr
from telegram import Update
from telegram.constants import ParseMode
from telegram.error import InvalidToken
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

import database as db
from config import CONFIG
from screener import ALERT_RETAIL_FOMO, ALERT_WHALE_BUY, screen_coins

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO if not CONFIG.SECRETS.DEBUG else logging.DEBUG,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("ccf.app")

# ---------------------------------------------------------------------------
# State global
# ---------------------------------------------------------------------------
BOT_APP: Optional[Application] = None          # instance PTB (di-set di loop bot)
BOT_READY = threading.Event()                  # sinyal "bot sudah polling"
BOT_LOOP: Optional[asyncio.AbstractEventLoop] = None  # loop utk kirim chat aktif
_last_alert_ts: dict[int, float] = {}          # telegram_id -> unix ts alert terakhir
                                               # (dibatasi via _remember_alert)
MAX_TRACKED_USERS = 5000                       # batas memori dict cooldown

START_TIME = datetime.now(timezone.utc)


def _fmt_dt(dt: Optional[str]) -> str:
    """'2026-10-15 19:20:31' (UTC string dari DB) -> tampilan rapi."""
    if not dt:
        return "-"
    try:
        parsed = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc)
        return parsed.strftime("%d %b %Y %H:%M UTC")
    except ValueError:
        return dt


# ---------------------------------------------------------------------------
# 1) GRADIO UI — heartbeat wajib HF Spaces
# ---------------------------------------------------------------------------
def heartbeat() -> str:
    """Teks status bot + uptime; dipanggil ulang tiap 10 detik oleh gr.Timer."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    uptime = datetime.now(timezone.utc) - START_TIME
    bot_state = "✅ online (polling)" if BOT_READY.is_set() else "⏳ starting…"
    chains = ", ".join(CONFIG.BOT.CHAINS_WATCHED)
    interval = CONFIG.BOT.POLL_INTERVAL_SECONDS
    t = CONFIG.THRESHOLDS
    return (
        f"🟢 **CCF Bot is Running...**\n\n"
        f"Last update : `{now}`\n"
        f"Telegram bot: `{bot_state}`\n"
        f"Screener    : `siklus tiap {interval}s | chains: {chains}`\n"
        f"Uptime      : `{str(uptime).split('.')[0]}`\n\n"
        f"Threshold → LP ≥ ${t.LP_MIN_USD:,.0f} | "
        f"Whale buy > ${t.WHALE_TX_USD:,.0f} & >{t.WHALE_TX_LP_PCT_MIN:.0f}% LP | "
        f"FOMO > {t.FOMO_UNIQUE_BUY_WALLETS_MIN} buyer/"
        f"{t.FOMO_WINDOW_SECONDS // 60}mnt, vol x{t.FOMO_BUY_VOLUME_RATIO_MIN:.0f}, "
        f"harga +{t.FOMO_PRICE_CHANGE_MIN_PCT:.0f}%"
    )


with gr.Blocks(title="CCF - Coin Cash Flow") as demo:
    gr.Markdown("# 💰 CCF — Coin Cash Flow")
    gr.Markdown(
        "Bot Telegram screening coin DEX: deteksi **Whale** & **Retail FOMO**, "
        "disaring **anti-rug** (GoPlus). Space ini sengaja kept-alive oleh "
        "halaman status di bawah."
    )
    status_box = gr.Markdown(heartbeat())
    # gr.Timer (Gradio >=4.x): callback server-side tiap 10 detik ->
    # timestamp selalu segar selama ada pengunjung; proses Python sendiri
    # juga tidak pernah idle karena loop screener + polling bot aktif 24/7.
    timer = gr.Timer(10.0)
    timer.tick(fn=heartbeat, outputs=status_box)


# ---------------------------------------------------------------------------
# 2) FORMAT PESAN ALERT
# ---------------------------------------------------------------------------
_EMOJI_SIGNAL = {ALERT_WHALE_BUY: "🐋", ALERT_RETAIL_FOMO: "🔥"}


def format_alert_message(coin: dict) -> str:
    """Ubah satu hasil screener.py menjadi pesan Telegram HTML yang rapi.

    Kompatibel dengan output screener: `signals` berupa list string tipe
    alert, `whale`/`retail_fomo` berupa dict detail deteksi.
    """
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
    msg = "\n".join(lines)
    return msg[:4096]  # batas keras Telegram


# ---------------------------------------------------------------------------
# 3) BROADCAST KE USER AKTIF
# ---------------------------------------------------------------------------
def _remember_alert(uid: int, ts: float) -> None:
    """Simpan timestamp cooldown dengan batas memori (anti memory leak).

    Pada Space CPU Basic 2 GB, dict tak-terbatas yang tumbuh per user adalah
    sumber leak utama. Bila melewati MAX_TRACKED_USERS, entri paling lama
    dibuang (eviction sederhana — konsekuensinya hanya cooldown reset).
    """
    if len(_last_alert_ts) >= MAX_TRACKED_USERS and uid not in _last_alert_ts:
        for old_uid in sorted(_last_alert_ts, key=_last_alert_ts.get)[:MAX_TRACKED_USERS // 10]:
            _last_alert_ts.pop(old_uid, None)
    _last_alert_ts[uid] = ts


async def broadcast_alerts(coins: list[dict]) -> int:
    """Kirim daftar coin lolos screening ke seluruh subscriber 'active'.

    Returns jumlah pesan terkirim. Kegagalan per-user hanya di-log
    (user diblokir / token invalid tidak boleh menghentikan broadcast).

    Catatan arsitektur: db.*() (sqlite3 sinkron) sengaja dipanggil langsung
    karena setiap operasi bersifat short-lived (<5 ms) sehingga blocking time
    pada event loop Telegram dapat diabaikan. Jika suatu saat query memberat,
    bungkus dengan `await asyncio.to_thread(db.get_all_active_users)`.
    """
    global BOT_APP
    if not coins or BOT_APP is None:
        return 0

    active = await asyncio.to_thread(db.get_all_active_users)
    if not active:
        logger.info("Siklus: %d sinyal tapi tidak ada user aktif — skip", len(coins))
        return 0

    cooldown_s = CONFIG.BOT.ALERT_COOLDOWN_MINUTES * 60
    sent = 0
    for user in active:
        uid = int(user["telegram_id"])
        last = _last_alert_ts.get(uid, 0.0)
        if time.monotonic() - last < cooldown_s:
            continue  # masih dalam masa cooldown anti-spam
        for coin in coins:
            text = format_alert_message(coin)
            try:
                await BOT_APP.bot.send_message(
                    chat_id=uid, text=text, parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True)
                sent += 1
                await asyncio.sleep(0.35)  # hormati rate-limit Telegram
            except Exception as exc:
                logger.warning("Kirim alert ke %s gagal: %s", uid, exc)
                break  # user bermasalah -> jangan coba coin berikutnya
        _remember_alert(uid, time.monotonic())
    logger.info("Broadcast selesai: %d pesan ke %d user", sent, len(active))
    return sent


# ---------------------------------------------------------------------------
# 4) BACKGROUND SCREENER LOOP (asyncio task di loop bot)
# ---------------------------------------------------------------------------
async def screener_loop() -> None:
    """Loop tak-berhenti: screen_coins() -> broadcast. Tidak pernah crash;
    setiap exception di-catch, wait, lalu lanjut siklus berikutnya."""
    interval = CONFIG.BOT.POLL_INTERVAL_SECONDS
    cycle = 0
    logger.info("Screener loop mulai | interval=%ss | chains=%s",
                interval, CONFIG.BOT.CHAINS_WATCHED)
    while True:
        cycle += 1
        started = time.monotonic()
        try:
            coins = await screen_coins()
            if coins:
                logger.info("Siklus #%d: %d sinyal -> broadcast", cycle, len(coins))
                await broadcast_alerts(coins)
            else:
                logger.info("Siklus #%d: tidak ada sinyal", cycle)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Siklus #%d gagal total (loop tetap hidup)", cycle)
        # housekeeping: nonaktifkan langganan kedaluwarsa sekali per jam
        if cycle % max(1, 3600 // max(interval, 1)) == 0:
            try:
                n = db.deactivate_expired_users()
                if n:
                    logger.info("Housekeeping: %d user kedaluwarsa dinonaktifkan", n)
            except Exception:
                logger.exception("Housekeeping gagal (diabaikan)")
        elapsed = time.monotonic() - started
        await asyncio.sleep(max(5.0, interval - elapsed))


# ---------------------------------------------------------------------------
# 5) HANDLER TELEGRAM
# ---------------------------------------------------------------------------
def _is_admin(update: Update) -> bool:
    admin_id = CONFIG.SECRETS.TELEGRAM_ADMIN_ID
    uid = update.effective_user.id if update.effective_user else None
    return bool(admin_id) and uid == admin_id


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Daftarkan/update user ke database dengan status 'inactive'."""
    user = update.effective_user
    db.add_or_update_user(
        telegram_id=user.id,
        username=user.username or user.first_name,
        status=db.STATUS_INACTIVE,
    )
    logger.info("/start dari %s (%s) — terdaftar inactive", user.id, user.username)
    await update.message.reply_text(
        f"👋 Halo <b>{html.escape(user.first_name)}</b>!\n\n"
        f"Selamat datang di <b>CCF — Coin Cash Flow</b> 🐋🔥\n"
        f"Bot screening coin DEX: deteksi Whale Buy, Retail FOMO, "
        f"disaring anti-rug (GoPlus).\n\n"
        f"Status kamu saat ini: <b>INACTIVE</b> (belum bisa menerima alert).\n"
        f"Hubungi admin untuk aktivasi langganan.\n\n"
        f"Perintah:\n"
        f"/status — cek masa aktif langganan\n"
        f"/help — bantuan\n\n<i>DYOR — bukan saran finansial.</i>",
        parse_mode=ParseMode.HTML,
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Cek status langganan user dari database."""
    uid = update.effective_user.id
    row = db.get_user(uid)
    if row is None:
        await update.message.reply_text(
            "❌ Kamu belum terdaftar. Ketik /start dulu ya.")
        return
    active = db.is_user_active(uid)
    expiry = _fmt_dt(row["expiry_date"])
    state = "✅ ACTIVE" if active else "🚫 INACTIVE"
    await update.message.reply_text(
        f"📋 <b>Status Langganan CCF</b>\n\n"
        f"User    : @{html.escape(update.effective_user.username or '-')}\n"
        f"ID      : <code>{uid}</code>\n"
        f"Status  : <b>{state}</b>\n"
        f"Expiry  : {expiry}\n\n"
        + ("🎉 Alert akan dikirim otomatis ke chat ini."
           if active else
           "⚠️ Langganan belum aktif. Hubungi admin untuk aktivasi."),
        parse_mode=ParseMode.HTML,
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "🤖 <b>CCF — Coin Cash Flow</b>\n\n"
        "/start — daftar / reset pendaftaran\n"
        "/status — cek status langganan\n"
        "/help — pesan ini\n"
        + ("/admin_approve <id> [hari] — aktifkan user (khusus admin)\n"
           if _is_admin(update) else ""),
        parse_mode=ParseMode.HTML,
    )


async def cmd_admin_approve(update: Update,
                            context: ContextTypes.DEFAULT_TYPE) -> None:
    """/admin_approve <telegram_id> [days] — aktifkan user manual (ADMIN only)."""
    if not _is_admin(update):
        await update.message.reply_text("🚫 Akses ditolak. Command khusus admin.")
        return

    args = context.args or []
    if not args:
        await update.message.reply_text(
            "Format: /admin_approve <telegram_id> [hari]\n"
            "Contoh: /admin_approve 123456789 30")
        return
    try:
        target_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ telegram_id harus angka.")
        return
    days = 30
    if len(args) > 1:
        try:
            days = int(args[1])
        except ValueError:
            await update.message.reply_text("❌ Jumlah hari harus angka.")
            return

    row = db.get_user(target_id)
    if row is None:
        await update.message.reply_text(
            f"⚠️ User <code>{target_id}</code> belum pernah /start. "
            f"Tetap didaftarkan sebagai active.",
            parse_mode=ParseMode.HTML)

    expiry = datetime.now(timezone.utc) + timedelta(days=days)
    db.add_or_update_user(
        telegram_id=target_id,
        username=(row["username"] if row else None),
        status=db.STATUS_ACTIVE,
        expiry_date=expiry,
    )
    await update.message.reply_text(
        f"✅ User <code>{target_id}</code> diaktifkan s/d "
        f"{expiry.strftime('%d %b %Y %H:%M UTC')} ({days} hari).",
        parse_mode=ParseMode.HTML)
    # beri tahu user yang baru di-approve
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text="🎉 Langganan CCF kamu sudah <b>AKTIF</b>! "
                 "Alert 🐋/🔥 akan mulai masuk.",
            parse_mode=ParseMode.HTML)
    except Exception as exc:
        logger.info("Notifikasi approval ke %s gagal: %s", target_id, exc)


# ---------------------------------------------------------------------------
# 6) BOOT: Gradio + Telegram bot + screener, tanpa saling memblokir
# ---------------------------------------------------------------------------
def _run_bot_and_screener(token: str) -> None:
    """Dijalankan di thread khusus: punya event loop sendiri.

    - application.initialize/start -> polling Telegram
    - screener_loop() dibuat asyncio.create_task di loop yang sama
    - run_polling() mem-block selamanya (thread daemon, tidak mengganggu
      main thread yang melayani Gradio).
    """
    global BOT_APP, BOT_LOOP

    async def _post_init(app: Application) -> None:
        global BOT_LOOP
        BOT_LOOP = asyncio.get_running_loop()
        BOT_READY.set()
        logger.info("Telegram bot polling — menyiapkan screener loop…")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    app = (ApplicationBuilder()
           .token(token)
           .post_init(_post_init)
           .build())
    BOT_APP = app

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("admin_approve", cmd_admin_approve))

    screener_started = False

    async def _ensure_screener() -> None:
        """Screener HARUS hidup walau token Telegram invalid — dibuat sekali
        saja di loop ini, tidak bergantung pada keberhasilan polling."""
        nonlocal screener_started
        if not screener_started:
            screener_started = True
            loop.create_task(screener_loop(), name="ccf-screener-loop")

    async def _boot() -> None:
        await _ensure_screener()          # dulu: screener tidak boleh ikut mati
        await app.initialize()
        await app.start()
        await app.updater.start_polling(
            allowed_updates=["message"],
            drop_pending_updates=True)

    async def _shutdown() -> None:
        for coro in (app.updater.stop(), app.stop(), app.shutdown()):
            try:
                await coro
            except Exception:
                pass

    try:
        loop.run_until_complete(_boot())
        loop.run_forever()
    except InvalidToken as exc:
        logger.error("Token Telegram ditolak server (%s). Screener tetap "
                     "jalan; perbaiki Secret TELEGRAM_BOT_TOKEN lalu restart.",
                     exc)
        loop.run_until_complete(_ensure_screener())
        loop.run_forever()               # fallback: jalankan screener saja
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Bot thread dihentikan — shutdown bersih…")
        try:
            loop.run_until_complete(_shutdown())
        except Exception:
            pass
        loop.close()


def main() -> None:
    token = CONFIG.SECRETS.TELEGRAM_BOT_TOKEN

    # Database init sudah terjadi saat import modul `database`.
    if not token:
        # Mode degraded: Gradio tetap hidup (Space tidak tidur), bot tidak jalan.
        logger.warning(
            "TELEGRAM_BOT_TOKEN kosong — menjalankan Gradio saja. "
            "Set Secret di HF Spaces lalu restart Space.")
    else:
        bot_thread = threading.Thread(
            target=_run_bot_and_screener, args=(token,),
            name="ccf-bot", daemon=True)
        bot_thread.start()
        if not BOT_READY.wait(timeout=60):
            logger.warning("Bot belum ready dalam 60s — cek log/token/network.")

    # prevent_thread_lock=True -> launch() mengembalikan kontrol ke main
    # thread sehingga Gradio melayani web sambil bot jalan di thread lain.
    demo.launch(
        server_name="0.0.0.0",
        server_port=int(os.environ.get("SPACE_PORT", 7860)),
        share=False,
        quiet=False,
        prevent_thread_lock=True,
    )

    logger.info("Gradio UI aktif di port %s — Space kept-alive.",
                os.environ.get("SPACE_PORT", 7860))

    # Tahan proses utama agar HF tidak menganggap app selesai.
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        logger.info("Sinyal stop diterima — shutting down.")
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.shutdown(wait=False)


if __name__ == "__main__":
    main()
