"""
CCF (Coin Cash Flow) - Konfigurasi Terpusat
===========================================

Modul ini bertanggung jawab untuk:
1. Memuat rahasia (secrets) dari environment variables Hugging Face Spaces
   via ``os.environ.get()``  ->  TELEGRAM_BOT_TOKEN, TELEGRAM_ADMIN_ID.
2. Menyimpan seluruh konstanta threshold screening (LP, whale, retail FOMO,
   rug-pull filter, DexScreener/GoPlus endpoints) di satu tempat agar mudah
   diubah tanpa menyentuh kode bisnis.

Cara pakai::

    from config import CONFIG, is_configured

    if not is_configured():
        raise RuntimeError("TELEGRAM_BOT_TOKEN belum di-set!")
    min_lp = CONFIG.LP_MIN_USD

Catatan:
- Nilai bertipe *float* dapat dioverride lewat env var dengan nama yang sama
  (contoh: ``LP_MIN_USD=8000``), sehingga threshold bisa disetel ulang di
  HF Spaces Secrets tanpa deploy ulang.
- Jangan pernah hardcode token di file ini dan jangan commit .env.
"""

from __future__ import annotations

import os
from typing import Final


# ---------------------------------------------------------------------------
# Helper: pembaca environment variable
# ---------------------------------------------------------------------------

def _env_str(name: str, default: str = "") -> str:
    """Ambil string dari env var, strip whitespace."""
    return os.environ.get(name, default).strip()


def _env_int(name: str, default: int) -> int:
    """Ambil integer dari env var dengan fallback aman."""
    raw = _env_str(name)
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    """Ambil float dari env var dengan fallback aman."""
    raw = _env_str(name)
    try:
        return float(raw) if raw else default
    except ValueError:
        return default


# ---------------------------------------------------------------------------
# 1. RAHASIA (Hugging Face Secrets / environment variables)
# ---------------------------------------------------------------------------

class Secrets:
    """Rahasia runtime — SELALU dibaca dari environment, never hardcoded."""

    # Token bot Telegram dari @BotFather (HF Secret: TELEGRAM_BOT_TOKEN)
    TELEGRAM_BOT_TOKEN: str = _env_str("TELEGRAM_BOT_TOKEN")

    # Chat-ID admin untuk notifikasi & perintah khusus (HF Secret: TELEGRAM_ADMIN_ID)
    TELEGRAM_ADMIN_ID: int = _env_int("TELEGRAM_ADMIN_ID", default=0)

    # Flag debug (bukan rahasia, tapi ikut env-driven)
    DEBUG: bool = _env_str("DEBUG", "false").lower() in ("1", "true", "yes")


def is_configured() -> bool:
    """Validasi cepat: apakah secrets wajib sudah terisi?"""
    return bool(Secrets.TELEGRAM_BOT_TOKEN) and Secrets.TELEGRAM_ADMIN_ID != 0


# ---------------------------------------------------------------------------
# 2. KONSTANTA THRESHOLD SCREENING
# ---------------------------------------------------------------------------

class Thresholds:
    """Batas-batas screening coin. Semua bisa dioverride via env var."""

    # ---- Liquidity Pool (LP) ----
    LP_MIN_USD: float = _env_float("LP_MIN_USD", 5_000.0)        # batas minimum LP USD
    LP_MAX_USD: float = _env_float("LP_MAX_USD", 5_000_000.0)    # skip coin terlalu besar (slippage kecil, bukan target)

    # ---- Volume & Aktivitas ----
    VOLUME_24H_MIN_USD: float = _env_float("VOLUME_24H_MIN_USD", 10_000.0)
    TXNS_24H_MIN: int = _env_int("TXNS_24H_MIN", 50)             # minimal total transaksi 24h
    AGE_MIN_HOURS: float = _env_float("AGE_MIN_HOURS", 1.0)      # coin terlalu baru rawan rug
    AGE_MAX_HOURS: float = _env_float("AGE_MAX_HOURS", 72.0)     # fokus koin "baru tapi terbukti"

    # ---- Whale Detection ----
    WHALE_TX_USD: float = _env_float("WHALE_TX_USD", 300.0)          # batas single tx Buy dianggap whale
    WHALE_TX_LP_PCT_MIN: float = _env_float("WHALE_TX_LP_PCT_MIN", 1.0)  # nilai tx harus > x% dari total LP
    WHALE_HOLDING_PCT: float = _env_float("WALE_HOLDING_PCT", 5.0)   # top holder > x% = bahaya konsentrasi
    WHALE_NET_BUY_RATIO_MIN: float = _env_float("WHALE_NET_BUY_RATIO_MIN", 1.5)  # buy/sell ratio whale

    # ---- Retail FOMO Detection ----
    FOMO_WINDOW_SECONDS: int = _env_int("FOMO_WINDOW_SECONDS", 180)              # window pengamatan (3 menit)
    FOMO_UNIQUE_BUY_WALLETS_MIN: int = _env_int("FOMO_UNIQUE_BUY_WALLETS_MIN", 15)  # >15 wallet unik Buy dalam window
    FOMO_BUY_VOLUME_RATIO_MIN: float = _env_float("FOMO_BUY_VOLUME_RATIO_MIN", 3.0)  # Vol Buy > 3x Vol Sell
    FOMO_PRICE_CHANGE_MIN_PCT: float = _env_float("FOMO_PRICE_CHANGE_MIN_PCT", 5.0)   # harga naik > 5% (window m5)

    # ---- Retail FOMO Detection (versi lama, dipertahankan utk kompatibilitas) ----
    RETAIL_FOMO_TX_COUNT_MIN: int = _env_int("RETAIL_FOMO_TX_COUNT_MIN", 100)   # ledakan jumlah transaksi
    RETAIL_FOMO_AVG_TX_USD_MAX: float = _env_float("RETAIL_FOMO_AVG_TX_USD_MAX", 50.0)  # rata-rata tx kecil = retail
    RETAIL_FOMO_BUY_RATIO_MIN: float = _env_float("RETAIL_FOMO_BUY_RATIO_MIN", 0.65)     # >65% arah buy

    # ---- Rug-Pull Filter (GoPlus Security) ----
    RUG_MAX_HOLDER_CONCENTRATION_PCT: float = _env_float(
        "RUG_MAX_HOLDER_CONCENTRATION_PCT", 30.0
    )
    RUG_REJECT_IF_MINTABLE: bool = _env_str("RUG_REJECT_IF_MINTABLE", "true").lower() in ("1", "true", "yes")
    RUG_REJECT_IF_FREEZABLE: bool = _env_str("RUG_REJECT_IF_FREEZABLE", "true").lower() in ("1", "true", "yes")
    RUG_REJECT_IF_TAXABLE: bool = _env_str("RUG_REJECT_IF_TAXABLE", "true").lower() in ("1", "true", "yes")
    RUG_MIN_LP_LOCK_PERCENT: float = _env_float("RUG_MIN_LP_LOCK_PERCENT", 0.0)  # 0 = tidak mewajibkan lock
    RUG_MAX_TAX_PERCENT: float = _env_float("RUG_MAX_TAX_PERCENT", 10.0)         # buy/sell tax maksimum

    # ---- Skor Lulus Screening ----
    SCORE_PASS_MIN: int = _env_int("SCORE_PASS_MIN", 60)         # skor agregat 0-100


# ---------------------------------------------------------------------------
# 3. API ENDPOINTS & RATE LIMIT (semua gratis, tanpa API key berbayar)
# ---------------------------------------------------------------------------

class Api:
    """Endpoint DexScreener & GoPlus + pengaturan HTTP."""

    # DexScreener Public API (gratis, tanpa key)
    DEXSCREENER_BASE: str = _env_str("DEXSCREENER_BASE", "https://api.dexscreener.com")
    DEXSCREENER_PAIRS: str = f"{DEXSCREENER_BASE}/latest/dex/pairs"
    DEXSCREENER_SEARCH: str = f"{DEXSCREENER_BASE}/latest/dex/search"
    DEXSCREENER_TOKENS: str = f"{DEXSCREENER_BASE}/tokens/v1"
    # Semua pair untuk satu token address -> dipakai utk fetch transaksi (m5/h1)
    DEXSCREENER_TOKEN_PAIRS: str = f"{DEXSCREENER_BASE}/latest/dex/tokens"

    # GeckoTerminal Public API (gratis, tanpa key — fallback / sumber trending)
    GECKOTERMINAL_BASE: str = _env_str("GECKOTERMINAL_BASE", "https://api.geckoterminal.com/api/v2")
    GECKO_TRENDING_POOLS: str = f"{GECKOTERMINAL_BASE}/networks/{{chain}}/trending_pools"
    GECKO_NEW_POOLS: str = f"{GECKOTERMINAL_BASE}/networks/{{chain}}/new_pools"
    GECKO_POOL_TXNS: str = f"{GECKOTERMINAL_BASE}/networks/{{chain}}/pools/{{pool}}/transactions"

    # GoPlus Security API (gratis, tanpa key)
    GOPLUS_BASE: str = _env_str("GOPLUS_BASE", "https://api.gopluslabs.io")
    GOPLUS_TOKEN_SECURITY: str = f"{GOPLUS_BASE}/api/v1/token_security"

    # HTTP
    HTTP_TIMEOUT_SECONDS: float = _env_float("HTTP_TIMEOUT_SECONDS", 15.0)
    HTTP_MAX_RETRIES: int = _env_int("HTTP_MAX_RETRIES", 3)
    USER_AGENT: Final[str] = "CCF-Bot/1.0 (+https://huggingface.co/spaces)"

    # Rate limit sederhana (request per menit) agar tidak kena 429
    DEXSCREENER_RPM_LIMIT: int = _env_int("DEXSCREENER_RPM_LIMIT", 60)
    GOPLUS_RPM_LIMIT: int = _env_int("GOPLUS_RPM_LIMIT", 30)


# ---------------------------------------------------------------------------
# 4. DATABASE & PATH
# ---------------------------------------------------------------------------

class Storage:
    """Lokasi SQLite lokal (tanpa server terpisah)."""

    DB_FILENAME: str = _env_str("DB_FILENAME", "ccf_users.db")
    DATA_DIR: str = _env_str("DATA_DIR", os.path.join(os.getcwd(), "data"))

    @property
    def db_path(self) -> str:
        return os.path.join(self.DATA_DIR, self.DB_FILENAME)


STORAGE = Storage()

# Tabel utama (dibuat oleh modul database.py nanti)
TABLE_SCHEMA_HINT: Final[str] = (
    "screened_tokens(address, chain, symbol, name, lp_usd, volume_24h, "
    "score, whale_flag, retail_fomo_flag, rug_flag, screened_at)"
)


# ---------------------------------------------------------------------------
# 5. TELEGRAM BOT BEHAVIOR
# ---------------------------------------------------------------------------

class Bot:
    POLL_INTERVAL_SECONDS: int = _env_int("POLL_INTERVAL_SECONDS", 60)
    MAX_RESULTS_PER_ALERT: int = _env_int("MAX_RESULTS_PER_ALERT", 10)
    CHAINS_WATCHED: tuple = tuple(
        c.strip() for c in _env_str(
            "CHAINS_WATCHED", "ethereum,bsc,solana,base"
        ).split(",") if c.strip()
    )
    ALERT_COOLDOWN_MINUTES: int = _env_int("ALERT_COOLDOWN_MINUTES", 30)


# ---------------------------------------------------------------------------
# Objek konfigurasi tunggal (facade) — dipakai modul lain
# ---------------------------------------------------------------------------

class _Config:
    SECRETS = Secrets
    THRESHOLDS = Thresholds
    API = Api
    BOT = Bot
    STORAGE = STORAGE


CONFIG = _Config()


# ---------------------------------------------------------------------------
# Self-test ringan saat dijalankan langsung:  python config.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 60)
    print("CCF Config Loader — ringkasan")
    print("=" * 60)
    print(f"Telegram token loaded : {bool(Secrets.TELEGRAM_BOT_TOKEN)}")
    print(f"Admin ID              : {Secrets.TELEGRAM_ADMIN_ID or '(belum diset)'}")
    print(f"is_configured()       : {is_configured()}")
    print(f"Min LP (USD)          : {Thresholds.LP_MIN_USD:,.0f}")
    print(f"Batas Whale (USD/tx)  : {Thresholds.WHALE_TX_USD:,.0f}")
    print(f"Chains watched        : {', '.join(Bot.CHAINS_WATCHED)}")
    print(f"SQLite path           : {STORAGE.db_path}")
    print(f"DexScreener base      : {Api.DEXSCREENER_BASE}")
    print(f"GoPlus security URL   : {Api.GOPLUS_TOKEN_SECURITY}")
