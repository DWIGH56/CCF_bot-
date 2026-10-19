"""
CCF (Coin Cash Flow) — Screener Engine
=====================================

"Otak" dari bot CCF. Modul async ini melakukan screening coin DEX secara
end-to-end dengan API gratis (tanpa API key berbayar):

Alur screening (``screen_coins``):
1. FETCH        — ambil kandidat pair dari DexScreener:
                  a) "trending"  : `/latest/dex/pairs/{chain}/{dex}` (top pools)
                     fallback    : GeckoTerminal trending_pools / new_pools
                  b) "new"       : token baru via GeckoTerminal new_pools
2. PRE-FILTER   — filter cepat berbasis data pair itu sendiri:
                  LP dalam rentang, volume 24h, umur coin, harga ada.
3. ANTI-RUG     — GoPlus Security API (`token_security`). Buang token jika:
                  honeypot == True, buy/sell tax > 10%, atau LP < $5000.
4. WHALE        — dari transaksi window terakhir (DexScreener txns m5/h1):
                  Buy > $300 DAN nilai tx > 1% total LP  ->  alert "WHALE BUY".
5. RETAIL FOMO  — dalam window 3 menit: > 15 wallet unik Buy,
                  Volume Buy > 3x Volume Sell, harga naik > 5% -> "RETAIL FOMO".
6. OUTPUT       — list[dict] coin yang lolos + kena threshold, siap diformat
                  menjadi pesan Telegram oleh bot.py.

Ketahanan (robustness):
- SEMUA panggilan API dibungkus try/except + retry dengan backoff.
- Kegagalan pada SATU token hanya di-log, loop utama lanjut ke token
  berikutnya — tidak pernah crash.
- Session aiohttp dipakai bersama (connection pooling) & ditutup rapi.
- Semaphore membatasi konkurensi agar tidak kena rate-limit 429.

Cara pakai::

    import asyncio
    from screener import run_screening_cycle

    results = asyncio.run(run_screening_cycle())
    for coin in results:
        print(coin["symbol"], coin["signals"])   # e.g. ["WHALE BUY", "RETAIL FOMO"]

Jalankan langsung untuk smoke-test offline (mock data, tanpa internet)::

    python screener.py
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

import aiohttp

from config import CONFIG

logger = logging.getLogger("ccf.screener")

# ---------------------------------------------------------------------------
# Konstanta lokal (di luar threshold bisnis yang hidup di config.py)
# ---------------------------------------------------------------------------
MAX_CONCURRENCY = 5            # maksimal token diproses paralel
TXNS_FETCH_LIMIT = 80          # ambil maks N transaksi terakhir per pair
ALERT_WHALE_BUY = "WHALE BUY"
ALERT_RETAIL_FOMO = "RETAIL FOMO"

# Peta chain CCF -> id chain versi GoPlus (numeric) & GeckoTerminal (network slug)
GOPLUS_CHAIN_IDS: dict[str, str] = {
    "ethereum": "1",
    "bsc": "56",
    "polygon": "137",
    "base": "8453",
    "arbitrum": "42161",
    # Solana tidak didukung GoPlus token_security -> dilewati (dianggap netral)
}
GECKO_CHAIN_SLUGS: dict[str, str] = {
    "ethereum": "eth",
    "bsc": "bsc",
    "solana": "solana",
    "base": "base",
    "polygon": "polygon_pos",
    "arbitrum": "arbitrum",
}


def _to_float(value: Any, default: float = 0.0) -> float:
    """Konversi string/number API yang tidak konsisten menjadi float aman."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Hasil deteksi
# ---------------------------------------------------------------------------

@dataclass
class DetectionResult:
    """Hasil analisis whale/FOMO satu token."""
    whale_detected: bool = False
    fomo_detected: bool = False
    whale_details: dict[str, Any] = field(default_factory=dict)
    fomo_details: dict[str, Any] = field(default_factory=dict)

    @property
    def signals(self) -> list[str]:
        out = []
        if self.whale_detected:
            out.append(ALERT_WHALE_BUY)
        if self.fomo_detected:
            out.append(ALERT_RETAIL_FOMO)
        return out


# ---------------------------------------------------------------------------
# HTTP layer — semua request lewat sini (retry + error handling terpusat)
# ---------------------------------------------------------------------------

class HttpFetcher:
    """Wrapper aiohttp dengan retry/backoff dan error handling wajib."""

    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None
        headers = {"User-Agent": CONFIG.API.USER_AGENT, "Accept": "application/json"}
        timeout = aiohttp.ClientTimeout(total=CONFIG.API.HTTP_TIMEOUT_SECONDS)
        self._session = aiohttp.ClientSession(timeout=timeout, headers=headers)

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def get_json(self, url: str, params: Optional[dict] = None) -> Optional[Any]:
        """GET JSON dengan retry. Return None bila semua percobaan gagal."""
        last_err: Optional[Exception] = None
        for attempt in range(1, CONFIG.API.HTTP_MAX_RETRIES + 1):
            try:
                async with self._session.get(url, params=params) as resp:
                    if resp.status == 429:  # rate-limited -> tunggu & coba lagi
                        retry_after = _to_float(resp.headers.get("Retry-After"), 2.0)
                        logger.warning("429 %s (attempt %d), wait %.1fs",
                                       url.split("?")[0], attempt, retry_after)
                        await asyncio.sleep(min(retry_after, 10.0))
                        continue
                    if resp.status >= 400:
                        raise aiohttp.ClientResponseError(
                            resp.request_info, resp.history, status=resp.status,
                            message=f"HTTP {resp.status}")
                    return await resp.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_err = exc
                backoff = min(2 ** attempt * 0.5, 8.0)
                logger.debug("Request gagal (%s/%s) attempt %d: %s",
                             type(exc).__name__, url[:80], attempt, exc)
                await asyncio.sleep(backoff)
            except Exception as exc:  # JSON rusak dll — jangan lanjut retry buta
                last_err = exc
                logger.debug("Non-HTTP error utk %s: %s", url[:80], exc)
                break
        logger.error("Gagal fetch %s setelah %d attempt: %s",
                     url.split("?")[0], CONFIG.API.HTTP_MAX_RETRIES, last_err)
        return None


# ---------------------------------------------------------------------------
# 1. FETCH — kandidat pair (DexScreener utama, GeckoTerminal fallback)
# ---------------------------------------------------------------------------

async def fetch_trending_pairs(http: HttpFetcher, chain: str) -> list[dict]:
    """Daftar pair 'panas' utk satu chain dari DexScreener.

    DexScreener tidak punya endpoint trending global tanpa key; pendekatan
    gratis yang stabil: query search per chain lalu sort by volume/LP, atau
    pakai GeckoTerminal trending_pools sebagai sumber ranking lalu kayakan
    datanya. Di sini kita gabungkan keduanya (dedup by base token).
    """
    pairs: list[dict] = []

    # --- Sumber A: GeckoTerminal trending pools (ranking terbaik) ---
    slug = GECKO_CHAIN_SLUGS.get(chain)
    if slug:
        url = CONFIG.API.GECKO_TRENDING_POOLS.format(chain=slug)
        data = await http.get_json(url, params={"page": 1})
        for pool in (data or {}).get("data", [])[:20]:
            attr = pool.get("attributes", {}) or {}
            rel = pool.get("relationships", {}) or {}
            base_addr = (rel.get("base_token", {}).get("data", {}) or {}).get("id", "")
            base_addr = base_addr.split("_")[-1] if base_addr else ""
            lp_usd = _to_float(attr.get("reserve_in_usd"))
            price = _to_float(attr.get("base_token_price_usd"))
            if not base_addr or price <= 0:
                continue
            changes = attr.get("price_change_percentage", {}) or {}
            vol = attr.get("volume_usd", {}) or {}
            txns = attr.get("transactions", {}) or {}
            pairs.append({
                "chain": chain,
                "source": "geckoterminal",
                "dex_id": (rel.get("dex", {}).get("data", {}) or {}).get("id", ""),
                "pair_address": pool.get("id", "").split("_")[-1],
                "base_token_address": base_addr,
                "symbol": attr.get("name", "?").split("/")[0],
                "name": attr.get("name", ""),
                "price_usd": price,
                "lp_usd": lp_usd,
                "volume_24h_usd": _to_float(vol.get("h24")),
                "txns_24h": int(_to_float((txns.get("h24") or {}).get("buys"))
                                + _to_float((txns.get("h24") or {}).get("sells"))),
                "price_change_m5": _to_float(changes.get("m5")),
                "price_change_h1": _to_float(changes.get("h1")),
                "created_at": attr.get("pool_created_at"),
                "url": f"https://dexscreener.com/{chain}/{pool.get('id','').split('_')[-1]}",
            })

    # --- Sumber B: DexScreener — top pools per chain via token-pairs endpoint
    # (pakai token bluechip chain tsb sbg query; response berisi banyak pair
    #  yang bisa di-ranking by volume). Ini pengganti /search yg tidak support
    #  query nama chain.
    CHANTOP_TOKEN = {
        "ethereum": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",   # WETH
        "bsc": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",          # WBNB
        "polygon": "0x0d500B1d8E8eF31E21C99d1Ec99E8Bb8fb735770",      # WMATIC
        "base": "0x4200000000000000000000000000000000000006",         # WETH
        "arbitrum": "0x82aF49447D8a07e3BD9f373241d95ccfB22fbaAd",     # WETH
        "solana": "So11111111111111111111111111111111111111112",      # wSOL
    }
    topo = CHANTOP_TOKEN.get(chain.lower())
    if topo:
        # Bentuk benar: /latest/dex/tokens/{tokenAddress} (tanpa prefix chain)
        data = await http.get_json(f"{CONFIG.API.DEXSCREENER_TOKEN_PAIRS}/{topo}")
        seen = {p["base_token_address"].lower() for p in pairs}
        for p in (data or {}).get("pairs", []) or []:
            try:
                if (p.get("chainId") or "").lower() != chain.lower():
                    continue
                base = (p.get("baseToken") or {}).get("address")
                quote = (p.get("quoteToken") or {}).get("address")
                # pilih sisi non-bluechip sebagai kandidat token
                cand, other = (base, quote) if base != topo else (quote, base)
                if not cand or cand.lower() in seen or cand == topo:
                    continue
                seen.add(cand.lower())
                txns = p.get("txns", {}) or {}
                h24 = txns.get("h24", {}) or {}
                created_ms = p.get("pairCreatedAt")
                created_iso = (datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc)
                               .isoformat() if created_ms else None)
                pairs.append({
                    "chain": chain,
                    "source": "dexscreener",
                    "dex_id": p.get("dexId", ""),
                    "pair_address": p.get("pairAddress", ""),
                    "base_token_address": cand,
                    "symbol": (p.get("baseToken") or {}).get("symbol", "?") if cand == base else (p.get("quoteToken") or {}).get("symbol", "?"),
                    "name": (p.get("baseToken") or {}).get("name", "") if cand == base else (p.get("quoteToken") or {}).get("name", ""),
                    "price_usd": _to_float(p.get("priceUsd")),
                    "lp_usd": _to_float((p.get("liquidity") or {}).get("usd")),
                    "volume_24h_usd": _to_float((p.get("volume") or {}).get("h24")),
                    "txns_24h": int(_to_float(h24.get("buys")) + _to_float(h24.get("sells"))),
                    "price_change_m5": _to_float((p.get("priceChange") or {}).get("m5")),
                    "price_change_h1": _to_float((p.get("priceChange") or {}).get("h1")),
                    "created_at": created_iso,
                    "url": p.get("url", ""),
                })
            except Exception as exc:  # satu pair rusak tidak boleh ganggu yang lain
                logger.debug("Skip pair DexScreener cacat: %s", exc)

    # Ranking: volume 24h tertinggi dulu, ambil atasnya saja utk hemat quota GoPlus
    pairs.sort(key=lambda x: x["volume_24h_usd"], reverse=True)
    return pairs[: CONFIG.BOT.MAX_RESULTS_PER_ALERT * 3]


# ---------------------------------------------------------------------------
# 2. PRE-FILTER — saringan murah sebelum panggil GoPlus
# ---------------------------------------------------------------------------

def passes_prefilter(pair: dict) -> tuple[bool, str]:
    """Return (lolos, alasan_ditolak). Hemat kuota: cek dulu, baru security."""
    t = CONFIG.THRESHOLDS
    if pair["price_usd"] <= 0:
        return False, "harga tidak tersedia"
    if pair["lp_usd"] < t.LP_MIN_USD:
        return False, f"LP ${pair['lp_usd']:,.0f} < ${t.LP_MIN_USD:,.0f}"
    if pair["lp_usd"] > t.LP_MAX_USD:
        return False, f"LP ${pair['lp_usd']:,.0f} > ${t.LP_MAX_USD:,.0f}"
    if pair["volume_24h_usd"] < t.VOLUME_24H_MIN_USD:
        return False, f"vol24h ${pair['volume_24h_usd']:,.0f} terlalu kecil"
    if pair["txns_24h"] < t.TXNS_24H_MIN:
        return False, f"txns24h {pair['txns_24h']} terlalu sepi"
    created = pair.get("created_at")
    if created:
        try:
            dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
            age_h = (_utcnow() - dt).total_seconds() / 3600
            if age_h < t.AGE_MIN_HOURS:
                return False, f"terlalu baru ({age_h:.1f}h)"
            if age_h > t.AGE_MAX_HOURS:
                return False, f"terlalu tua ({age_h:.0f}h)"
        except (ValueError, TypeError):
            pass  # format tanggal tak dikenal -> jangan tolak buta
    return True, ""


# ---------------------------------------------------------------------------
# 3. ANTI-RUG — GoPlus Security API
# ---------------------------------------------------------------------------

async def check_goplus_security(http: HttpFetcher, chain: str,
                                token_address: str) -> tuple[bool, dict]:
    """Cek rug-pull risk via GoPlus. Return (aman, detail).

    Dibuang bila: honeypot True | buy/sell tax > RUG_MAX_TAX_PERCENT |
    mintable/freezable aktif | holder top-10 terkonsentrasi.
    LP < minimum sudah ditangani pre-filter (data DexScreener).
    Chain tanpa dukungan GoPlus (mis. solana) -> netral (aman bersyarat).
    """
    detail: dict[str, Any] = {"checked": False, "reasons": []}
    chain_id = GOPLUS_CHAIN_IDS.get(chain.lower())
    if not chain_id:
        detail["reasons"].append(f"GoPlus tidak mendukung chain '{chain}' (skip)")
        return True, detail  # netral — tetap butuh sinyal kuat utk lolos

    url = f"{CONFIG.API.GOPLUS_TOKEN_SECURITY}/{chain_id}"
    data = await http.get_json(url, params={"contract_addresses": token_address})
    # GoPlus mengembalikan {"code": 1, "message": "OK", "result": {...}}
    if not data or int(_to_float(data.get("code"), -1)) != 1:
        # API gagal -> perlakukan TIDAK aman (fail-safe, jangan alert palsu)
        detail["reasons"].append("GoPlus tidak dapat dijangkau")
        return False, detail

    result_map = data.get("result") or {}
    result = (result_map.get(token_address.lower())
              or result_map.get(token_address) or {})
    if isinstance(result, list):          # beberapa versi API mengembalikan list
        result = result[0] if result else {}
    if not result:
        detail["reasons"].append("token tidak terdaftar di GoPlus")
        return False, detail

    t = CONFIG.THRESHOLDS
    reasons: list[str] = []
    if str(result.get("is_honeypot", "0")) == "1":
        reasons.append("honeypot=true")
    buy_tax = _to_float(result.get("buy_tax")) * 100
    sell_tax = _to_float(result.get("sell_tax")) * 100
    if buy_tax > t.RUG_MAX_TAX_PERCENT or sell_tax > t.RUG_MAX_TAX_PERCENT:
        reasons.append(f"tax tinggi (buy {buy_tax:.1f}% / sell {sell_tax:.1f}%)")
    if t.RUG_REJECT_IF_MINTABLE and str(result.get("is_mintable", "0")) == "1":
        reasons.append("token bisa di-mint ulang")
    if t.RUG_REJECT_IF_FREEZABLE and str(result.get("is_freezable", "0")) == "1":
        reasons.append("token bisa di-freeze")
    holders = result.get("holders") or []
    if holders:
        # GoPlus mengembalikan `percent` sebagai string PERSEN (mis. "12.34"
        # = 12,34%) — JANGAN dikali 100 lagi.
        top10 = sum(_to_float(h.get("percent")) for h in holders[:10])
        if top10 > t.RUG_MAX_HOLDER_CONCENTRATION_PCT:
            reasons.append(f"top-10 holder {top10:.1f}%")
        detail["holder_count"] = int(_to_float(result.get("holder_count")))

    detail.update({"checked": True, "buy_tax_pct": round(buy_tax, 2),
                   "sell_tax_pct": round(sell_tax, 2), "reasons": reasons})
    return (len(reasons) == 0), detail


# ---------------------------------------------------------------------------
# 4 & 5. WHALE + RETAIL FOMO — analisis transaksi on-chain (via DexScreener)
# ---------------------------------------------------------------------------

async def fetch_recent_txns(http: HttpFetcher, chain: str,
                             pair_address: str,
                             token_address: Optional[str] = None) -> list[dict]:
    """Transaksi terbaru utk satu pair.

    Sumber utama: DexScreener `/latest/dex/pairs/{chain}/{pair}` — berisi
    agregat tx per window (m5/h1/h6/h24): jumlah buys/sells + volume USD.
    Fallback: GeckoTerminal pool info (agregat serupa).

    Catatan API gratis: DexScreener public TIDAK mengembalikan daftar wallet
    individual, jadi "wallet unik" diproksikan dengan jumlah buy-tx dalam
    window (asumsi tiap tx = wallet berbeda; digabung 2 syarat FOMO lain
    hasilnya tetap konservatif). Whale single-tx diproksikan dari rata-rata
    volume buy per tx pada window terkecil yang memenuhi ambang.
    """
    out: list[dict] = []

    def _windows_from_txns(txns: dict) -> list[dict]:
        rows = []
        for window_key, seconds in (("m5", 300), ("h1", 3600)):
            w = txns.get(window_key) or {}
            buys_n = int(_to_float(w.get("buys")))
            sells_n = int(_to_float(w.get("sells")))
            vol_buy = _to_float(w.get("buyVolume"))
            vol_sell = _to_float(w.get("sellVolume"))
            avg_buy = vol_buy / buys_n if buys_n else 0.0
            # Proxy single-tx TERKECIL yang mungkin memenuhi ambang whale:
            # hanya rata-rata buy per tx (BUKAN volume total window — itu
            # akan memicu false-positive pada pool ramai). Pool dengan satu
            # whale besar tetap terdeteksi krn rata-rata ikut melonjak.
            est_largest = avg_buy * 3 if buys_n else 0.0   # faktor ekor konservatif
            rows.append({
                "window": window_key,
                "window_seconds": seconds,
                "buys": buys_n, "sells": sells_n,
                "buy_volume_usd": vol_buy, "sell_volume_usd": vol_sell,
                "avg_buy_usd": round(avg_buy, 2),
                "largest_single_buy_usd": est_largest,
            })
        return rows

    # --- Sumber utama: DexScreener pairs endpoint (dengan prefix chain) ---
    url = f"{CONFIG.API.DEXSCREENER_PAIRS}/{chain}/{pair_address}"
    data = await http.get_json(url)
    pair_obj = None
    ps = (data or {}).get("pairs")
    if isinstance(ps, list):
        for p in ps:
            if isinstance(p, dict) and (p.get("pairAddress") or "").lower() == pair_address.lower():
                pair_obj = p
                break
    if pair_obj is None and isinstance((data or {}).get("pair"), dict):
        pair_obj = data["pair"]

    if pair_obj:
        return _windows_from_txns(pair_obj.get("txns") or {})

    # --- Fallback: GeckoTerminal pool info ---
    slug = GECKO_CHAIN_SLUGS.get(chain)
    if slug:
        gt = await http.get_json(
            f"{CONFIG.API.GECKOTERMINAL_BASE}/networks/{slug}/pools/{pair_address}")
        attr = ((gt or {}).get("data") or {}).get("attributes") or {}
        txns = attr.get("transactions") or {}
        changes = attr.get("price_change_percentage") or {}
        for window_key, gt_key, seconds in (("m5", "m5", 300), ("h1", "h1", 3600)):
            w = txns.get(gt_key) or {}
            buys_n = int(_to_float(w.get("buys")))
            sells_n = int(_to_float(w.get("sells")))
            vol = (attr.get("volume_usd") or {})
            # GT tidak memecah volume per arah di level ini -> pakai rasio tx
            total_vol = _to_float(vol.get("h24"))
            vol_buy = total_vol * (buys_n / max(buys_n + sells_n, 1)) if window_key == "h1" else 0.0
            vol_sell = total_vol - vol_buy if window_key == "h1" else 0.0
            avg_buy = vol_buy / buys_n if buys_n else 0.0
            out.append({
                "window": window_key, "window_seconds": seconds,
                "buys": buys_n, "sells": sells_n,
                "buy_volume_usd": vol_buy, "sell_volume_usd": vol_sell,
                "avg_buy_usd": round(avg_buy, 2),
                "largest_single_buy_usd": max(vol_buy, avg_buy * 3) if buys_n else 0.0,
            })
    return out


def analyze_signals(pair: dict, txns: list[dict]) -> DetectionResult:
    """Terapkan aturan whale & retail-FOMO terhadap data transaksi."""
    t = CONFIG.THRESHOLDS
    res = DetectionResult()
    lp = pair["lp_usd"]
    if lp <= 0:
        return res

    # ---- WHALE BUY: Buy > $300 DAN nilai tx > 1% dari total LP ----
    whale_min_abs = t.WHALE_TX_USD
    whale_min_pct = lp * (t.WHALE_TX_LP_PCT_MIN / 100.0)
    for w in txns:
        biggest = w["largest_single_buy_usd"]
        if biggest > whale_min_abs and biggest > whale_min_pct:
            res.whale_detected = True
            res.whale_details = {
                "window": w["window"],
                "tx_usd": round(biggest, 2),
                "lp_pct": round(biggest / lp * 100, 2),
            }
            break

    # ---- RETAIL FOMO: window 3 menit ----
    # >15 wallet unik Buy, Vol Buy > 3x Vol Sell, harga naik > 5%.
    # Proxy wallet unik: jumlah buys window m5 (asumsi wallet berbeda;
    # angka sebenarnya lebih rendah, jadi ini batas ATAS — tetap konservatif
    # karena digabung 2 syarat lain).
    m5 = next((w for w in txns if w["window"] == "m5"), None)
    if m5:
        unique_buy_wallets = m5["buys"]
        vol_buy = m5["buy_volume_usd"]
        vol_sell = m5["sell_volume_usd"]
        price_up = pair.get("price_change_m5", 0.0)
        ratio_ok = vol_buy > t.FOMO_BUY_VOLUME_RATIO_MIN * max(vol_sell, 1e-9)
        if (unique_buy_wallets > t.FOMO_UNIQUE_BUY_WALLETS_MIN
                and ratio_ok and price_up > t.FOMO_PRICE_CHANGE_MIN_PCT):
            res.fomo_detected = True
            res.fomo_details = {
                "unique_buy_wallets~": unique_buy_wallets,
                "buy_volume_usd": round(vol_buy, 2),
                "sell_volume_usd": round(vol_sell, 2),
                "buy_sell_ratio": round(vol_buy / max(vol_sell, 1e-9), 2),
                "price_change_m5_pct": price_up,
            }
    return res


# ---------------------------------------------------------------------------
# Pipeline utama per-token (dipanggil paralel dgn semaphore)
# ---------------------------------------------------------------------------

async def screen_pair(http: HttpFetcher, pair: dict,
                      sem: asyncio.Semaphore) -> Optional[dict]:
    """Screening lengkap satu kandidat. SELALU return None saat gagal —
    exception tidak dibiarkan lolos ke loop utama."""
    symbol = pair.get("symbol", "?")
    addr = pair.get("base_token_address", "?")
    try:
        async with sem:
            # 2. Pre-filter (gratis, tanpa API call)
            ok, reason = passes_prefilter(pair)
            if not ok:
                logger.info("[%s] ditolak pre-filter: %s", symbol, reason)
                return None

            # 3. Anti-rug (GoPlus)
            safe, rug_detail = await check_goplus_security(
                http, pair["chain"], addr)
            if not safe:
                logger.info("[%s %s] dibuang anti-rug: %s",
                            symbol, addr[:10], "; ".join(rug_detail["reasons"]))
                return None

            # 4-5. Transaksi -> whale & FOMO
            txns = await fetch_recent_txns(
                http, pair["chain"], pair["pair_address"])
            detection = analyze_signals(pair, txns)
            if not detection.signals:
                logger.debug("[%s] lolos filter tapi tanpa sinyal", symbol)
                return None

            # 6. Rakit hasil siap-Telegram
            score = _compute_score(pair, detection)
            if score < CONFIG.THRESHOLDS.SCORE_PASS_MIN:
                logger.info("[%s] skor %d < %d — skip",
                            symbol, score, CONFIG.THRESHOLDS.SCORE_PASS_MIN)
                return None

            return {
                "chain": pair["chain"],
                "symbol": symbol,
                "name": pair.get("name", ""),
                "token_address": addr,
                "pair_address": pair.get("pair_address", ""),
                "price_usd": pair["price_usd"],
                "lp_usd": pair["lp_usd"],
                "volume_24h_usd": pair["volume_24h_usd"],
                "price_change_m5": pair.get("price_change_m5", 0.0),
                "price_change_h1": pair.get("price_change_h1", 0.0),
                "url": pair.get("url", ""),
                "signals": detection.signals,
                "whale": detection.whale_details,
                "retail_fomo": detection.fomo_details,
                "security": {
                    "goplus_checked": rug_detail.get("checked", False),
                    "buy_tax_pct": rug_detail.get("buy_tax_pct"),
                    "sell_tax_pct": rug_detail.get("sell_tax_pct"),
                    "holder_count": rug_detail.get("holder_count"),
                },
                "score": score,
                "screened_at": _utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            }
    except Exception as exc:  # jaring pengaman terakhir — log & lanjut
        logger.error("Unexpected error screening %s (%s): %s",
                     symbol, addr[:12], exc, exc_info=CONFIG.SECRETS.DEBUG)
        return None


def _compute_score(pair: dict, det: DetectionResult) -> int:
    """Skor agregat 0-100 sederhana utk prioritas alert."""
    t = CONFIG.THRESHOLDS
    score = 40 if det.whale_detected else 0
    score += 40 if det.fomo_detected else 0
    # bonus likuiditas sehat
    if pair["lp_usd"] >= t.LP_MIN_USD * 4:
        score += 10
    if pair["volume_24h_usd"] >= pair["lp_usd"]:  # turnover > 1x LP/hari
        score += 10
    return min(score, 100)


# ---------------------------------------------------------------------------
# Entry point publik
# ---------------------------------------------------------------------------

async def screen_coins(chains: Optional[Iterable[str]] = None,
                       limit_per_chain: int = 12) -> list[dict]:
    """Jalankan satu siklus screening penuh utk semua chain yang diawasi.

    Args:
        chains: override daftar chain (default: CONFIG.BOT.CHAINS_WATCHED).
        limit_per_chain: maksimum kandidat yg dianalisis per chain.

    Returns:
        List dict coin lolos-filter + kena-threshold, terurut skor desc,
        siap diformat menjadi pesan Telegram. List kosong = tidak ada sinyal.
    """
    chains = list(chains or CONFIG.BOT.CHAINS_WATCHED)
    http = HttpFetcher()
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    results: list[dict] = []
    started = time.monotonic()

    try:
        for chain in chains:
            try:
                candidates = await fetch_trending_pairs(http, chain)
            except Exception as exc:
                logger.error("Fetch kandidat %s gagal total: %s", chain, exc)
                continue
            candidates = candidates[:limit_per_chain]
            logger.info("Chain %s: %d kandidat utk discreen", chain, len(candidates))

            tasks = [screen_pair(http, p, sem) for p in candidates]
            done = await asyncio.gather(*tasks)  # tiap task sudah try/except sendiri
            results.extend(r for r in done if r)
    finally:
        await http.close()

    results.sort(key=lambda c: c["score"], reverse=True)
    results = results[: CONFIG.BOT.MAX_RESULTS_PER_ALERT]
    logger.info("Siklus selesai dlm %.1fs — %d sinyal lolos",
                time.monotonic() - started, len(results))
    return results


async def run_screening_cycle() -> list[dict]:
    """Alias nyaman utk bot.py / loop periodik."""
    return await screen_coins()


# ---------------------------------------------------------------------------
# Self-test OFFLINE (mock) — `python screener.py` tanpa akses internet
# ---------------------------------------------------------------------------

def _mock_pair(**over) -> dict:
    base = {
        "chain": "bsc", "source": "mock", "dex_id": "pancakeswap",
        "pair_address": "0xPAIR", "base_token_address": "0xTOKEN",
        "symbol": "MOCK", "name": "Mock Coin", "price_usd": 0.001,
        "lp_usd": 50_000.0, "volume_24h_usd": 120_000.0, "txns_24h": 400,
        "price_change_m5": 8.0, "price_change_h1": 25.0,
        "created_at": None, "url": "https://dexscreener.com/bsc/0xPAIR",
    }
    base.update(over)
    return base


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    print("=" * 60)
    print("CCF Screener — offline logic test (mock data)")
    print("=" * 60)

    # Skenario 1: whale buy besar (> $300 dan > 1% LP) -> harus terdeteksi
    mock_txns_whale = [{
        "window": "h1", "window_seconds": 3600, "buys": 30, "sells": 10,
        "buy_volume_usd": 20_000, "sell_volume_usd": 4_000,
        "largest_single_buy_usd": 2_500,      # 5% dari LP $50k, > $300 ✔
    }]
    d1 = analyze_signals(_mock_pair(), mock_txns_whale)
    assert d1.whale_detected, "Whale harus terdeteksi"
    print("✔ Whale BUY terdeteksi:", d1.whale_details)

    # Skenario 2: retail FOMO (16 wallet, rasio 5x, +8%) -> harus terdeteksi
    mock_txns_fomo = [
        {"window": "m5", "window_seconds": 300, "buys": 16, "sells": 3,
         "buy_volume_usd": 5_000, "sell_volume_usd": 900,
         "largest_single_buy_usd": 120},
    ]
    d2 = analyze_signals(_mock_pair(price_change_m5=8.0), mock_txns_fomo)
    assert d2.fomo_detected, "Retail FOMO harus terdeteksi"
    print("✔ Retail FOMO terdeteksi:", d2.fomo_details)

    # Skenario 3: transaksi kecil sepi -> tidak ada sinyal
    d3 = analyze_signals(_mock_pair(price_change_m5=0.5), [
        {"window": "m5", "window_seconds": 300, "buys": 4, "sells": 4,
         "buy_volume_usd": 300, "sell_volume_usd": 280,
         "largest_single_buy_usd": 90}])
    assert not d3.signals, "Tidak boleh ada sinyal palsu"
    print("✔ Sepi -> tanpa sinyal (benar)")

    # Skenario 4: pre-filter menolak LP kecil
    ok, why = passes_prefilter(_mock_pair(lp_usd=3_000))
    assert not ok and "LP" in why
    print("✔ Pre-filter menolak LP $3.000:", why)

    print("-" * 60)
    print("Semua uji logika screener LULUS ✔")
    print("(Jalankan via bot.py utk screening live dengan API asli.)")
