"""
CCF (Coin Cash Flow) - Modul Database (SQLite lokal)
====================================================

Mengelola database SQLite ``ccf_users.db`` untuk data pelanggan/bot.

Skema tabel ``subscribers``:
    - telegram_id  INTEGER PRIMARY KEY   -- ID unik user Telegram
    - username     TEXT                  -- @username (boleh kosong)
    - status       TEXT DEFAULT 'inactive'  -- 'active' | 'inactive'
    - expiry_date  DATETIME              -- akhir langganan (UTC, ISO-8601)

Desain koneksi:
    Setiap fungsi membuka koneksi *short-lived* lewat context-manager
    ``get_connection()`` yang otomatis melakukan COMMIT/ROLLBACK dan CLOSE.
    Ini menghindari error "database is locked" pada HF Spaces (CPU Basic),
    karena tidak ada koneksi panjang yang menahan lock — cocok dipakai
    dari loop async python-telegram-bot maupun thread Gradio sekaligus.

Inisialisasi otomatis:
    Saat modul ini di-import, ``init_db()`` langsung dijalankan sehingga
    folder ``data/`` dan tabel ``subscribers`` selalu siap pakai.

Cara pakai::

    from database import (
        add_or_update_user,
        is_user_active,
        get_all_active_users,
    )

    add_or_update_user(123456789, "@alice", status="active", days=30)

    if is_user_active(123456789):
        ...

    targets = get_all_active_users()   # utk broadcast alert
"""

from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Iterator, List, Optional

try:  # pakai path dari config.py bila tersedia
    from config import STORAGE
    DB_PATH: str = STORAGE.db_path
except Exception:  # pragma: no cover - fallback agar modul tetap mandiri
    DB_PATH = os.path.join(os.getcwd(), "data", "ccf_users.db")

logger = logging.getLogger("ccf.database")

# Format waktu ISO-8601 tanpa mikrodetik, selalu UTC
_DT_FORMAT = "%Y-%m-%d %H:%M:%S"

STATUS_ACTIVE = "active"
STATUS_INACTIVE = "inactive"

# Penanda bahwa folder data hanya READ-ONLY (mis. HF Spaces tanpa persistent
# storage). Bila True, seluruh operasi tulis menjadi no-op yang aman sehingga
# aplikasi tetap hidup; pembacaan memakai salinan in-memory (:memory:).
DB_READONLY_MODE: bool = False

_SCHEMA = """
CREATE TABLE IF NOT EXISTS subscribers (
    telegram_id INTEGER PRIMARY KEY,
    username    TEXT,
    status      TEXT NOT NULL DEFAULT 'inactive',
    expiry_date DATETIME
);
CREATE INDEX IF NOT EXISTS idx_subscribers_status
    ON subscribers (status, expiry_date);
"""


# ---------------------------------------------------------------------------
# Manajemen koneksi
# ---------------------------------------------------------------------------

def _can_write(path: str) -> bool:
    """Cek apakah file/direktori target dapat ditulisi (tanpa melempar error)."""
    try:
        d = os.path.dirname(path) or "."
        if not os.path.isdir(d):
            os.makedirs(d, exist_ok=True)
        probe = os.path.join(d, ".ccf_write_test")
        with open(probe, "w") as fh:
            fh.write("ok")
        os.remove(probe)
        return True
    except OSError:
        return False


@contextmanager
def get_connection(db_path: str = DB_PATH) -> Iterator[sqlite3.Connection]:
    """
    Context manager koneksi SQLite yang aman terhadap 'database is locked'.

    - row_factory = sqlite3.Row  -> hasil query bisa diakses by nama kolom
    - timeout     = 10 detik     -> retry internal sebelum melempar error
    - WAL mode                   -> pembaca & penulis bersamaan lebih stabil
      (digrup-off otomatis bila filesystem read-only / tidak mendukung)
    - commit otomatis saat sukses, rollback saat exception, selalu ditutup.
    - Fallback :memory: bila disk read-only (lihat DB_READONLY_MODE).
    """
    global DB_READONLY_MODE

    if DB_READONLY_MODE:
        # Mode read-only: pakai database in-memory sementara agar query
        # SELECT tetap berfungsi (hasilnya kosong — bukan crash).
        conn = sqlite3.connect(":memory:", timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            conn.executescript(_SCHEMA)
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return

    try:
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
    except sqlite3.OperationalError as exc:
        # Disk read-only? Aktifkan mode tangguh sekali saja, lalu recursive
        # ke cabang :memory: di atas.
        if "readonly" in str(exc).lower() or "unable to open" in str(exc).lower():
            DB_READONLY_MODE = True
            logger.warning(
                "Filesystem read-only terdeteksi (%s). Beralih ke mode "
                "in-memory: subscriber TIDAK akan persisten antar-restart. "
                "Untuk produksi, aktifkan Persistent Storage di HF Spaces.",
                exc,
            )
            with get_connection(db_path) as conn:
                yield conn
            return
        raise

    try:
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
        except sqlite3.Error:
            # Beberapa filesystem (NFS/overlayfs tertentu) menolak WAL —
            # lanjut dengan mode default DELETE, tetap aman.
            pass
        conn.execute("PRAGMA busy_timeout=10000;")
        yield conn
        conn.commit()
    except sqlite3.OperationalError as exc:
        msg = str(exc).lower()
        if "readonly" in msg or "disk i/o error" in msg:
            DB_READONLY_MODE = True
            logger.warning(
                "Tulis ke SQLite gagal (read-only?): %s — mode in-memory "
                "diaktifkan agar bot tetap hidup.", exc)
            conn.rollback()
            conn.close()
            with get_connection(db_path) as mem_conn:
                yield mem_conn
            return
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: str = DB_PATH) -> None:
    """Buat folder data & tabel subscribers bila belum ada.

    Tidak pernah melempar exception pada lingkungan read-only — cukup log.
    """
    if not _can_write(db_path):
        global DB_READONLY_MODE
        DB_READONLY_MODE = True
        logger.warning(
            "Path %s tidak dapat ditulis (filesystem read-only?). CCF berjalan "
            "dalam MODE SEMENTARA: data subscriber hanya di RAM dan hilang "
            "saat Space restart. Solusi produksi: aktifkan 'Persistent "
            "Storage' pada Settings HF Spaces, atau set env DATA_DIR ke "
            "/persistent/data.", db_path)
        with get_connection(db_path) as conn:  # cabang :memory:
            conn.executescript(_SCHEMA)
        return
    with get_connection(db_path) as conn:
        conn.executescript(_SCHEMA)
    logger.info("Database siap: %s", db_path)


# ---------------------------------------------------------------------------
# Helper waktu (semua disimpan dalam UTC, format string agar konsisten)
# ---------------------------------------------------------------------------

def _utcnow_str() -> str:
    return datetime.now(timezone.utc).strftime(_DT_FORMAT)


def _to_utc_str(value: Optional[object], *, days_from_now: Optional[float] = None) -> Optional[str]:
    """
    Normalisasi berbagai input expiry menjadi string UTC 'YYYY-MM-DD HH:MM:SS'.

    Menerima: None, string ISO/spasi, datetime (aware/naive), atau
    ``days_from_now`` (float jumlah hari ke depan).
    """
    if days_from_now is not None:
        dt = datetime.now(timezone.utc) + timedelta(days=float(days_from_now))
        return dt.strftime(_DT_FORMAT)

    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value).strip().replace("T", " ")
        if raw.endswith("Z"):
            raw = raw[:-1]
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            raise ValueError(f"Format expiry_date tidak dikenali: {value!r}")

    if dt.tzinfo is None:  # anggap naive sebagai UTC
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime(_DT_FORMAT)


# ---------------------------------------------------------------------------
# CRUD subscribers
# ---------------------------------------------------------------------------

def add_or_update_user(
    telegram_id: int,
    username: Optional[str] = None,
    status: str = STATUS_INACTIVE,
    expiry_date: Optional[object] = None,
    days: Optional[float] = None,
    db_path: str = DB_PATH,
) -> None:
    """
    Tambah user baru atau update yang sudah ada (UPSERT by telegram_id).

    Args:
        telegram_id : ID Telegram (wajib, primary key).
        username    : @username, boleh None/kosong.
        status      : 'active' atau 'inactive'.
        expiry_date : eksplisit (str ISO / datetime), atau...
        days        : ...jumlah hari langganan dari sekarang (mis. 30).

    Jika ``days`` diberikan, expiry dihitung otomatis dari waktu sekarang.
    """
    if status not in (STATUS_ACTIVE, STATUS_INACTIVE):
        raise ValueError(f"status harus '{STATUS_ACTIVE}' atau '{STATUS_INACTIVE}'")

    exp = _to_utc_str(expiry_date, days_from_now=days)

    sql = """
        INSERT INTO subscribers (telegram_id, username, status, expiry_date)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(telegram_id) DO UPDATE SET
            username    = excluded.username,
            status      = excluded.status,
            expiry_date = excluded.expiry_date;
    """
    with get_connection(db_path) as conn:
        conn.execute(sql, (int(telegram_id), username, status, exp))


def is_user_active(telegram_id: int, db_path: str = DB_PATH) -> bool:
    """
    True jika user ada, status='active', DAN expiry_date > waktu sekarang (UTC).
    """
    sql = """
        SELECT 1 FROM subscribers
        WHERE telegram_id = ?
          AND status = ?
          AND expiry_date IS NOT NULL
          AND expiry_date > ?
        LIMIT 1;
    """
    with get_connection(db_path) as conn:
        row = conn.execute(
            sql, (int(telegram_id), STATUS_ACTIVE, _utcnow_str())
        ).fetchone()
    return row is not None


def get_all_active_users(db_path: str = DB_PATH) -> List[sqlite3.Row]:
    """
    Ambil semua subscriber aktif (untuk broadcast alert).

    Returns:
        List[sqlite3.Row] dengan kolom: telegram_id, username, status, expiry_date.
    """
    sql = """
        SELECT telegram_id, username, status, expiry_date
        FROM subscribers
        WHERE status = ?
          AND expiry_date IS NOT NULL
          AND expiry_date > ?
        ORDER BY expiry_date DESC;
    """
    with get_connection(db_path) as conn:
        return conn.execute(
            sql, (STATUS_ACTIVE, _utcnow_str())
        ).fetchall()


def deactivate_expired_users(db_path: str = DB_PATH) -> int:
    """
    Set status='inactive' untuk user yang expiry_date-nya sudah lewat.
    Cocok dijadwalkan harian. Returns jumlah baris yang diubah.
    """
    sql = """
        UPDATE subscribers
        SET status = ?
        WHERE status = ?
          AND (expiry_date IS NULL OR expiry_date <= ?);
    """
    with get_connection(db_path) as conn:
        cur = conn.execute(sql, (STATUS_INACTIVE, STATUS_ACTIVE, _utcnow_str()))
        return cur.rowcount


def remove_user(telegram_id: int, db_path: str = DB_PATH) -> bool:
    """Hapus subscriber. Returns True bila ada baris yang terhapus."""
    with get_connection(db_path) as conn:
        cur = conn.execute(
            "DELETE FROM subscribers WHERE telegram_id = ?", (int(telegram_id),)
        )
        return cur.rowcount > 0


def get_user(telegram_id: int, db_path: str = DB_PATH) -> Optional[sqlite3.Row]:
    """Ambil satu record subscriber (atau None bila tidak ditemukan)."""
    with get_connection(db_path) as conn:
        return conn.execute(
            "SELECT * FROM subscribers WHERE telegram_id = ?", (int(telegram_id),)
        ).fetchone()


# ---------------------------------------------------------------------------
# Inisialisasi otomatis saat modul di-import
# ---------------------------------------------------------------------------
init_db()


# ---------------------------------------------------------------------------
# Self-test ringan:  python database.py
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    TEST_ID = 999_999_999

    add_or_update_user(TEST_ID, "@tester", status="active", days=30)
    assert is_user_active(TEST_ID), "user seharusnya aktif"

    rows = get_all_active_users()
    print(f"Aktif: {[dict(r)['telegram_id'] for r in rows]}")

    add_or_update_user(TEST_ID, "@tester", status="active", days=-1)  # expired
    assert not is_user_active(TEST_ID), "user kadaluarsa harus inactive-check"
    changed = deactivate_expired_users()
    print(f"Dinonaktifkan otomatis: {changed}")

    assert remove_user(TEST_ID)
    print("Self-test database.py OK ✔")
