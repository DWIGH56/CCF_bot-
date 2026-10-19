"""
CCF (Coin Cash Flow) - Subscriber Store berbasis JSON
=====================================================

Pengganti SQLite untuk deployment **GitHub Actions Scheduled Cron Job**.

Kenapa JSON?
------------
Runner GitHub Actions adalah environment *sementara* — setiap run dimulai
dari checkout repo yang bersih dan semua file di luar workspace hilang saat
job selesai. Solusi persistensi paling simpel & andal: simpan data subscriber
di file ``subscribers.json`` DI ROOT REPO, lalu **commit + push balik** ke
repository di akhir setiap run (dengan write token). Data pun awet antar-run
tanpa server database apa pun.

Skema data (selaras dengan tabel `subscribers` versi SQLite):
    {
      "users": {
        "123456789": {
          "telegram_id": 123456789,
          "username":    "alice",
          "status":      "active",            -- 'active' | 'inactive'
          "expiry_date": "2026-11-14 12:00:00" -- string UTC, atau null
        }, ...
      }
    }

API publik (drop-in setara fungsi database.py):
    add_or_update_user(...)   is_user_active(uid)   get_all_active_users()
    deactivate_expired_users()  get_user(uid)       remove_user(uid)

Catatan async: operasi file di sini tiny (<1 ms) sehingga aman dipanggil
langsung dari event loop; bila ingin super-rapi gunakan via
``asyncio.to_thread`` (lihat main.py).
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger("ccf.subscribers_db")

# ---------------------------------------------------------------------------
# Lokasi file — default root repo (agar mudah di-commit oleh git actions),
# override via env var SUBSCRIBERS_FILE bila perlu.
# ---------------------------------------------------------------------------
SUBSCRIBERS_FILE: str = os.environ.get(
    "SUBSCRIBERS_FILE", os.path.join(os.getcwd(), "subscribers.json"))

STATUS_ACTIVE = "active"
STATUS_INACTIVE = "inactive"

_DT_FORMAT = "%Y-%m-%d %H:%M:%S"  # selalu UTC, tanpa mikrodetik


def _utcnow_str() -> str:
    return datetime.now(timezone.utc).strftime(_DT_FORMAT)


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, _DT_FORMAT).replace(tzinfo=timezone.utc)
    except ValueError:
        # toleransi format lain (mis. diedit manual)
        try:
            dt = datetime.fromisoformat(str(value))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            return None


# ---------------------------------------------------------------------------
# Load / Save atomik
# ---------------------------------------------------------------------------
def load_data() -> dict:
    """Baca subscribers.json; kembalikan struktur kosong bila belum ada/rusak."""
    if not os.path.exists(SUBSCRIBERS_FILE):
        return {"users": {}}
    try:
        with open(SUBSCRIBERS_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict) or not isinstance(data.get("users"), dict):
            logger.warning("Format %s tidak valid — mulai dari kosong.",
                           SUBSCRIBERS_FILE)
            return {"users": {}}
        return data
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Gagal membaca %s (%s) — mulai dari kosong.",
                     SUBSCRIBERS_FILE, exc)
        return {"users": {}}


def save_data(data: dict) -> None:
    """Tulis atomik (tmp file + os.replace) agar tidak korup bila job di-kill
    di tengah penulisan — penting di runner CI yang bisa timeout."""
    directory = os.path.dirname(SUBSCRIBERS_FILE) or "."
    fd, tmp_path = tempfile.mkstemp(prefix=".subscribers_", suffix=".tmp",
                                    dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, ensure_ascii=False, sort_keys=True)
            fh.write("\n")
        os.replace(tmp_path, SUBSCRIBERS_FILE)
    except Exception:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# CRUD API (setara database.py)
# ---------------------------------------------------------------------------
def get_user(telegram_id: int) -> Optional[dict]:
    return load_data()["users"].get(str(telegram_id))


def add_or_update_user(telegram_id: int,
                       username: Optional[str] = None,
                       status: str = STATUS_INACTIVE,
                       expiry_date: Optional[datetime] = None,
                       days: Optional[float] = None) -> dict:
    """Tambah baru atau update user yang sudah ada (UPSERT).

    Args:
        days: praktis — expiry dihitung dari sekarang + N hari.
        expiry_date: datetime eksplisit (menang atas `days`).
    """
    if status not in (STATUS_ACTIVE, STATUS_INACTIVE):
        raise ValueError(f"status harus '{STATUS_ACTIVE}'/'{STATUS_INACTIVE}'")

    if expiry_date is None and days is not None:
        expiry_date = datetime.now(timezone.utc) + timedelta(days=days)

    expiry_str = (expiry_date.astimezone(timezone.utc).strftime(_DT_FORMAT)
                  if expiry_date else None)

    data = load_data()
    uid = str(int(telegram_id))
    record = data["users"].get(uid, {"telegram_id": int(telegram_id)})
    record.update({
        "telegram_id": int(telegram_id),
        "username": username if username is not None else record.get("username"),
        "status": status,
        "expiry_date": expiry_str,
    })
    data["users"][uid] = record
    save_data(data)
    return record


def is_user_active(telegram_id: int) -> bool:
    """True hanya bila status='active' DAN expiry_date > waktu sekarang."""
    rec = get_user(telegram_id)
    if not rec or rec.get("status") != STATUS_ACTIVE:
        return False
    exp = _parse_dt(rec.get("expiry_date"))
    if exp is None:
        return False  # active tanpa expiry dianggap tidak valid (konsisten DB)
    return exp > datetime.now(timezone.utc)


def get_all_active_users() -> list[dict]:
    """Semua subscriber aktif (untuk broadcast alert), urut telegram_id."""
    now = datetime.now(timezone.utc)
    out = []
    for rec in load_data()["users"].values():
        exp = _parse_dt(rec.get("expiry_date"))
        if rec.get("status") == STATUS_ACTIVE and exp and exp > now:
            out.append(rec)
    out.sort(key=lambda r: r["telegram_id"])
    return out


def deactivate_expired_users() -> int:
    """Set status='inactive' utk user aktif yang expiry-nya sudah lewat."""
    now = datetime.now(timezone.utc)
    data = load_data()
    changed = 0
    for rec in data["users"].values():
        exp = _parse_dt(rec.get("expiry_date"))
        if rec.get("status") == STATUS_ACTIVE and (exp is None or exp <= now):
            rec["status"] = STATUS_INACTIVE
            changed += 1
    if changed:
        save_data(data)
    return changed


def remove_user(telegram_id: int) -> bool:
    data = load_data()
    if str(telegram_id) in data["users"]:
        del data["users"][str(telegram_id)]
        save_data(data)
        return True
    return False


# ---------------------------------------------------------------------------
# Self-test singkat: python subscribers_db.py
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import atexit

    backup = None
    if os.path.exists(SUBSCRIBERS_FILE):
        with open(SUBSCRIBERS_FILE, "rb") as fh:
            backup = fh.read()

    def restore() -> None:
        if backup is not None:
            with open(SUBSCRIBERS_FILE, "wb") as fh:
                fh.write(backup)
        elif os.path.exists(SUBSCRIBERS_FILE):
            os.remove(SUBSCRIBERS_FILE)

    atexit.register(restore)

    TEST_ID = 999_999_999
    add_or_update_user(TEST_ID, "tester", STATUS_ACTIVE, days=30)
    assert is_user_active(TEST_ID), "harusnya aktif"
    assert any(u["telegram_id"] == TEST_ID for u in get_all_active_users())
    add_or_update_user(TEST_ID, "tester", STATUS_ACTIVE, days=-1)  # expired
    assert not is_user_active(TEST_ID), "expired harus inactive-check False"
    assert deactivate_expired_users() >= 1
    assert remove_user(TEST_ID)
    print("Self-test subscribers_db.py OK ✔")
