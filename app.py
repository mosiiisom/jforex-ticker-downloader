# app.py
import datetime as dt
import json
import logging
import logging.handlers
import time
from pathlib import Path
from dateutil import tz

from config import load_config
from downloader import get_historical_data
from logs import log

cfg = load_config()
LOGS_DIR = Path(cfg["LOGS_PATH"])

# ────────────────────────────────────────────────────────────────
# Application logic
# ────────────────────────────────────────────────────────────────

SYMBOLS = cfg["SYMBOLS_TO_STORE"]
DAYS_TO_SYNC = cfg.get("RECENT_DAYS_TO_DOWNLOAD", 7)
TIMEFRAME = "tick"

DATA_DIR = Path(cfg["CHART_CACHE_PATH"])
REPORT_FILE = LOGS_DIR / "report.json"


def ensure_directories():
    """Create data and logs directories if they don't exist"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


def load_or_init_report() -> dict:
    """Load report.json or initialize a new one if missing / invalid"""
    if not REPORT_FILE.exists():
        log.info("report.json not found → creating new one")
        return {sym: None for sym in SYMBOLS}

    try:
        with REPORT_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            raise ValueError("report.json root is not a dictionary")

        cleaned = {}
        for sym in SYMBOLS:
            if sym in data and isinstance(data[sym], str):
                cleaned[sym] = data[sym]
            else:
                cleaned[sym] = None

        if cleaned != data:
            log.info("report.json had missing or invalid entries → cleaned up")

        return cleaned

    except Exception as e:
        log.warning(f"Invalid or corrupted report.json → recreating ({e})")
        return {sym: None for sym in SYMBOLS}


def save_report(report: dict):
    """Atomic write of report.json"""
    tmp_path = REPORT_FILE.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, sort_keys=True)
    tmp_path.replace(REPORT_FILE)
    log.debug("report.json updated")


def download_one_day(symbol: str, target_date: dt.date) -> int:
    """Download tick data for one calendar day and return count of ticks"""
    start = dt.datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=tz.UTC)
    end = start + dt.timedelta(days=1)

    df = get_historical_data(
        instrument=symbol,
        start_date=start,
        end_date=end,
        timeframe=TIMEFRAME
    )

    return len(df)


def seconds_to_next_hour_plus_buffer(buffer_minutes: int = 2) -> int:
    """Seconds until next full hour + safety buffer"""
    now = dt.datetime.now(tz.UTC)
    next_hour = (now + dt.timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    target = next_hour + dt.timedelta(minutes=buffer_minutes)
    seconds = (target - now).total_seconds()
    return max(60, int(seconds))


def log_sync_start():
    now_str = dt.datetime.now(tz.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    log.info("═" * 70)
    log.info(f"Sync cycle started at {now_str}")
    log.info(f"Symbols   : {', '.join(SYMBOLS)}")
    log.info(f"Days      : last {DAYS_TO_SYNC}")
    log.info(f"Timeframe : {TIMEFRAME}")
    log.info("═" * 70)


def main_loop():
    ensure_directories()
    report = load_or_init_report()

    while True:
        log_sync_start()

        today = dt.datetime.now(tz.UTC).date()
        updated = False

        for symbol in SYMBOLS:
            log.info(f"Processing {symbol}")

            total_ticks = 0
            days_with_data = 0

            for offset in range(DAYS_TO_SYNC):
                target_date = today - dt.timedelta(days=offset)
                day_str = target_date.strftime("%Y-%m-%d (%A)")

                count = download_one_day(symbol, target_date)

                if count > 0:
                    days_with_data += 1
                    total_ticks += count
                    log.info(f"  {day_str: <18} → {count:>9,} ticks")
                else:
                    log.info(f"  {day_str: <18} → no data (weekend/holiday)")

            log.info(f"  → Total: {total_ticks:,} ticks over {days_with_data} active days")

            # Record last attempt timestamp
            now_iso = dt.datetime.now(tz.UTC).isoformat(timespec="seconds")
            report[symbol] = now_iso
            updated = True

        if updated:
            save_report(report)
            log.info("report.json updated with latest timestamps")

        # Calculate sleep time to next desired run
        sleep_sec = seconds_to_next_hour_plus_buffer(buffer_minutes=2)
        next_run = dt.datetime.now(tz.UTC) + dt.timedelta(seconds=sleep_sec)
        next_run_str = next_run.strftime('%Y-%m-%d %H:%M UTC')

        log.info(f"Next sync scheduled ≈ {next_run_str}")
        log.info(f"Sleeping for {sleep_sec // 60} minutes ({sleep_sec} seconds)...")
        time.sleep(sleep_sec)


if __name__ == "__main__":
    log.info("Downloader application started")
    try:
        main_loop()
    except KeyboardInterrupt:
        log.info("Stopped by user (Ctrl+C)")
    except Exception as e:
        log.exception(f"Unexpected error during execution: {e}")
        raise
    finally:
        log.info("Downloader application stopped")