# update-recent.py
import datetime as dt
from dateutil import tz
from downloader import get_historical_data
from config import load_config

cfg = load_config()

SYMBOLS = cfg["SYMBOLS_TO_STORE"]
DAYS = cfg.get("RECENT_DAYS_TO_DOWNLOAD", 7)
TIMEFRAME = "tick"


def download_one_day(symbol: str, target_date: dt.date) -> int:
    """Download ticks for one calendar day → returns number of ticks"""
    start = dt.datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=tz.UTC)
    end   = start + dt.timedelta(days=1)

    # Because downloader already caches per hour → safe & fast to call repeatedly
    df = get_historical_data(
        instrument=symbol,
        start_date=start,
        end_date=end,
        timeframe=TIMEFRAME
    )

    count = len(df)
    return count


if __name__ == "__main__":
    now_utc = dt.datetime.now(tz=tz.UTC)
    today = now_utc.date()

    print("Recent data update started")
    print(f"Symbols   : {', '.join(SYMBOLS)}")
    print(f"Days      : last {DAYS} days (up to {today})")
    print(f"Timeframe : {TIMEFRAME}")
    print("-" * 50)

    for symbol in SYMBOLS:
        print(f"\n{symbol}")

        total_ticks = 0
        days_with_data = 0

        for offset in range(DAYS):
            target_date = today - dt.timedelta(days=offset)
            day_str = target_date.strftime("%Y-%m-%d (%A)")

            count = download_one_day(symbol, target_date)

            if count > 0:
                days_with_data += 1
                total_ticks += count
                print(f"  {day_str: <18} → {count:>8,} ticks")
            else:
                print(f"  {day_str: <18} → no data (weekend/holiday)")

        print(f"  → Total for {symbol}: {total_ticks:,} ticks over {days_with_data} active days")

    print("\nUpdate finished.")