import datetime as dt
import struct
import lzma
import requests
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from pathlib import Path
import os
from dateutil import tz
import logging
from logging.handlers import RotatingFileHandler
from config import load_config
import calendar

# Load config first
cfg = load_config()

# Create logs directory if needed
logs_dir = cfg["LOGS_PATH"]
os.makedirs(logs_dir, exist_ok=True)

# Log file path
log_file = os.path.join(logs_dir, "chart-downloader.log")

# === Get log level from config (with fallback) ===
log_level_str = cfg.get("CHART_DOWNLOADER_LOG_LEVEL", "INFO").upper()
valid_levels = {"DEBUG": logging.DEBUG, "INFO": logging.INFO,
                "WARNING": logging.WARNING, "ERROR": logging.ERROR,
                "CRITICAL": logging.CRITICAL}

log_level = valid_levels.get(log_level_str, logging.INFO)
if log_level_str not in valid_levels:
    print(f"Warning: Invalid log level '{log_level_str}' in config → falling back to INFO")

# === Setup dedicated logger ===
logger = logging.getLogger("chart_downloader")
logger.setLevel(log_level)                     # ← uses config value
logger.handlers.clear()                        # prevent duplicate handlers

# Rotating file handler
file_handler = RotatingFileHandler(
    log_file,
    maxBytes=cfg["CHART_DOWNLOADER_LOG_MAX_MB"] * 1024 * 1024,
    backupCount=cfg.get("CHART_DOWNLOADER_LOG_BACKUP_COUNT", 3),
    encoding="utf-8"
)

# Console handler (usually a bit less verbose)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)         # console stays INFO even if file is DEBUG
console_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
console_handler.setFormatter(console_formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

log = logger

# Optional: first log entry to confirm setup
log.info("=== Chart Downloader started ===")
log.info(f"Log file: {log_file}")
log.info(f"Max log size: {cfg['CHART_DOWNLOADER_LOG_MAX_MB']} MB")

URL_ROOT = 'https://datafeed.dukascopy.com/datafeed'
CACHE_ROOT = cfg["CHART_CACHE_PATH"]

CACHE_RESAMPLED_TIMEFRAMES = True  # Set True to cache 2h, etc.
# All supported user TFs
TIMEFRAME_MAPPING = {
    'tick': 'tick',
    '1s': '1s',
    '1m': '1m',
    '5m': '5m',
    '15m': '15m',
    '30m': '30m',
    '1h': '1h',
    '2h': '2h',
    '3h': '3h',
    '4h': '4h',
    '6h': '6h',
    '8h': '8h',
    '12h': '12h',
    '1d': '1d',
    '1M': '1M',
}

# Only tick is native download
NATIVE_TIMEFRAMES = {'tick'}

# Resample fallback from tick
RESAMPLE_FALLBACK = {
    '1s': ['tick'],
    '1m': ['tick'],
    '5m': ['tick'],
    '15m': ['tick'],
    '30m': ['tick'],
    '1h': ['tick'],
    '2h': ['tick'],
    '3h': ['tick'],
    '4h': ['tick'],
    '6h': ['tick'],
    '8h': ['tick'],
    '12h': ['tick'],
    '1d': ['tick'],
    '1M': ['tick'],
}

PRICE_TYPES = ['bid', 'ask']


def normalize_timeframe(tf: str) -> str:
    norm = TIMEFRAME_MAPPING.get(tf.lower())
    if not norm:
        raise ValueError(f"Invalid timeframe: {tf}")
    return norm


def get_freq(norm_tf: str):
    freq_map = {
        '1s': '1s',
        '1m': '1min',
        '5m': '5min',
        '15m': '15min',
        '30m': '30min',
        '1h': '1h',
        '2h': '2h',
        '3h': '3h',
        '4h': '4h',
        '6h': '6h',
        '8h': '8h',
        '12h': '12h',
        '1d': '1d',
        '1M': '1M',
        'tick': None,
    }
    return freq_map.get(norm_tf)


def get_months(start: dt.datetime, end: dt.datetime) -> list[tuple[int, int]]:
    months = []
    current = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while current < end:
        months.append((current.year, current.month))
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return months


# Cache helpers for resampled timeframes (per month)
def get_resampled_cache_path(symbol: str, tf: str, year: int, month: int) -> str:
    symbol = symbol.upper()
    return f"{CACHE_ROOT}/{symbol}/{tf}/data_{year}_{pad(month)}.parquet"


def load_from_cache(symbol: str, tf: str, start_date: dt.datetime, end_date: dt.datetime) -> pd.DataFrame | None:
    """Load all cached data that overlaps the requested period for resampled TFs."""
    months = get_months(start_date, end_date)
    dfs = []
    for year, month in months:
        path = get_resampled_cache_path(symbol, tf, year, month)
        if os.path.exists(path):
            # Parquet can filter directly → very fast
            df = pd.read_parquet(
                path,
                filters=[
                    ('timestamp', '>=', start_date),
                    ('timestamp', '<', end_date)
                ]
            )
            if not df.empty:
                dfs.append(df)
    if dfs:
        return pd.concat(dfs).sort_index()
    return None


def save_to_cache(df: pd.DataFrame, symbol: str, tf: str):
    """Split by month and write/append to Parquet files."""
    if df.empty:
        return
    df['year'] = df.index.year
    df['month'] = df.index.month
    for (year, month), group in df.groupby(['year', 'month']):
        path = get_resampled_cache_path(symbol, tf, year, month)
        Path(path).parent.mkdir(parents=True, exist_ok=True)

        group_clean = group.drop(columns=['year', 'month'])

        if os.path.exists(path):
            existing = pd.read_parquet(path)
            combined = pd.concat([existing, group_clean]).sort_index().drop_duplicates()
        else:
            combined = group_clean

        combined.to_parquet(path, compression='snappy')  # snappy is fast & good enough


# Cache helpers for ticks (per hour, unchanged)
def get_tick_cache_path(instrument: str, ts: dt.datetime) -> str:
    symbol = instrument.upper()
    y, m, d, h = ts.year, ts.month, ts.day, ts.hour
    return f"{CACHE_ROOT}/{symbol}/tick/{y}/{pad(m)}/{pad(d)}/{pad(h)}h_ticks.parquet"


def load_tick_hour(instrument: str, hour_dt: dt.datetime) -> pd.DataFrame | None:
    path = get_tick_cache_path(instrument, hour_dt)
    if os.path.exists(path):
        df = pd.read_parquet(path)
        if not df.empty:
            return df
    return None


def save_tick_hour(df: pd.DataFrame, instrument: str, hour_dt: dt.datetime):
    """Save tick data for one hour — even if empty or from 404."""
    path = get_tick_cache_path(instrument, hour_dt)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression='snappy')
    log.debug(f"Cached tick hour {hour_dt}: {len(df)} ticks → {path}")


def should_download_hour(instrument: str, hour_dt: dt.datetime) -> bool:
    """Return True only if cache file does NOT exist."""
    path = get_tick_cache_path(instrument, hour_dt)
    return not os.path.exists(path)


def pad(n):
    return f"{n:02d}"


def get_url(instrument, date, price_type):
    y, m, d, h = date.year, date.month, date.day, date.hour
    return f"{URL_ROOT}/{instrument.upper()}/{y}/{pad(m-1)}/{pad(d)}/{pad(h)}h_ticks.bi5"


def generate_urls(instrument, price_type, start_date, end_date):
    log.debug(f"generate_urls for {instrument} from {start_date} to {end_date}")
    urls = []
    current = start_date.replace(minute=0, second=0, microsecond=0)
    while current < end_date:
        urls.append(get_url(instrument, current, price_type))
        current += dt.timedelta(hours=1)
    log.debug(f"Generated {len(urls)} tick URLs")
    return urls


def download_file(url):
    try:
        proxies = {
            "http": cfg["HTTP_PROXY"],
            "https": cfg["HTTPS_PROXY"],
        }
        proxies = {}
        r = requests.get(url, timeout=60, proxies=proxies)
        r.raise_for_status()
        return url, r.content
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return url, None


def download_urls(urls, max_threads=10):
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        results = list(executor.map(download_file, urls))
    return {url: content for url, content in results if content is not None}


def parse_bi5(url, raw_data, price_type='bid', digits=5):
    log.debug(f"Parsing BI5 file: {url}, size={len(raw_data)} bytes")

    if len(raw_data) == 0:
        log.debug("Empty file (0 bytes) — returning empty DataFrame")
        return pd.DataFrame()  # empty → no ticks

    try:
        decompressed = lzma.decompress(raw_data)
    except lzma.LZMAError as e:
        log.warning(f"LZMA decompress failed for {url}: {e} — treating as empty")
        return pd.DataFrame()

    scale = 10 ** digits
    parts = url.split('/')
    year = int(parts[5])

    data = []

    if 'h_ticks.bi5' in url:
        fmt = '>IIIff'
        size = struct.calcsize(fmt)
        month = int(parts[6])
        day = int(parts[7])
        hour = int(parts[8].split('h')[0])
        base_time = dt.datetime(year, month + 1, day, hour, 0, 0, tzinfo=tz.UTC)
        for i in range(0, len(decompressed), size):
            delta_ms, ask_int, bid_int, ask_vol, bid_vol = struct.unpack(fmt, decompressed[i:i + size])
            ts = base_time + dt.timedelta(milliseconds=delta_ms)
            price = (bid_int if price_type == 'bid' else ask_int) / scale
            vol = bid_vol if price_type == 'bid' else ask_vol
            data.append({'timestamp': ts, 'open': price, 'high': price, 'low': price, 'close': price, 'volume': vol})
    else:
        fmt = '>IIIIIf'
        size = struct.calcsize(fmt)
        if 'candles_day_1.bi5' in url:
            base_time = dt.datetime(year, 1, 1, 0, 0, 0, tzinfo=tz.UTC)
        elif 'candles_hour_1.bi5' in url:
            month = int(parts[6])
            base_time = dt.datetime(year, month + 1, 1, 0, 0, 0, tzinfo=tz.UTC)
        elif 'candles_min_1.bi5' in url:
            month = int(parts[6])
            day = int(parts[7])
            base_time = dt.datetime(year, month + 1, day, 0, 0, 0, tzinfo=tz.UTC)
        else:
            return pd.DataFrame()

        for i in range(0, len(decompressed), size):
            delta_sec, o, h, l, c, vol = struct.unpack(fmt, decompressed[i:i + size])
            ts = base_time + dt.timedelta(seconds=delta_sec)
            data.append({'timestamp': ts,
                         'open': o / scale, 'high': h / scale,
                         'low': l / scale, 'close': c / scale,
                         'volume': vol})

    df = pd.DataFrame(data)
    if not df.empty:
        df = df.set_index('timestamp').sort_index()
    return df


def get_historical_data(
    instrument: str,
    start_date: dt.datetime,
    end_date: dt.datetime,
    timeframe: str,
    price_type: str = 'bid',
    digits: int = 5,
    max_threads: int = 10,
    use_cache_only: bool = False
) -> pd.DataFrame:
    norm_tf = normalize_timeframe(timeframe)
    instrument = instrument.upper()

    now = dt.datetime.now(tz.UTC)
    if end_date > now:
        end_date = now

    log.info(f"[{instrument} {timeframe}] Request: {start_date} → {end_date}")

    # For non-tick timeframes, try loading from resampled cache first
    if norm_tf != 'tick':
        cached = load_from_cache(instrument, norm_tf, start_date, end_date)
        if cached is not None:
            log.info(f"[{instrument} {norm_tf}] Loaded fully from resampled cache: {len(cached)} bars")
            return cached
        log.debug(f"[{instrument} {norm_tf}] No/full resampled cache — falling back to tick resampling")

    # Generate list of needed hours for ticks
    hours_needed = []
    current = start_date.replace(minute=0, second=0, microsecond=0)
    while current < end_date:
        hours_needed.append(current)
        current += dt.timedelta(hours=1)

    # Load from tick cache or download missing (if not use_cache_only)
    all_dfs = []
    to_download = []
    for hour_dt in hours_needed:
        cached = load_tick_hour(instrument, hour_dt)
        if cached is not None:
            all_dfs.append(cached)
        elif not use_cache_only:
            if should_download_hour(instrument, hour_dt):
                url = get_url(instrument, hour_dt, price_type)
                to_download.append((hour_dt, url))

    if to_download:
        log.info(f"[{instrument} {timeframe}] Downloading {len(to_download)} uncached hours")
        urls = [url for _, url in to_download]
        data_dict = download_urls(urls, max_threads)

        for hour_dt, url in to_download:
            raw = data_dict.get(url)
            if raw is not None:
                if len(raw) == 0:
                    log.debug(f"Empty response (0 bytes) for {url} — caching empty")
                    df_hour = pd.DataFrame()
                else:
                    df_hour = parse_bi5(url, raw, price_type, digits)
            else:
                # 404 or network error
                log.warning(f"404 or failed: {url} — caching empty to avoid retry")
                df_hour = pd.DataFrame()

            all_dfs.append(df_hour)
            save_tick_hour(df_hour, instrument, hour_dt)  # always save!

    if not all_dfs:
        return pd.DataFrame()

    df = pd.concat(all_dfs).sort_index()

    # Resample
    freq = get_freq(norm_tf)
    if freq is None:  # tick
        resampled = df
    else:
        resampled = df.resample(freq).agg({
            'open': 'first', 'high': 'max', 'low': 'min',
            'close': 'last', 'volume': 'sum'
        }).dropna(how='all')

    log.info(f"[{instrument} {timeframe}] {len(df)} ticks → {len(resampled)} {norm_tf} candles")

    # Optional: cache resampled
    if CACHE_RESAMPLED_TIMEFRAMES and timeframe.lower() in ["15m","1h","6h"]:
        save_to_cache(resampled, instrument, timeframe)

    return resampled.loc[start_date:end_date]