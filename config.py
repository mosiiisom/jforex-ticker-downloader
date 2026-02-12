import os
import json
import sys

DEFAULT_CONFIG = {
    "CHART_CACHE_PATH": "data",
    "LOGS_PATH": "logs",
    "HTTP_PROXY": "",
    "HTTPS_PROXY": "",
    "CHART_DOWNLOADER_LOG_MAX_MB": 10,
    "CHART_DOWNLOADER_LOG_BACKUP_COUNT": 5,
    "CHART_DOWNLOADER_LOG_LEVEL": "INFO",

    "SYMBOLS_TO_STORE": ["EURUSD", "GBPUSD", "XAUUSD", "USDJPY"],

    "RECENT_DAYS_TO_DOWNLOAD": 7      # ← added
}


def get_exe_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def load_config():
    exe_dir = get_exe_dir()
    cfg_path = os.path.join(exe_dir, "config.json")

    if not os.path.exists(cfg_path):
        print("config.json not found → creating with defaults")
        try:
            default = {**DEFAULT_CONFIG}
            default['CHART_CACHE_PATH'] = os.path.join(exe_dir, default['CHART_CACHE_PATH'])
            default['LOGS_PATH'] = os.path.join(exe_dir, default['LOGS_PATH'])
            with open(cfg_path, "w", encoding="utf-8") as f:
                json.dump(default, f, indent=4)
        except Exception as e:
            print("Failed to create default config.json:", e)
        return DEFAULT_CONFIG

    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            user_cfg = json.load(f)
            cfg = {**DEFAULT_CONFIG, **user_cfg}
            # Make paths absolute (important!)
            cfg['CHART_CACHE_PATH'] = os.path.join(exe_dir, cfg['CHART_CACHE_PATH'])
            cfg['LOGS_PATH'] = os.path.join(exe_dir, cfg['LOGS_PATH'])
            return cfg
    except Exception as e:
        print("Failed to load config.json:", e)
        return DEFAULT_CONFIG