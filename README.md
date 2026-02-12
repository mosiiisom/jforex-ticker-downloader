# JForex Ticker Downloader

Lightweight automated downloader that periodically fetches **tick-level historical data** from Dukascopy's public JForex datafeed and stores it in Parquet format.

The project keeps the most recent days (configurable) continuously updated — ideal for backtesting, analysis, or feeding live-updating trading systems.

## Features

- Downloads **tick data** (bid & ask) from Dukascopy public feed
- Supports multiple symbols (forex majors, metals, …)
- Incremental / smart updates — only downloads missing hours
- Stores data in **hourly Parquet files** → efficient compression & fast queries
- Keeps configurable number of recent days fresh (default: 7)
- Infinite loop with intelligent sleep (next full hour + safety buffer)
- Centralized logging → single rotating `logs.log` file
- Docker-ready with volume mounts for config / data / logs

## Project structure

```
jforex-ticker-downloader/
├── app.py                  # Main loop: download → sleep → repeat
├── downloader.py           # Dukascopy fetching, caching, parsing logic
├── config.py               # Loads & merges config.json
├── logs.py                 # Centralized logging setup
├── config.json             # Symbols, days, paths, log levels, …
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── data/                   # ← generated .parquet files (mounted)
└── logs/                   # ← logs.log + report.json (mounted)
```
## Requirements

```text
pandas>=2.0
python-dateutil
requests
```

```Bash
pip install -r requirements.txt
```

### Configuration (config.json)

```JSON
{
  "CHART_CACHE_PATH": "data",
  "LOGS_PATH": "logs",
  "HTTP_PROXY": "",
  "HTTPS_PROXY": "",
  "LOG_LEVEL": "WARNING",
  "CONSOLE_LOG_LEVEL": "WARNING",

  "SYMBOLS_TO_STORE": [
    "EURUSD", "GBPUSD", "XAUUSD", "USDJPY",
    "XAGUSD", "AUDUSD", "USDCHF", "USDCAD"
  ],

  "RECENT_DAYS_TO_DOWNLOAD": 7
}
```

### Running locally
```Bash
python app.py
```

### Behavior:

Fetches missing hours for the last N days
Updates report.json with last sync timestamp per symbol
Sleeps until next full hour + ~2-minute buffer
Repeats forever

### Docker deployment
```Bash
# Build image
docker compose build --no-cache

# Start container in background
docker compose up -d

# Follow logs in real-time
docker compose logs -f

# Stop & remove container
docker compose down
```

## Mounted volumes

./config.json → read-only inside container
./data/ → all generated Parquet files
./logs/ → logs.log + report.json

### Important notes

All timestamps & data are in UTC
Weekends → usually 0 ticks (expected)
Current (ongoing) hour → may be partial or return 404 (gracefully handled)
Sunday sessions → often very thin (low tick count)
Storage usage: ~10–60 MB per symbol per active day (highly compressible)

## License
### MIT
Feel free to fork, modify, use in commercial projects.