# Crypto Hornet — Exchange Listings Watcher

Crypto Hornet now focuses on **direct exchange APIs** instead of Telegram scraping. The bot polls the official endpoints for Binance, OKX, Gate.io, Bitget, MEXC, BingX and Bybit (spot and futures where available) and instantly posts every newly listed USDT pair to your Telegram channel.

## Features

- Concurrent polling of multiple exchanges with configurable interval.
- Comparison between the last known snapshot and the current API response to detect fresh listings.
- Deduplicated Telegram notifications with timestamps and trading links.
- JSON state file to persist snapshots and avoid reposts between restarts.
- Optional HTTP/HTTPS proxy support and BingX API key header.

## Quick start

1. Install Python 3.11 or newer.
2. Copy `.env.example` to `.env` and fill at least `BOT_TOKEN` and `TARGET_CHAT_ID`.
3. Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

4. Run the watcher:

```bash
python main.py
```

The first run seeds the snapshots (no messages are sent). Subsequent runs will post every new pair detected by the exchanges.

## Configuration

All settings are read from environment variables (see `.env.example`). Useful options:

- `POLL_INTERVAL_SEC` — delay between requests for every feed (defaults to 60s).
- `API_TIMEOUT_SEC` — HTTP timeout in seconds.
- `ONLY_USDT` — keep only USDT quoted pairs (set `0` to disable the filter).
- `SEED_ON_START` — when `1`, the first snapshot is stored without notifications.
- `HTTP_PROXY` / `HTTPS_PROXY` — optional proxies.
- `BINGX_API_KEY` — adds the header required for some BingX endpoints.
- `STATE_FILE` — path to the persistence JSON file (default `state.json`).

## Project structure

```
crypto_hornet/
├── config.py          # Pydantic Settings wrapper
├── exchanges/         # Exchange-specific API clients
├── runner.py          # Async orchestrator and feed loop
├── state.py           # Snapshot persistence
├── telegram.py        # Telegram Bot API wrapper
└── templates.py       # Message formatting helpers
main.py                # Entry point
requirements.txt       # Dependencies
```

## Supported exchanges & endpoints

| Exchange | Market  | Endpoint |
|----------|---------|----------|
| Binance  | Spot    | `https://api.binance.com/api/v3/exchangeInfo` |
| Binance  | Futures | `https://fapi.binance.com/fapi/v1/exchangeInfo` |
| OKX      | Spot    | `https://www.okx.com/api/v5/public/instruments?instType=SPOT` |
| OKX      | Futures | `https://www.okx.com/api/v5/public/instruments?instType=SWAP` |
| Gate.io  | Spot    | `https://api.gateio.ws/api/v4/spot/currency_pairs` |
| Gate.io  | Futures | `https://api.gateio.ws/api/v4/futures/usdt/contracts` |
| Bitget   | Spot    | `https://api.bitget.com/api/spot/v1/public/products` |
| Bitget   | Futures | `https://api.bitget.com/api/mix/v1/market/contracts?productType=umcbl` |
| MEXC     | Futures | `https://contract.mexc.com/api/v1/contract/detail`, `.../contract/list` |
| BingX    | Spot    | `https://open-api.bingx.com/openApi/spot/v1/common/symbols` |
| BingX    | Futures | `https://open-api.bingx.com/openApi/swap/v2/quote/contracts` |
| Bybit    | Spot    | `https://api.bybit.com/v5/market/instruments-info?category=spot` |
| Bybit    | Futures | `https://api.bybit.com/v5/market/instruments-info?category=linear` |

## Development

Formatting is intentionally minimal; run `python -m crypto_hornet.runner` during development or execute `python main.py` directly. Contributions are welcome — feel free to extend `crypto_hornet/exchanges` with additional markets or better heuristics.
