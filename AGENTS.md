# Repository Guidelines

## Project Structure & Module Organization
`start.py` is the main entry point: it reads `monitors.json` and starts one `TransactionsMonitor` per enabled provider. Core logic lives in `monitor.py`; provider adapters in `client.py`; storage backends in `db_backend.py`; `api.py` serves read-only stats; `migrate_db.py` upgrades schemas. Tracked network configs live in `configs/`, system service files in `systemd/`, and TON helpers in `ts/`. Use `dev/` for one-off local scripts, but note that `dev/` is ignored by Git. Runtime artifacts such as `monitors.json`, `db/`, `*.log`, `seed.hex`, and wallet files are local-only.

## Build, Test, and Development Commands
`python3 -m venv .venv && source .venv/bin/activate` creates a local environment. `pip install -r requirements.txt` installs dependencies. Prefer `.venv/bin/python` for direct runs because `python` may be absent from `PATH`. `cp monitors.json.example monitors.json` creates the local monitor config; fill in API keys and wallet paths before running anything.

`.venv/bin/python start.py` starts the monitor workers. `.venv/bin/python api.py` launches the Waitress-backed API on port `8000`. `.venv/bin/python migrate_db.py` adds newly introduced columns to existing SQLite or ClickHouse storage. There is no repo-local Node build script; only touch `ts/` if you already have the TON Blueprint toolchain configured.

## Coding Style & Naming Conventions
Match the existing code style rather than introducing a new one. Python uses 4-space indentation, snake_case for functions and variables, and CapWords for classes such as `TransactionsMonitor` and `TonCenterStreamingClient`. Keep JSON keys aligned with current config names like `send_interval`, `valid_until_timeout`, and `dbname`. TypeScript files in `ts/` use 4-space indentation, single quotes, and semicolons.

## Testing Guidelines
There is no automated test suite checked in. Use lightweight smoke checks before opening a PR: `.venv/bin/python -m py_compile start.py api.py monitor.py client.py db_backend.py migrate_db.py`, then run the affected flow against a disposable `monitors.json` and testnet wallets. For API or DB changes, verify both `.venv/bin/python start.py` and `.venv/bin/python api.py` against a fresh local `db/*.db`. For one-off helpers, reuse `TransactionsMonitor.prepare_and_send_to_wallet()` and existing message-building logic instead of reimplementing wallet serialization.

## Commit & Pull Request Guidelines
Recent history uses short imperative subjects like `Make intervals individual for monitors` and `Optimize WHERE`. Keep commit titles concise, sentence case, and focused on one change. PRs should state the behavior change, note any config or schema impact, and include the manual validation commands you ran. Add sample logs or API responses when they clarify provider-specific behavior.

## Security & Configuration Tips
Do not commit secrets or runtime data: `monitors.json`, `.env`, wallet seeds, `seed.hex`, database files, or logs. `w/` is also ignored, so treat wallet files there as local secrets. Prefer sanitized examples in docs and use `MONITORS_CONFIG=/path/to/file.json` when you need to point `api.py` or `migrate_db.py` at a non-default config.
