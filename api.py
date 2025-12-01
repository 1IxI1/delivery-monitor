import json
import math
import os
import time
from pathlib import Path
from typing import Optional

from flask import Flask, request, send_file
from flask_cors import CORS
from loguru import logger
from waitress import serve

from db_backend import DatabaseBackend, SQLiteBackend, ClickHouseBackend, create_backend

app = Flask(__name__)
CORS(app)

# global db config, loaded at startup
_db_config: dict = {}


def load_db_config():
    """load db config from monitors.json"""
    global _db_config
    config_path = os.environ.get("MONITORS_CONFIG", "monitors.json")
    try:
        with open(config_path, "r") as f:
            data = json.load(f)
        _db_config = {
            "db_backend": data.get("db_backend", "sqlite"),
            "clickhouse_host": data.get("clickhouse_host", "localhost"),
            "clickhouse_port": data.get("clickhouse_port", 9000),
            "clickhouse_user": data.get("clickhouse_user", "default"),
            "clickhouse_password": data.get("clickhouse_password", ""),
            "clickhouse_database_mainnet": data.get("clickhouse_database_mainnet", "default"),
            "clickhouse_database_testnet": data.get("clickhouse_database_testnet", "default"),
        }
        logger.info(f"Loaded db config: backend={_db_config['db_backend']}")
    except Exception as e:
        logger.warning(f"Failed to load monitors.json, using sqlite: {e}")
        _db_config = {"db_backend": "sqlite"}


def get_backend(dbname: str, testnet: bool = False) -> Optional[DatabaseBackend]:
    """create backend for given dbname, returns None if db doesn't exist"""
    backend_type = _db_config.get("db_backend", "sqlite")
    
    if backend_type == "sqlite":
        # check if file exists
        if not Path(f"db/{dbname}.db").is_file():
            return None
        return SQLiteBackend(dbname)
    else:
        # select database based on testnet flag
        if testnet:
            database = _db_config.get("clickhouse_database_testnet", "default")
        else:
            database = _db_config.get("clickhouse_database_mainnet", "default")
        
        return ClickHouseBackend(
            dbname=dbname,
            host=_db_config.get("clickhouse_host", "localhost"),
            port=_db_config.get("clickhouse_port", 9000),
            user=_db_config.get("clickhouse_user", "default"),
            password=_db_config.get("clickhouse_password", ""),
            database=database,
        )


def format_stats_result(res: tuple) -> dict:
    """format raw stats tuple into dict"""
    return {
        "txs": res[0],
        "success_rate": round(res[1] or 0, 6),
        "executed_in_avg": round(res[2] or 0, 6),
        "executed_in_min": round(res[3] or 0, 6) if res[3] is not None else None,
        "executed_in_max": round(res[4] or 0, 6) if res[4] is not None else None,
        # stdev = sqrt(variance)
        "executed_in_sdev": round(math.sqrt(res[5] or 0), 6),
        "found_in_avg": round(res[6] or 0, 6),
        "found_in_min": round(res[7] or 0, 6) if res[7] is not None else None,
        "found_in_max": round(res[8] or 0, 6) if res[8] is not None else None,
        # stdev = sqrt(variance)
        "found_in_sdev": round(math.sqrt(res[9] or 0), 6),
        "commited_in_avg": round(res[10] or 0, 6),
        "commited_in_min": round(res[11] or 0, 6) if res[11] is not None else None,
        "commited_in_max": round(res[12] or 0, 6) if res[12] is not None else None,
        # stdev = sqrt(variance)
        "commited_in_sdev": round(math.sqrt(res[13] or 0), 6),
        "sendboc_took_avg": round(res[14] or 0, 6) if res[14] is not None else None,
        # streaming metrics
        "pending_tx_in_avg": round(res[15] or 0, 6) if res[15] is not None else None,
        "pending_action_in_avg": round(res[16] or 0, 6) if res[16] is not None else None,
        "confirmed_tx_in_avg": round(res[17] or 0, 6) if res[17] is not None else None,
        "confirmed_action_in_avg": round(res[18] or 0, 6) if res[18] is not None else None,
        "finalized_tx_in_avg": round(res[19] or 0, 6) if res[19] is not None else None,
        "finalized_action_in_avg": round(res[20] or 0, 6) if res[20] is not None else None,
        "streaming_to_v3_lag_avg": round(res[21] or 0, 6) if res[21] is not None else None,
        "ping_ws_avg": round(res[22] or 0, 6) if res[22] is not None else None,
        "ping_v3_avg": round(res[23] or 0, 6) if res[23] is not None else None,
    }


@app.route("/")
def index():
    return {
        "error": "Invalid endpoint, my dear!",
        "hint": "Use /interval/liteserver?seconds=3600 or /stats/liteserver,",
        "n also": "replace 'liteserver' with 'toncenter' or 'tonapi'.",
        "db_backend": _db_config.get("db_backend", "sqlite"),
    }


@app.route("/db/<dbname>")
def download_db(dbname):
    """Download the raw .db file. Handle with care! (sqlite only)"""
    if "/" in dbname or ".." in dbname:
        logger.debug(f"Nice try: {dbname}")
        return {"error": "invalid path"}

    if _db_config.get("db_backend") != "sqlite":
        return {"error": "db download only available for sqlite backend"}

    db_path = Path(f"db/{dbname}.db")
    if not db_path.is_file():
        logger.debug(f"No such db: {dbname}")
        return {"error": "no such db"}

    logger.info(f"Serving db file: {dbname}")
    return send_file(db_path, as_attachment=True)


time_intervals = [
    ["10m", 10 * 60],
    ["30m", 30 * 60],
    ["1h", 60 * 60],
    ["3h", 3 * 60 * 60],
    ["12h", 12 * 60 * 60],
    ["24h", 24 * 60 * 60],
    ["3d", 3 * 24 * 60 * 60],
    ["7d", 7 * 24 * 60 * 60],
    ["30d", 30 * 24 * 60 * 60],
    ["90d", 90 * 24 * 60 * 60],
]


@app.route("/interval/<path:path>")
def interval(path):
    """Get averages on period of now-1h to now-60sec. 1h interval can
    be changed to any value by setting `seconds` parameter.
    Use ?testnet=1 for testnet database."""

    if path.find("/") != -1:
        logger.debug(f"Invalid db path: {path}")
        return {"error": "invalid path: / not allowed"}

    testnet = request.args.get("testnet", "0") == "1"
    backend = get_backend(path, testnet=testnet)
    if backend is None:
        logger.debug(f"Invalid db path: {path}")
        return {"error": "no such db"}

    interval_sec = float(request.args.get("seconds", 3600))

    try:
        backend.init_db()  # ensure connected
        res = backend.get_interval_stats(interval_sec)

        if res is None or res[0] == 0:
            backend.close()
            return {"error": "no txs in this period"}

        result = format_stats_result(res)
        backend.close()
        logger.debug(f"Completed request on /interval/{path}")
        return result

    except Exception as e:
        try:
            backend.close()
        except:
            pass
        logger.error(str(e))
        return {"error": str(e)}


@app.route("/stats/<path:path>")
def get_processed(path):
    """Get stats for multiple time intervals. Use ?testnet=1 for testnet database."""
    if path.find("/") != -1:
        logger.debug(f"Invalid db path: {path}")
        return {"error": "invalid path: / not allowed"}

    testnet = request.args.get("testnet", "0") == "1"
    backend = get_backend(path, testnet=testnet)
    if backend is None:
        logger.debug(f"Invalid db path: {path}")
        return {"error": "no such db"}

    addr = request.args.get("addr", "") or None

    try:
        backend.init_db()  # ensure connected
        result = {}
        last_len = 0

        for interval_txt, interval_sec in time_intervals:
            res = backend.get_stats_for_interval(interval_sec, addr=addr)

            if res is None or res[0] == last_len:
                continue

            result[interval_txt] = format_stats_result(res)
            last_len = res[0]

        backend.close()
        logger.debug(f"Completed request on /stats/{path}")
        return result

    except Exception as e:
        try:
            backend.close()
        except:
            pass
        logger.error(str(e))
        return {"error": str(e)}


if __name__ == "__main__":
    logger.remove()
    logger.add("api.log", level="DEBUG", rotation="500 MB", compression="zip")
    load_db_config()
    logger.info("Starting tiny API")
    serve(app, host="0.0.0.0", port=8000)
