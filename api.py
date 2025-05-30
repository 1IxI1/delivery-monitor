import math
import sqlite3
import time
from pathlib import Path

from flask import Flask, request
from flask_cors import CORS
from loguru import logger
from waitress import serve

app = Flask(__name__)
CORS(app)


@app.route("/")
def index():
    return {
        "error": "Invalid endpoint, my dear!",
        "hint": "Use /interval/liteserver?seconds=3600 or /stats/liteserver,",
        "n also": "replace 'liteserver' with 'toncenter' or 'tonapi'.",
    }


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
    be changed to any value by setting `seconds` parameter."""

    if path.find("/") != -1:
        logger.debug(f"Invalid db path: {path}")
        return {"error": "invalid path: / not allowed"}

    if not Path(f"db/{path}.db").is_file():
        logger.debug(f"Invalid db path: {path}")
        return {"error": "no such db"}

    interval_sec = float(request.args.get("seconds", 3600))

    # to close connection in case of exception
    class Connection:
        def close(self):
            pass

    connection = Connection()

    try:
        connection = sqlite3.connect(f"db/{path}.db")
        cursor = connection.cursor()

        now = time.time()

        cursor.execute(
            f"""
            SELECT COUNT(*), AVG(is_found),

                   AVG(executed_in), MIN(executed_in),
                   MAX(executed_in),
                   -- variance of executed_in
                   AVG(executed_in*executed_in) - AVG(executed_in)*AVG(executed_in),

                   AVG(found_in), MIN(found_in),
                   MAX(found_in),
                   -- variance of found_in
                   AVG(found_in*found_in) - AVG(found_in)*AVG(found_in),

                   AVG(commited_in), MIN(commited_in),
                   MAX(commited_in),
                   -- variance of commited_in
                   AVG(commited_in*commited_in) - AVG(commited_in)*AVG(commited_in)

            FROM txs WHERE utime >= ?
                AND utime <= ?
            ORDER BY utime DESC LIMIT 1""",
            (now - 60 - interval_sec, now - 60),
        )

        res = cursor.fetchone()

        if res[0] == 0:
            return {"error": "no txs in this period"}

        result = {
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
        }

        connection.close()
        logger.debug(f"Completed request on /{path}")
        return result

    except Exception as e:
        try:
            connection.close()
            logger.info("Forcefully, but successfully closed the connection")
        except:
            logger.warning("Failed to close connection")
            pass
        logger.error(str(e))
        return {"error": str(e)}


@app.route("/stats/<path:path>")
def get_processed(path):
    if path.find("/") != -1:
        logger.debug(f"Invalid db path: {path}")
        return {"error": "invalid path: / not allowed"}

    if not Path(f"db/{path}.db").is_file():
        logger.debug(f"Invalid db path: {path}")
        return {"error": "no such db"}

    addr = request.args.get("addr", "")

    # to close connection in case of exception
    class Connection:
        def close(self):
            pass

    connection = Connection()

    try:
        connection = sqlite3.connect(f"db/{path}.db")
        cursor = connection.cursor()

        now = time.time()

        result = {}

        last_len = 0
        for interval_txt, interval_sec in time_intervals:
            addr_appendix = ""
            if addr:
                addr_appendix = f"AND addr = '{addr}'"

            cursor.execute(
                f"""
                SELECT COUNT(*), AVG(is_found),

                       AVG(executed_in), MIN(executed_in),
                       MAX(executed_in),
                       -- variance of executed_in
                       AVG(executed_in*executed_in) - AVG(executed_in)*AVG(executed_in),

                       AVG(found_in), MIN(found_in),
                       MAX(found_in),
                       -- variance of found_in
                       AVG(found_in*found_in) - AVG(found_in)*AVG(found_in),

                       AVG(commited_in), MIN(commited_in),
                       MAX(commited_in),
                       -- variance of commited_in
                       AVG(commited_in*commited_in) - AVG(commited_in)*AVG(commited_in)

                FROM txs WHERE utime >= ?
                {addr_appendix}
                ORDER BY utime DESC LIMIT 1""",
                (now - interval_sec,),
            )

            res = cursor.fetchone()
            if res[0] == last_len:
                continue

            result[interval_txt] = {
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
            }
            last_len = res[0]

        connection.close()
        logger.debug(f"Completed request on /{path}")
        return result

    except Exception as e:
        try:
            connection.close()
            logger.info("Forcefully, but successfully closed the connection")
        except:
            logger.warning("Failed to close connection")
            pass
        logger.error(str(e))
        return {"error": str(e)}


if __name__ == "__main__":
    logger.remove()
    logger.add("api.log", level="DEBUG", rotation="500 MB", compression="zip")
    logger.info("Starting tiny API")
    serve(app, host="0.0.0.0", port=8000)
