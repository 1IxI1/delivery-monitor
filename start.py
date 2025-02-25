import asyncio
import json

from loguru import logger
from pytonapi import AsyncTonapi
from pytoniq import LiteBalancer, LiteClient

from client import TonCenterClient, TonCenterV3Client
from monitor import TransactionsMonitor

filename = "monitors.json"


async def start_monitor(monitor_params: dict):
    """Parse config and start monitoring with given parameters."""

    provider = monitor_params["provider"]
    wallets_path = monitor_params["wallets"]

    if provider == "liteserver":
        config_path = monitor_params["config"]
        config = json.loads(open(config_path).read())
        client = LiteBalancer.from_config(config, timeout=15)

    elif provider == "toncenter":
        api_url = monitor_params["toncenter_api_url"]
        api_key = monitor_params["toncenter_api_key"]
        if monitor_params.get("v3", False):
            client = TonCenterV3Client(api_url, api_key)
        else:
            client = TonCenterClient(api_url, api_key)

    else:  # provider == "tonapi":
        api_key = monitor_params["tonapi_key"]
        is_testnet = monitor_params.get("testnet", False)
        client = AsyncTonapi(api_key, is_testnet=is_testnet, max_retries=10)

    dbname = provider
    if "dbname" in monitor_params:
        dbname = monitor_params["dbname"]

    dbname_second = None
    if "out_dbname" in monitor_params:
        dbname_second = monitor_params["out_dbname"]

    to_send = None
    if "to_send" in monitor_params:
        to_send = int(monitor_params["to_send"])

    monitor = TransactionsMonitor(
        client, wallets_path, dbname, dbname_second=dbname_second, to_send=to_send
    )
    await monitor.start_worker()


async def start_all():
    with open(filename, "r") as f:
        data = json.load(f)
    # if server run - log to file.
    # otherwise it'll write to console
    if not data.get("cli", False):
        print("Starting delivery monitor in server mode with logging to monitor.log")
        logger.remove()
        logger.add(
            f"monitor.log",
            level="INFO",
            rotation="1 GB",
            compression="zip",
        )
    tasks = []
    for monitor_params in data["monitors"]:
        if monitor_params.get("enabled", False):
            tasks.append(start_monitor(monitor_params))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(start_all())
