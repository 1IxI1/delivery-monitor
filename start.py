import asyncio
import json

from loguru import logger
from pytonapi import AsyncTonapi
from pytoniq import LiteBalancer, LiteClient

from client import TonCenterClient, TonCenterV3Client, TonCenterStreamingClient
from monitor import TransactionsMonitor

filename = "monitors.json"

# global config, set from top-level of monitors.json
_db_config_base: dict = {}
_valid_until_timeout: int = 40
_send_interval: int = 40


def get_db_config(is_testnet: bool) -> dict:
    """get db config with correct database for testnet/mainnet"""
    config = _db_config_base.copy()
    if config.get("db_backend") == "clickhouse":
        if is_testnet:
            config["clickhouse_database"] = config.get("clickhouse_database_testnet", "default")
        else:
            config["clickhouse_database"] = config.get("clickhouse_database_mainnet", "default")
    return config


async def start_monitor(monitor_params: dict):
    """Parse config and start monitoring with given parameters."""

    provider = monitor_params["provider"]
    wallets_path = monitor_params["wallets"]
    is_testnet = monitor_params.get("testnet", False)

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

    elif provider == "toncenter_streaming_ws":
        api_key = monitor_params["toncenter_api_key"]
        api_url = monitor_params.get("toncenter_api_url", "https://toncenter.com/api/v3/")
        is_testnet = monitor_params.get("testnet", False)
        client = TonCenterStreamingClient(api_key, testnet=is_testnet)
        # sender_client for seqno and sending txs
        sender_client = TonCenterV3Client(api_url, api_key)

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

    # sender_client only for streaming mode
    monitor_sender_client = None
    if provider == "toncenter_streaming_ws":
        monitor_sender_client = sender_client

    # with_state_init for deploying contracts on first tx
    with_state_init = monitor_params.get("with_state_init", False)
    
    # extra_msg_boc for sending additional message in action list
    extra_msg_boc = monitor_params.get("extra_msg_boc", None)
    target_action_type = monitor_params.get("target_action_type", "unknown")

    # get db config with correct database for this monitor's network
    db_config = get_db_config(is_testnet)

    monitor = TransactionsMonitor(
        client, wallets_path, dbname, db_config=db_config,
        dbname_second=dbname_second, to_send=to_send,
        sender_client=monitor_sender_client, with_state_init=with_state_init,
        extra_msg_boc=extra_msg_boc, target_action_type=target_action_type,
        valid_until_timeout=_valid_until_timeout, send_interval=_send_interval
    )
    await monitor.start_worker()


async def start_all():
    global _db_config_base, _valid_until_timeout, _send_interval
    
    with open(filename, "r") as f:
        data = json.load(f)
    
    # load global timing config
    _valid_until_timeout = data.get("valid_until_timeout", 40)
    _send_interval = data.get("send_interval", 40)
    
    # load db config from top-level (shared by all monitors)
    _db_config_base = {
        "db_backend": data.get("db_backend", "sqlite"),
        "clickhouse_host": data.get("clickhouse_host", "localhost"),
        "clickhouse_port": data.get("clickhouse_port", 9000),
        "clickhouse_user": data.get("clickhouse_user", "default"),
        "clickhouse_password": data.get("clickhouse_password", ""),
        "clickhouse_database_mainnet": data.get("clickhouse_database_mainnet", "default"),
        "clickhouse_database_testnet": data.get("clickhouse_database_testnet", "default"),
    }
    
    # if server run - log to file.
    # otherwise it'll write to console
    if not data.get("cli", False):
        print("Starting delivery monitor in server mode with logging to monitor.log")
        logger.remove()
        logger.add(
            f"monitor.log",
            level="DEBUG",
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
