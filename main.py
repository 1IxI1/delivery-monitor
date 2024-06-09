import asyncio
import base64
import os
import sqlite3
import sys
import time

from dotenv import load_dotenv
from pytonapi import AsyncTonapi
from pytonapi.async_tonapi.client import json
from pytoniq import LiteClient
from pytoniq.contract.wallets import Wallet
from pytoniq_core import Builder
from pytoniq_core.boc import Address, Cell
from pytoniq_core.crypto import keys
from tonsdk.utils import sign_message

from client import TonCenterClient

# from wallets import wallets

extended_message = False
wallets = []

sent_count = 0

def add_new_tx(tx_id: int, addr: str):
    cursor.execute(
        "INSERT INTO txs (addr, utime, is_found) VALUES (?, ?, 0)",
        (addr, tx_id),
    )
    connection.commit()

def get_missing_ids():
    cursor.execute("SELECT addr, utime FROM txs WHERE is_found = 0")
    result = cursor.fetchall()
    missing_ids = []
    for addr, utime in result:
        missing_ids.append(f"{utime}:{addr}")
    return missing_ids


def make_found(tx_full_id: str, executed_in: int, found_in: int):
    utime, addr = tx_full_id.split(":")
    cursor.execute(
        "UPDATE txs SET is_found = 1, executed_in = ?, found_in = ? WHERE addr = ? AND utime = ?",
        (executed_in, found_in, addr, utime),
    )
    connection.commit()


def parse_and_add_msg(msg: Cell, blockutime: int, addr: str) -> bool:
    msg_slice = msg.begin_parse()
    msg_slice.skip_bits(512)
    tx_id = msg_slice.load_uint(32)
    tx_full_id = f"{tx_id}:{addr}"
    if tx_full_id in get_missing_ids():
        # missing_txs.remove(tx_full_id)
        current = int(time.time())
        print(
            f"Found tx: {tx_full_id} at {current}. Executed in {blockutime - tx_id} sec. Found in {current - tx_id} sec."
        )
        executed_in = blockutime - tx_id
        found_in = current - tx_id
        make_found(tx_full_id, executed_in, found_in)
        return True
    else:
        return False


async def watch_transactions():
    """Watches for sent tx to be shown up on wallets
    and adds them to found_tx_ids."""
    while True:
        try:
            for tx_id in get_missing_ids():
                addr = tx_id.split(":")[1]
                if isinstance(client, LiteClient):
                    txs = await client.get_transactions(addr, 3, from_lt=0)
                    for tx in txs:
                        if tx.in_msg is not None and tx.in_msg.is_external:
                            parse_and_add_msg(tx.in_msg.body, tx.now, addr)

                elif isinstance(client, TonCenterClient):
                    txs = await client.get_transactions(addr, 3, from_lt=0)
                    for tx in txs:
                        if (
                            "in_msg" in tx
                            and tx["in_msg"]
                            # from nowhere:
                            and tx["in_msg"]["source"] == ""
                        ):
                            body_b64 = tx["in_msg"]["msg_data"]["body"]
                            body = Cell.from_boc(body_b64)[0]
                            r = parse_and_add_msg(body, tx["utime"], addr)
                else:
                    txs = await client.blockchain.get_account_transactions(
                        addr, limit=3
                    )
                    for tx in txs.transactions:
                        if tx.in_msg is not None and tx.in_msg.source is None:
                            body = tx.in_msg.raw_body
                            if isinstance(body, str):
                                body = Cell.from_boc(body)[0]
                                parse_and_add_msg(body, tx.utime, addr)
                    # from pprint import pprint
                    # pprint(txs)
                    await asyncio.sleep(2)
        except Exception as e:
            print("watch_transactions failed, retrying:", e)
        await asyncio.sleep(4)


async def init_client():
    if isinstance(client, LiteClient):
        await client.connect()
    elif isinstance(client, TonCenterClient):
        pass
    else:
        pass


async def get_seqno(address: str) -> int:
    if isinstance(client, TonCenterClient):
        return await client.seqno(address)
    elif isinstance(client, LiteClient):
        return (await client.run_get_method(address, "seqno", []))[0]
    else:
        return (await client.wallet.get_account_seqno(address)) or 0


async def sendboc(boc: bytes):
    if isinstance(client, LiteClient):
        await client.raw_send_message(boc)
    elif isinstance(client, TonCenterClient):
        print(base64.b64encode(boc))
        print(await client.send(boc))
    else:
        # raise NotImplementedError("Tonapi or smth")
        api_body = {"boc": base64.b64encode(boc).decode()}
        await client.blockchain.send_message(body=api_body)
        await asyncio.sleep(2)


async def send_tx_with_id(tx_id: int, wallet_address: str, private_key: bytes):
    global sent_count
    seqno = await get_seqno(wallet_address)

    # ext_msg_body#_ signature:bits512 some_value:uint32
    #                valid_until:uint32 msg_seqno:uint32
    #                = ExtMsgBody;
    body = Builder()
    body.store_uint(tx_id, 32)
    body.store_uint(int(time.time()) + 9000, 32)
    body.store_uint(seqno, 32)

    if extended_message:  # making the size of 1MB
        extension1 = Builder().store_bits("11" * 499).end_cell()
        extension2 = Builder().store_bits("01" * 499).end_cell()
        extension3 = Builder().store_bits("10" * 499).end_cell()
        body.store_ref(
            Builder().store_bits("11" * 500).store_ref(extension1).end_cell()
        )
        body.store_ref(
            Builder().store_bits("01" * 501).store_ref(extension2).end_cell()
        )
        body.store_ref(
            Builder().store_bits("10" * 502).store_ref(extension3).end_cell()
        )
        body.store_ref(Builder().store_bits("00" * 503).end_cell())
    body = body.end_cell()

    signature = sign_message(body.hash, private_key).signature
    signed_body = Builder().store_bytes(signature).store_cell(body).end_cell()

    addr = Address(wallet_address)
    message = Wallet.create_external_msg(
        dest=addr,
        body=signed_body,
    )

    boc = message.serialize().to_boc()
    await sendboc(boc)

    add_new_tx(tx_id, wallet_address)
    tx_full_id = f"{tx_id}:{wallet_address}"
    # missing_txs.add(tx_full_id)
    sent_count += 1
    print(f"Sent tx {tx_full_id}")
    # with open(out_sent, "a") as f:
    #     f.write(tx_full_id + "\n")


async def start_sending():
    while sent_count < sends_count:
        for wdata in wallets:
            tx_id = int(time.time())
            try:
                await send_tx_with_id(tx_id, wdata["addr"], wdata["sk"])
            except Exception as e:
                # raise e
                print(
                    "Failed to send tx with id",
                    tx_id,
                    "from wallet",
                    wdata["addr"],
                    "error:",
                    str(e),
                )
        await asyncio.sleep(60)


async def read_wallets():
    global wallets
    with open(wallets_path, "r") as f:
        for line in f.readlines():
            addr, seed = line.split()
            seed = int(seed, 16)
            seed_bytes = seed.to_bytes(32, "big")
            _, private_key = keys.crypto_sign_seed_keypair(seed_bytes)
            wallets.append(
                {
                    "addr": addr,
                    "sk": private_key,
                }
            )


async def printer():
    while True:
        missing_ids = get_missing_ids()
        print("--------------")
        print("Missing txs:", missing_ids or "-")
        print(f"Found/sent: {sent_count - len(missing_ids)}/{sent_count}")
        print("Success rate:", 1 - len(missing_ids) / (sent_count or 1))
        await asyncio.sleep(5)


async def worker():
    await init_client()
    await read_wallets()
    asyncio.create_task(watch_transactions())
    asyncio.create_task(printer())
    await start_sending()
    print(f"\nDone sending {sent_count} txs")


if __name__ == "__main__":
    sends_count = 100
    if len(sys.argv) > 1:
        sends_count = int(sys.argv[1])

    load_dotenv()

    # logs_path = os.getenv("LOGDIR") or "log"
    # os.makedirs(logs_path, exist_ok=True)
    # out_sent = logs_path + "/sent.txt"
    # out_found = logs_path + "/found.txt"

    wallets_path = os.getenv("WALLETS") or "wallets.txt"

    provider = os.getenv("PROVIDER")
    if not provider or provider not in ["toncenter", "liteserver", "tonapi"]:
        print(provider)
        raise ValueError("Invalid PROVIDER env variable")

    if provider == "tonapi":
        api_key = os.getenv("TONAPI_KEY")
        if not api_key:
            raise ValueError("No API_KEY env variable")

        is_testnet = os.getenv("TESTNET", "False").lower() in ("true", "1", "t")
        client = AsyncTonapi(api_key, is_testnet=is_testnet, max_retries=10)

    elif provider == "liteserver":
        config_path = os.getenv("CONFIG")
        if not config_path:
            raise ValueError("No CONFIG env variable")
        config = json.loads(open(config_path).read())
        client = LiteClient.from_config(config, timeout=15)

    elif provider == "toncenter":
        api_url = os.getenv("TONCENTER_API_URL")
        api_key = os.getenv("TONCENTER_API_KEY")
        print(api_key)
        if not api_url or not api_key:
            raise ValueError("No API_URL or API_KEY env variable")
        client = TonCenterClient(api_url, api_key)

    # default is for ex. db/liteserver.db
    dbname = os.getenv("DBNAME", provider)

    os.makedirs("db/", exist_ok=True)
    connection = sqlite3.connect(f"db/{dbname}.db")
    cursor = connection.cursor()

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS txs (
        addr TEXT,
        utime INTEGER,
        is_found BOOLEAN,
        executed_in INTEGER,
        found_in INTEGER,
        PRIMARY KEY (addr, utime)
    )
    """
    )

    asyncio.run(worker())
    connection.close()
