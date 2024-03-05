import asyncio
import base64
import os
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

config = json.loads(open("testnet.json").read())
client = LiteClient.from_config(config, timeout=10)

wallets = []

missing_txs = set([])
sent_count = 0


def parse_and_add_msg(msg: Cell, blockutime: int, addr: str) -> bool:
    msg_slice = msg.begin_parse()
    msg_slice.skip_bits(512)
    tx_id = msg_slice.load_uint(32)
    tx_full_id = f"{tx_id}:{addr}"
    if tx_full_id in missing_txs:
        missing_txs.remove(tx_full_id)
        current = int(time.time())
        print(
            f"Found tx: {tx_full_id} at {current}. Executed in {blockutime - tx_id} sec. Found in {current - tx_id} sec."
        )
        with open(out_found, "a") as f:
            f.write(f"{tx_full_id}, {blockutime - tx_id}, {current - tx_id}\n")
            return True
    return False


async def watch_transactions():
    """Watches for sent tx to be shown up on wallets
    and adds them to found_tx_ids."""
    while True:
        for tx_id in missing_txs.copy():
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
                txs = await client.blockchain.get_account_transactions(addr, limit=3)
                for tx in txs.transactions:
                    if tx.in_msg is not None and tx.in_msg.source is None:
                        body = tx.in_msg.raw_body
                        if isinstance(body, str):
                            body = Cell.from_boc(body)[0]
                            parse_and_add_msg(body, tx.utime, addr)
                # from pprint import pprint
                # pprint(txs)
                await asyncio.sleep(2)
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
        await client.send(boc)
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
    body.store_uint(int(time.time()) + 60, 32)
    body.store_uint(seqno, 32)
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

    tx_full_id = f"{tx_id}:{wallet_address}"
    missing_txs.add(tx_full_id)
    sent_count += 1
    print(f"Sent tx {tx_full_id}")
    with open(out_sent, "a") as f:
        f.write(tx_full_id + "\n")


async def start_sending():
    while sent_count < sends_count:
        for wdata in wallets:
            tx_id = int(time.time())
            try:
                await send_tx_with_id(tx_id, wdata["addr"], wdata["sk"])
            except Exception as e:
                raise e
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
        print("--------------")
        print("Missing txs:", list(missing_txs) or "-")
        print(f"Found/sent: {sent_count - len(missing_txs)}/{sent_count}")
        print("Success rate:", 1 - len(missing_txs) / (sent_count or 1))
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

    logs_path = os.getenv("LOGDIR") or "log"
    os.makedirs(logs_path, exist_ok=True)
    out_sent = logs_path + "/sent.txt"
    out_found = logs_path + "/found.txt"

    wallets_path = os.getenv("WALLETS") or "wallets.txt"

    provider = os.getenv("PROVIDER")
    if not provider or provider not in ["toncenter", "liteserver", "tonapi"]:
        raise ValueError("Invalid PROVIDER env variable")

    if provider == "tonapi":
        # raise NotImplementedError("TON API provider is not implemented yet")
        api_key = os.getenv("TONAPI_KEY")
        if not api_key:
            raise ValueError("No API_KEY env variable")
        is_testnet = bool(os.getenv("TESTNET"))
        if is_testnet is None:
            raise ValueError("No TESTNET env variable")
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
        if not api_url or not api_key:
            raise ValueError("No API_URL or API_KEY env variable")
        client = TonCenterClient(api_url, api_key)

    asyncio.run(worker())
