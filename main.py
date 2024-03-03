import asyncio
import os
import sys
import time

from dotenv import load_dotenv

# from pytonapi import AsyncTonapi
from pytonapi.async_tonapi.client import json
from pytoniq import LiteClient
from pytoniq.contract.wallets import Wallet
from pytoniq_core import Builder
from pytoniq_core.boc import Address, Cell
from pytoniq_core.crypto import keys
from tonsdk.utils import sign_message

from client import TonCenterClient
from wallets import wallets

config = json.loads(open("testnet.json").read())
client = LiteClient.from_config(config, timeout=10)

sent_tx_ids = set([])
found_tx_ids = set([])


def parse_and_add_msg(msg: Cell, addr: str):
    msg_slice = msg.begin_parse()
    msg_slice.skip_bits(512)
    tx_id = msg_slice.load_uint(32)
    tx_id = f"{tx_id}:{addr}"
    if tx_id in sent_tx_ids and tx_id not in found_tx_ids:
        found_tx_ids.add(tx_id)
        print(f"Found tx: {tx_id}")
        with open(out_found, "a") as f:
            f.write(tx_id + "\n")


async def watch_transactions():
    """Watches for sent tx to be shown up on wallets
    and adds them to found_tx_ids."""
    while True:
        for wdata in wallets:
            if isinstance(client, LiteClient):
                txs = await client.get_transactions(wdata["addr"], 3, from_lt=0)
                for tx in txs:
                    if tx.in_msg is not None and tx.in_msg.is_external:
                        parse_and_add_msg(tx.in_msg.body, wdata["addr"])

            elif isinstance(client, TonCenterClient):
                txs = await client.get_transactions(wdata["addr"], 3, from_lt=0)
                for tx in txs:
                    if (
                        "in_msg" in tx
                        and tx["in_msg"]
                        # from nowhere:
                        and tx["in_msg"]["source"] == ""
                    ):
                        body_b64 = tx["in_msg"]["msg_data"]["body"]
                        body = Cell.from_boc(body_b64)[0]
                        parse_and_add_msg(body, wdata["addr"])
            else:
                raise NotImplementedError("Tonapi or smth")
        await asyncio.sleep(15)


async def init_client():
    if isinstance(client, LiteClient):
        await client.connect()
    elif isinstance(client, TonCenterClient):
        pass
    else:
        raise NotImplementedError("Tonapi or smth")


async def get_seqno(address: str) -> int:
    if isinstance(client, TonCenterClient):
        return await client.seqno(address)
    elif isinstance(client, LiteClient):
        return (await client.run_get_method(address, "seqno", []))[0]
    else:
        raise NotImplementedError("Tonapi or smth")


async def sendboc(boc: bytes):
    if isinstance(client, LiteClient):
        await client.raw_send_message(boc)
    elif isinstance(client, TonCenterClient):
        await client.send(boc)
    else:
        raise NotImplementedError("Tonapi or smth")


async def send_tx_with_id(tx_id: int, wallet_address: str, private_key: bytes):
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
    sent_tx_ids.add(tx_full_id)
    print(f"Sent tx {tx_full_id}")
    with open(out_sent, "a") as f:
        f.write(tx_full_id + "\n")


async def start_sending():
    while len(sent_tx_ids) < sends_count:
        tx_id = int(time.time())
        for wdata in wallets:
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
    for k, wdata in enumerate(wallets):
        seed = int(wdata["seed"], 16)
        seed_bytes = seed.to_bytes(32, "big")
        public_key, private_key = keys.crypto_sign_seed_keypair(seed_bytes)
        wallets[k]["sk"] = private_key


async def printer():
    while True:
        missing = []
        for i in sent_tx_ids:
            if i not in found_tx_ids:
                missing.append(i)
        print("--------------")
        print("Missing txs:", missing)
        print(f"Found/sent: {len(found_tx_ids)}/{len(sent_tx_ids)}")
        print("Success rate:", 1 - len(missing) / (len(sent_tx_ids) or 1))
        await asyncio.sleep(5)


async def worker():
    await init_client()
    await read_wallets()
    asyncio.create_task(watch_transactions())
    asyncio.create_task(printer())
    await start_sending()
    print(f"\nDone sending {len(sent_tx_ids)} txs")


if __name__ == "__main__":
    sends_count = 100
    if len(sys.argv) > 1:
        sends_count = int(sys.argv[1])

    load_dotenv()

    logs_path = os.getenv("LOGDIR") or "log"
    os.makedirs(logs_path, exist_ok=True)
    out_sent = logs_path + "/sent.txt"
    out_found = logs_path + "/found.txt"

    provider = os.getenv("PROVIDER")
    if not provider or provider not in ["toncenter", "liteserver", "tonapi"]:
        raise ValueError("Invalid PROVIDER env variable")

    if provider == "tonapi":
        raise NotImplementedError("TON API provider is not implemented yet")
    elif provider == "liteserver":
        config_path = os.getenv("CONFIG")
        if not config_path:
            raise ValueError("No CONFIG env variable")
        config = json.loads(open(config_path).read())
        client = LiteClient.from_config(config, timeout=10)
    elif provider == "toncenter":
        api_url = os.getenv("TONCENTER_API_URL")
        api_key = os.getenv("TONCENTER_API_KEY")
        if not api_url or not api_key:
            raise ValueError("No API_URL or API_KEY env variable")
        client = TonCenterClient(api_url, api_key)

    asyncio.run(worker())
