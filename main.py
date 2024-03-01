import asyncio
import os
import time

from pytonapi import AsyncTonapi
from pytonapi.async_tonapi.client import json
from pytoniq import LiteClient
from pytoniq.contract.wallets import Wallet, WalletV3R2, WalletV4R2
from pytoniq_core import Address, BlockIdExt, InternalMsgInfo, MessageAny, WalletMessage
from pytoniq_core.boc import Cell
from pytoniq_core.boc.cell import Boc
from pytoniq_core.boc.deserialize import base64
from pytoniq_core.crypto import keys

from wallets import wallets

out_sent = "log/sent.txt"
out_found = "log/found.txt"
os.makedirs("log", exist_ok=True)

wallet_code = Cell.from_boc(
    "b5ee9c72410108010076000114ff00f4a413f4bcf2c80b01020120020302014804050076f28308d71820d31f31d31fd31f3001f823bbf263ed44d0d31fd31fd3ffd15132baf2a103f901541042f910f2a3f80002a4c8cb1fcb1fcbffc9ed540004d03002014806070017bb39ced44d0d33f31d70bff80011b8c97ed44d0d70b1f8e62bd459"
)[0]

config = json.loads(open("testnet.json").read())
client = LiteClient.from_config(config, timeout=10)

sent_tx_ids = set([])
found_tx_ids = set([])


async def watch_transactions():
    """Watches for sent tx to be shown up on wallets
    and adds them to found_tx_ids."""
    while True:
        for wdata in wallets:
            wallet = wdata["wallet"]
            txs = await client.get_transactions(wallet.address, 3, from_lt=0)
            for i in txs:
                if i.in_msg is not None:
                    if i.in_msg.is_external:
                        msg_slice = i.in_msg.body.begin_parse()
                        msg_slice.skip_bits(512)
                        tx_id = msg_slice.load_uint(32)
                        tx_id = f"{tx_id}:{wallet.address.to_str()}"
                        if tx_id in sent_tx_ids and tx_id not in found_tx_ids:
                            found_tx_ids.add(tx_id)
                            print(f"Found tx: {tx_id}")
                            with open(out_found, "a") as f:
                                f.write(tx_id + "\n")
        await asyncio.sleep(15)


async def send_tx_with_id(tx_id: int, wallet: WalletV3R2, private_key: bytes):
    seqno = await wallet.get_seqno()
    msgs = [
        Wallet.create_wallet_internal_message(
            destination=wallet.address,
            send_mode=1,
            value=0,
        )
    ]

    body = wallet.raw_create_transfer_msg(
        private_key=private_key,
        wallet_id=tx_id,
        valid_until=int(time.time()) + 60,
        seqno=seqno,
        messages=msgs,
    )

    message = wallet.create_external_msg(
        dest=wallet.address,
        body=body,
    )

    boc = message.serialize().to_boc()
    await client.raw_send_message(boc)

    tx_full_id = f"{tx_id}:{wallet.address.to_str()}"
    sent_tx_ids.add(tx_full_id)
    print(f"Sent tx {tx_full_id}")
    with open(out_sent, "a") as f:
        f.write(tx_full_id + "\n")


async def start_sending():
    while True:
        tx_id = int(time.time())
        for wdata in wallets:
            try:
                await send_tx_with_id(tx_id, wdata["wallet"], wdata["sk"])
            except Exception as e:
                print(
                    "Failed to send tx with id",
                    tx_id,
                    "from wallet",
                    wdata["addr"],
                    "error:",
                    str(e),
                )
        await asyncio.sleep(30)


async def main():
    await client.connect()

    for k, wdata in enumerate(wallets):
        seed = int(wdata["seed"], 16)
        seed_bytes = seed.to_bytes(32, "big")
        public_key, private_key = keys.crypto_sign_seed_keypair(seed_bytes)
        wallet = await WalletV3R2.from_code_and_data(
            client,
            public_key=public_key,
            private_key=private_key,
            code=wallet_code,
            wallet_id=0,
        )
        if wdata["addr"] != wallet.address.to_str():
            raise ValueError("Invalid seed for wallet " + wdata["addr"])
        wallets[k]["wallet"] = wallet
        wallets[k]["sk"] = private_key

    asyncio.create_task(watch_transactions())
    asyncio.create_task(start_sending())

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


if __name__ == "__main__":
    asyncio.run(main())
