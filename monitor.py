import asyncio
import base64
import os
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union

from dotenv import load_dotenv
from loguru import logger
from pytonapi import AsyncTonapi
from pytonapi.async_tonapi.client import json
from pytoniq import LiteClient, begin_cell
from pytoniq.contract.wallets import Wallet
from pytoniq_core import Builder
from pytoniq_core.boc import Address, Cell
from pytoniq_core.crypto import keys
from tonsdk.utils import sign_message

from client import TonCenterClient


@dataclass
class WalletInfo:
    addr: str
    sk: bytes
    pk_hex: str


VALID_UNTIL_TIMEOUT = 60
SEND_INTERVAL = 120

Client = Union[LiteClient, TonCenterClient, AsyncTonapi]


class TransactionsMonitor:
    def init_db(self):
        os.makedirs("db/", exist_ok=True)
        self.connection = sqlite3.connect(f"db/{self.dbname}.db")
        self.cursor = self.connection.cursor()

        self.cursor.execute(
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
        self.connection.commit()

    def __init__(
        self,
        client: Client,
        wallets_path: str,
        dbname: str,
        to_send: Optional[int] = None,
        extended_message: bool = False,
    ):
        self.dbname = dbname
        self.init_db()
        self.client = client
        self.wallets_path = wallets_path
        self.to_send = to_send
        self.extended_message = extended_message
        self.wallets: List[WalletInfo] = []
        self.sent_count = 0

    async def init_client(self):
        if isinstance(self.client, LiteClient):
            await self.client.connect()
        elif isinstance(self.client, TonCenterClient):
            pass
        else:
            pass

    async def read_wallets(self):
        wallets = []
        with open(self.wallets_path, "r") as f:
            for line in f.readlines():
                addr, seed = line.split()
                seed = int(seed, 16)
                seed_bytes = seed.to_bytes(32, "big")
                public_key, private_key = keys.crypto_sign_seed_keypair(seed_bytes)
                pk_hex = "0x" + public_key.hex()
                wallets.append(WalletInfo(addr=addr, pk_hex=pk_hex, sk=private_key))
        self.wallets = wallets

    def add_new_tx(self, tx_id: int, addr: str) -> None:
        self.cursor.execute(
            "INSERT INTO txs (addr, utime, is_found) VALUES (?, ?, 0)",
            (addr, tx_id),
        )
        self.connection.commit()

    def get_missing_ids(self) -> List[str]:
        self.cursor.execute("SELECT addr, utime FROM txs WHERE is_found = 0")
        result = self.cursor.fetchall()
        missing_ids = []
        for addr, utime in result:
            missing_ids.append(f"{utime}:{addr}")
        return missing_ids

    def make_found(self, tx_full_id: str, executed_in: int, found_in: int) -> None:
        utime, addr = tx_full_id.split(":")
        self.cursor.execute(
            "UPDATE txs SET is_found = 1, executed_in = ?, found_in = ? WHERE addr = ? AND utime = ?",
            (executed_in, found_in, addr, utime),
        )
        self.connection.commit()

    async def get_seqno(self, address: str) -> int:
        if isinstance(self.client, TonCenterClient):
            return await self.client.seqno(address)
        elif isinstance(self.client, LiteClient):
            return (await self.client.run_get_method(address, "seqno", []))[0]
        else:
            return (await self.client.wallet.get_account_seqno(address)) or 0

    async def sendboc(self, boc: bytes):
        if isinstance(self.client, LiteClient):
            await self.client.raw_send_message(boc)
        elif isinstance(self.client, TonCenterClient):
            await self.client.send(boc)
        else:
            api_body = {"boc": base64.b64encode(boc).decode()}
            await self.client.blockchain.send_message(body=api_body)
            await asyncio.sleep(2)

    async def parse_and_add_msg(self, msg: Cell, blockutime: int, addr: str) -> bool:
        msg_slice = msg.begin_parse()
        msg_slice.skip_bits(512)  # signature
        msg_slice.skip_bits(32)  # seqno
        valid_until = msg_slice.load_uint(48)
        tx_id = valid_until - VALID_UNTIL_TIMEOUT  # get sending time
        tx_full_id = f"{tx_id}:{addr}"

        if tx_full_id in self.get_missing_ids():
            current = int(time.time())
            logger.info(
                f"{self.dbname}: Found tx: {tx_full_id} at {current}. Executed in {blockutime - tx_id} sec. Found in {current - tx_id} sec."
            )
            executed_in = blockutime - tx_id
            found_in = current - tx_id
            self.make_found(tx_full_id, executed_in, found_in)
            return True
        else:
            return False

    def extend_message_to_1kb(self, body: Builder):
        # writing some mash data
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
        return body

    async def send_tx_with_id(self, tx_utime: int, wdata: WalletInfo):
        seqno = await self.get_seqno(wdata.addr)

        relative_path = "logger-c5.fif"
        full_path = os.path.join(os.path.dirname(__file__), relative_path)
        new_code_hex = subprocess.check_output(
            ["fift", "-s", full_path, str(seqno + 1), wdata.pk_hex]
        ).decode()

        new_seqno_code = Cell.from_boc(new_code_hex)[0]
        action_set_code = (
            begin_cell().store_uint(0xAD4DE08E, 32).store_ref(new_seqno_code).end_cell()
        )
        actions = (
            begin_cell()
            .store_ref(begin_cell().end_cell())
            .store_slice(action_set_code.to_slice())
            .end_cell()
        )
        body = (
            begin_cell()
            .store_uint(seqno, 32)
            .store_uint(tx_utime + VALID_UNTIL_TIMEOUT, 48)
            .store_ref(actions)
        )
        if self.extended_message:
            body = self.extend_message_to_1kb(body)
        body = body.end_cell()
        signature = sign_message(body.hash, wdata.sk).signature
        signed_body = Builder().store_bytes(signature).store_cell(body).end_cell()
        addr = Address(wdata.addr)
        message = Wallet.create_external_msg(
            dest=addr,
            body=signed_body,
        )
        boc = message.serialize().to_boc()
        await self.sendboc(boc)
        self.add_new_tx(tx_utime, wdata.addr)
        tx_full_id = f"{tx_utime}:{wdata.addr}"
        self.sent_count += 1
        logger.info(f"{self.dbname}: Sent tx {tx_full_id}")

    async def start_sending(self):
        """Txs sender. Sends them every `SEND_INTERVAL` seconds to
        all the wallets specified in `self.wallets`."""
        while self.sent_count < (self.to_send or 100000000):  # 400 years by default
            for wdata in self.wallets:
                tx_id = int(time.time())
                try:
                    await self.send_tx_with_id(tx_id, wdata)
                except Exception as e:
                    logger.warning(
                        f"Failed to send tx with id {str(tx_id)} from wallet "
                        + f"{wdata.addr} error: {str(e)}"
                    )
            await asyncio.sleep(SEND_INTERVAL)

    async def watch_transactions(self):
        """Watches for sent tx to be shown up on wallets
        and adds them to found_tx_ids."""
        while True:
            try:
                for tx_id in self.get_missing_ids():
                    addr = tx_id.split(":")[1]
                    if isinstance(self.client, LiteClient):
                        txs = await self.client.get_transactions(addr, 3, from_lt=0)
                        for tx in txs:
                            if tx.in_msg is not None and tx.in_msg.is_external:
                                await self.parse_and_add_msg(
                                    tx.in_msg.body, tx.now, addr
                                )

                    elif isinstance(self.client, TonCenterClient):
                        txs = await self.client.get_transactions(addr, 3, from_lt=0)
                        for tx in txs:
                            if (
                                "in_msg" in tx
                                and tx["in_msg"]
                                # from nowhere:
                                and tx["in_msg"]["source"] == ""
                            ):
                                body_b64 = tx["in_msg"]["msg_data"]["body"]
                                body = Cell.from_boc(body_b64)[0]
                                await self.parse_and_add_msg(body, tx["utime"], addr)
                    else:
                        txs = await self.client.blockchain.get_account_transactions(
                            addr, limit=3
                        )
                        for tx in txs.transactions:
                            if tx.in_msg is not None and tx.in_msg.source is None:
                                body = tx.in_msg.raw_body
                                if isinstance(body, str):
                                    body = Cell.from_boc(body)[0]
                                    await self.parse_and_add_msg(body, tx.utime, addr)
                        await asyncio.sleep(2)
            except Exception as e:
                logger.warning(
                    f"{self.dbname}: watch_transactions failed, retrying: {str(e)}"
                )
            await asyncio.sleep(4)

    async def printer(self):
        """Prints some stats about sent txs every 5 sec."""
        while True:
            missing_ids = self.get_missing_ids()
            found = self.sent_count - len(missing_ids)
            rate = max(found, 0) / (self.sent_count or 1)
            logger.debug(
                f"{self.dbname}: Found/sent: {found}/{self.sent_count}, success rate: {rate}"
            )
            await asyncio.sleep(5)

    async def start_worker(self):
        """This thing inits database and starts txs sending with txs watching.
        It should be run for every provider and every db separately."""
        logger.warning(
            f"Starting worker to db/{self.dbname} of {self.to_send} txs using {self.wallets_path} wallets."
        )
        await self.init_client()
        await self.read_wallets()
        asyncio.create_task(self.watch_transactions())
        asyncio.create_task(self.printer())
        await self.start_sending()
        logger.info(f"\n{self.dbname}: Done sending {self.sent_count} txs")


# Usage example
async def main():
    load_dotenv()
    dbname = "ls"
    config = json.loads(open("configs/testnet.json").read())
    client = LiteClient.from_config(config, timeout=15)
    wallets_path = "mywallets/w-c5-1.txt"
    monitor = TransactionsMonitor(client, wallets_path, dbname)
    await monitor.start_worker()


if __name__ == "__main__":
    asyncio.run(main())
