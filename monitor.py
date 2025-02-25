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
from pytoniq import LiteBalancer, begin_cell
from pytoniq.contract.wallets import Wallet
from pytoniq_core import Builder, StateInit
from pytoniq_core.boc import Address, Cell
from pytoniq_core.crypto import keys
from tonsdk.utils import sign_message

from client import TonCenterClient, TonCenterV3Client


@dataclass
class WalletInfo:
    addr: str
    sk: bytes
    pk_hex: str


@dataclass
class MsgInfo:
    addr: str
    utime: int
    msghash: str


VALID_UNTIL_TIMEOUT = 60
SEND_INTERVAL = 240

Client = Union[LiteBalancer, TonCenterClient, AsyncTonapi]


class TransactionsMonitor:
    dbname: str
    connection: sqlite3.Connection
    cursor: sqlite3.Cursor
    dbname_second: Optional[str] = None
    connection_second: Optional[sqlite3.Connection] = None
    cursor_second: Optional[sqlite3.Cursor] = None

    @property
    def dbstr(self):
        dbstr = self.dbname
        if self.dbname_second:
            dbstr += " -> " + self.dbname_second
        return dbstr

    def init_db(self, second_db: bool = False):
        # if seconds db specified, we'll look (no send) for txs
        # in the first db and write to second when found
        os.makedirs("db/", exist_ok=True)
        query = """
            CREATE TABLE IF NOT EXISTS txs (
                addr TEXT,
                utime INTEGER,
                msg_hash STRING
                is_found BOOLEAN,
                executed_in INTEGER,
                found_in INTEGER,
                PRIMARY KEY (addr, utime)
            )
            """
        if not second_db:
            self.connection = sqlite3.connect(f"db/{self.dbname}.db")
            self.cursor = self.connection.cursor()
            self.cursor.execute(query)
            self.connection.commit()
        else:
            self.connection_second = sqlite3.connect(f"db/{self.dbname_second}.db")
            self.cursor_second = self.connection_second.cursor()
            self.cursor_second.execute(query)
            self.connection_second.commit()

    def __init__(
        self,
        client: Client,
        wallets_path: str,
        dbname: str,
        dbname_second: Optional[str] = None,
        to_send: Optional[int] = None,
        extended_message: bool = False,
    ):
        self.dbname = dbname
        self.dbname_second = None
        self.connection_second = None
        self.cursor_second = None
        self.init_db()
        if dbname_second:
            self.dbname_second = dbname_second
            self.init_db(second_db=True)
        self.client = client
        self.wallets_path = wallets_path
        self.to_send = to_send
        self.extended_message = extended_message
        self.wallets: List[WalletInfo] = []
        self.sent_count = 0

    async def init_client(self):
        if isinstance(self.client, LiteBalancer):
            await self.client.start_up()
        elif isinstance(self.client, TonCenterClient):
            pass
        elif isinstance(self.client, TonCenterV3Client):
            pass
        elif isinstance(self.client, AsyncTonapi):
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

    def add_new_tx(self, msg: MsgInfo) -> None:
        # insert in first db
        self.cursor.execute(
            "INSERT INTO txs (addr, utime, msghash, is_found) VALUES (?, ?, ?, 0)",
            (msg.addr, msg.utime, msg.msghash),
        )
        self.connection.commit()

    def get_missing_msgs(self) -> List[MsgInfo]:
        self.cursor.execute(
            "SELECT addr, utime, msghash FROM txs WHERE is_found = 0 AND utime > ?",
            (int(time.time()) - 1200,),  # 20 min ago or newer
        )
        result = self.cursor.fetchall()
        msgs = []
        for addr, utime, msghash in result:
            msgs.append(MsgInfo(addr=addr, utime=utime, msghash=msghash))
        return msgs

    def make_found(self, msg: MsgInfo, executed_in: int, found_in: int) -> None:
        if not self.dbname_second:
            self.cursor.execute(
                "UPDATE txs SET is_found = 1, executed_in = ?, found_in = ? WHERE addr = ? AND utime = ?",
                (executed_in, found_in, msg.addr, msg.utime),
            )
            self.connection.commit()
        else:
            if self.cursor_second is None or self.connection_second is None:
                raise RuntimeError("Secondary database is not initialized")
            self.cursor_second.execute(
                "INSERT INTO txs (addr, utime, msghash, is_found, executed_in, found_in) VALUES (?, ?, ?, 1, ?, ?)",
                (msg.addr, msg.utime, msg.msghash, executed_in, found_in),
            )
            self.connection_second.commit()

    async def get_seqno(self, address: str) -> int:
        if isinstance(self.client, TonCenterClient) or isinstance(
            self.client, TonCenterV3Client
        ):
            return await self.client.seqno(address)
        elif isinstance(self.client, LiteBalancer):
            return (await self.client.run_get_method(address, "seqno", []))[0]
        else:
            res = await self.client.blockchain.execute_get_method(address, "seqno")
            if res.success is False:
                raise Exception(f"Error with tonapi get method: {res.exit_code}")
            return int(res.decoded["state"]) if res.decoded else 0

    async def sendboc(self, boc: bytes):
        if isinstance(self.client, TonCenterClient) or isinstance(
            self.client, TonCenterV3Client
        ):
            await self.client.send(boc)
        elif isinstance(self.client, LiteBalancer):
            await self.client.raw_send_message(boc)
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

        for i in self.get_missing_msgs():
            if i.msghash == base64.b64encode(msg.hash) and i.addr == addr:
                msg_info = i
                break
        else:
            return False

        current = int(time.time())
        logger.info(
            f"{self.dbstr}: Found tx: {msg_info.utime}:{addr} at {current}. Executed in {blockutime - tx_id} sec. Found in {current - tx_id} sec."
        )
        executed_in = blockutime - tx_id
        found_in = current - tx_id
        self.make_found(msg_info, executed_in, found_in)
        return True

    def extend_message_to_1kb(self, body: Builder):
        # writing some misc data
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

        # compile new code
        relative_path = "logger-c5.fif"
        full_path = os.path.join(os.path.dirname(__file__), relative_path)
        new_code_hex = subprocess.check_output(
            ["fift", "-s", full_path, str(seqno + 1), wdata.pk_hex]
        ).decode()

        # pack set_code action
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

        # make a signature of `valid_until * seqno`
        valid_until = tx_utime + VALID_UNTIL_TIMEOUT
        stamp = valid_until * seqno
        stamp_bytes = stamp.to_bytes(32, "big")
        signature = sign_message(stamp_bytes, wdata.sk).signature

        # build msg body
        body = (
            begin_cell()
            .store_bytes(signature)
            .store_uint(seqno, 32)
            .store_uint(valid_until, 48)
            .store_ref(actions)
        )
        if self.extended_message:
            body = self.extend_message_to_1kb(body)
        body = body.end_cell()

        # make msg
        addr = Address(wdata.addr)
        message = Wallet.create_external_msg(
            dest=addr,
            body=body,
        )

        # send msg and save hash
        msg_cell = message.serialize()
        hashstr = base64.b64encode(msg_cell.hash).decode()
        await self.sendboc(msg_cell.to_boc())

        # add to db and log
        self.add_new_tx(MsgInfo(addr=wdata.addr, utime=tx_utime, msghash=hashstr))
        self.sent_count += 1
        logger.info(f"{self.dbstr}: Sent tx with seqno {seqno}")
        logger.debug(f"{self.dbstr}: Tx details - hash: {hashstr}, valid_until: {valid_until}, addr: {wdata.addr}, utime: {tx_utime}")

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
                # go for possible multiple addresses
                # it's rarely to have 1+ missings found in one request
                for missing in self.get_missing_msgs():
                    addr = missing.addr

                    if isinstance(self.client, LiteBalancer):
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

                    elif isinstance(self.client, TonCenterV3Client):
                        txs = await self.client.get_transaction_by_hash(missing.msghash)
                        for tx in txs:
                            from pprint import pprint
                            pprint(tx)
                            if (
                                "in_msg" in tx
                                and tx["in_msg"]
                                # from nowhere:
                                and (
                                    "source" not in tx["in_msg"]
                                    or tx["in_msg"]["source"] == ""
                                )
                            ):
                                body_b64 = tx["in_msg"]["msg_content"]["body"]
                                body = Cell.from_boc(body_b64)[0]
                                print("here")
                                await self.parse_and_add_msg(body, tx["now"], addr)

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
                    f"{self.dbstr}: watch_transactions failed, retrying: {str(e)}"
                )
            await asyncio.sleep(4)

    async def printer(self):
        """Prints some stats about sent txs every 5 sec."""
        while True:
            missing_ids = self.get_missing_msgs()
            found = self.sent_count - len(missing_ids)
            rate = max(found, 0) / (self.sent_count or 1)
            logger.debug(
                f"{self.dbstr}: Found/sent: {found}/{self.sent_count}, success rate: {rate}"
            )
            await asyncio.sleep(5)

    async def start_worker(self):
        """This thing inits database and starts txs sending with txs watching.
        It should be run for every provider and every db separately."""
        logger.warning(
            f"Starting worker {self.dbstr} of {self.to_send} txs using {self.wallets_path} wallets."
        )
        await self.init_client()
        await self.read_wallets()
        asyncio.create_task(self.watch_transactions())
        asyncio.create_task(self.printer())
        if not self.dbname_second:  # read sended txs from first
            await self.start_sending()
        logger.info(f"\n{self.dbstr}: Done sending {self.sent_count} txs")


# example
async def main():
    load_dotenv()
    dbname = "ls"
    config = json.loads(open("configs/mainnet.json").read())
    client = LiteBalancer.from_config(config, timeout=15)
    wallets_path = "mywallets/w-c5-1.txt"
    monitor = TransactionsMonitor(client, wallets_path, dbname)
    await monitor.start_worker()


if __name__ == "__main__":
    asyncio.run(main())
