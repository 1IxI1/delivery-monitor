import asyncio
import base64
import json
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
from pytoniq import LiteBalancer, begin_cell
from pytoniq.contract.wallets import Wallet
from pytoniq_core import Builder, StateInit
from pytoniq_core.boc import Address, Cell
from pytoniq_core.crypto import keys
from tonsdk.utils import sign_message

from client import TonCenterClient, TonCenterV3Client, TonCenterStreamingClient


@dataclass
class WalletInfo:
    addr: str
    sk: bytes
    pk_hex: str


@dataclass
class MsgInfo:
    addr: str
    utime: float
    msghash: str


VALID_UNTIL_TIMEOUT = 40
SEND_INTERVAL = 40

Client = Union[LiteBalancer, TonCenterClient, AsyncTonapi, TonCenterStreamingClient]


class TransactionsMonitor:
    dbname: str
    connection: sqlite3.Connection
    cursor: sqlite3.Cursor
    dbname_second: Optional[str] = None
    connection_second: Optional[sqlite3.Connection] = None
    cursor_second: Optional[sqlite3.Cursor] = None
    with_state_init: bool = False
    sender_client: Optional[TonCenterV3Client] = None  # for streaming mode

    @property
    def dbstr(self):
        dbstr = self.dbname
        if self.dbname_second:
            dbstr += "->" + self.dbname_second
        return dbstr

    def init_db(self, second_db: bool = False, with_state_init: bool = False):
        # if seconds db specified, we'll look (no send) for txs
        # in the first db and write to second when found
        os.makedirs("db/", exist_ok=True)
        query = """
            CREATE TABLE IF NOT EXISTS txs (
                addr TEXT,
                utime REAL,
                msghash STRING,
                is_found BOOLEAN,
                executed_in REAL,
                found_in REAL,
                commited_in REAL,
                sendboc_took REAL,
                -- streaming fields
                pending_tx_in REAL,
                pending_action_in REAL,
                confirmed_tx_in REAL,
                confirmed_action_in REAL,
                finalized_tx_in REAL,
                finalized_action_in REAL,
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
        sender_client: Optional[TonCenterV3Client] = None,
        with_state_init: bool = False,
        extra_msg_boc: Optional[str] = None,
        target_action_type: str = "unknown",
    ):
        self.dbname = dbname
        self.dbname_second = None
        self.connection_second = None
        self.cursor_second = None
        self.with_state_init = with_state_init
        self.extra_msg_boc = extra_msg_boc
        self.target_action_type = target_action_type
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
        self.sender_client = sender_client  # for streaming mode

    async def init_client(self):
        if isinstance(self.client, LiteBalancer):
            await self.client.start_up()

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

    def add_new_tx(self, msg: MsgInfo, sendboc_took: Optional[float] = None) -> None:
        # insert in first db
        self.cursor.execute(
            "INSERT INTO txs (addr, utime, msghash, is_found, sendboc_took) VALUES (?, ?, ?, 0, ?)",
            (msg.addr, msg.utime, msg.msghash, sendboc_took),
        )
        self.connection.commit()

    def get_missing_msgs(self) -> List[MsgInfo]:
        now = time.time()

        # second db may find msg later, when it's found in the first
        if self.dbname_second:
            is_found_filter = ""  # fetch all
        else:
            is_found_filter = "is_found = 0 AND"

        self.cursor.execute(
            f"SELECT addr, utime, msghash FROM txs WHERE {is_found_filter} utime > ?",
            (now - 1200,),  # 20 min ago or newer
        )
        result = self.cursor.fetchall()
        msgs = []
        for addr, utime, msghash in result:
            msgs.append(MsgInfo(addr=addr, utime=utime, msghash=msghash))

        if self.dbname_second and self.cursor_second:
            # get message hashes found in second DB in last 20 minutes
            self.cursor_second.execute(
                "SELECT msghash FROM txs WHERE is_found = 1 AND utime > ?",
                (now - 1200,),
            )
            found_hashes = {row[0] for row in self.cursor_second.fetchall()}

            # filter results, excluding already found hashes
            msgs = [msg for msg in msgs if msg.msghash not in found_hashes]

        return msgs

    def make_found(
        self, msg: MsgInfo, executed_in: float, found_in: float, commited_in: Optional[float]
    ) -> None:
        if not self.dbname_second:
            self.cursor.execute(
                "UPDATE txs SET is_found = 1, executed_in = ?, found_in = ?, commited_in = ? WHERE addr = ? AND utime = ?",
                (executed_in, found_in, commited_in, msg.addr, msg.utime),
            )
            self.connection.commit()
        else:
            if self.cursor_second is None or self.connection_second is None:
                raise RuntimeError("Secondary database is not initialized")
            self.cursor_second.execute(
                "INSERT OR IGNORE INTO txs (addr, utime, msghash, is_found, executed_in, found_in, commited_in) VALUES (?, ?, ?, 1, ?, ?, ?)",
                (msg.addr, msg.utime, msg.msghash, executed_in, found_in, commited_in),
            )
            self.connection_second.commit()

    async def get_seqno(self, address: str) -> int:
        if isinstance(self.client, TonCenterStreamingClient):
            # streaming client uses separate sender_client for seqno
            if self.sender_client:
                return await self.sender_client.get_seqno(address)
            raise RuntimeError("sender_client not set for streaming")
        elif isinstance(self.client, TonCenterV3Client):
            return await self.client.get_seqno(address)
        elif isinstance(self.client, TonCenterClient):
            return await self.client.seqno(address)
        elif isinstance(self.client, LiteBalancer):
            return (await self.client.run_get_method(address, "seqno", []))[0]
        else:
            res = await self.client.blockchain.execute_get_method(address, "seqno")
            if res.success is False:
                raise Exception(f"Error with tonapi get method: {res.exit_code}")
            return int(res.decoded["state"]) if res.decoded else 0

    async def sendboc(self, boc: bytes) -> str | None | dict:
        if isinstance(self.client, TonCenterStreamingClient):
            # streaming client uses separate sender_client for sending
            if self.sender_client:
                return await self.sender_client.send(boc)
            else:
                raise RuntimeError("sender_client not set for streaming")
        elif isinstance(self.client, TonCenterClient):  # v3 included
            return await self.client.send(boc)
        elif isinstance(self.client, LiteBalancer):
            await self.client.raw_send_message(boc)
        else:
            api_body = {"boc": base64.b64encode(boc).decode()}
            await self.client.blockchain.send_message(body=api_body)
            await asyncio.sleep(2)

    def insert_found_msg(
        self, msg_info: MsgInfo, blockutime: float, found_at: float, commited_at: float
    ) -> None:
        executed_in = blockutime - msg_info.utime
        found_in = found_at - msg_info.utime
        commited_in = commited_at - msg_info.utime if commited_at != 0 else None
        
        if commited_in is None:
            commited_str = "None"
        else:
            commited_str = f"{commited_in:.6f}"
            
        logger.success(
            f"{self.dbstr}: Found tx: {msg_info.utime:.6f}:{msg_info.addr}. Executed in {executed_in:.6f} sec. Found in {found_in:.6f} sec. Commited in {commited_str} sec."
        )
        self.make_found(msg_info, executed_in, found_in, commited_in)

    def update_streaming_field(self, msg: MsgInfo, field: str, value: float) -> bool:
        """update single streaming field for a message, only if not already set.
        enforces order: pending only if no confirmed/finalized, confirmed only if no finalized.
        also nullifies pending time if it arrived too close to confirmed/finalized (<0.1s).
        returns True if field was updated, False if skipped."""
        # build extra conditions for ordering
        extra_cond = ""
        pending_field = None  # field to nullify if too close
        if field == "pending_tx_in":
            extra_cond = " AND confirmed_tx_in IS NULL AND finalized_tx_in IS NULL"
        elif field == "pending_action_in":
            extra_cond = " AND confirmed_action_in IS NULL AND finalized_action_in IS NULL"
        elif field == "confirmed_tx_in":
            extra_cond = " AND finalized_tx_in IS NULL"
            pending_field = "pending_tx_in"
        elif field == "confirmed_action_in":
            extra_cond = " AND finalized_action_in IS NULL"
            pending_field = "pending_action_in"
        elif field == "finalized_tx_in":
            pending_field = "pending_tx_in"
        elif field == "finalized_action_in":
            pending_field = "pending_action_in"
        
        if not self.dbname_second:
            self.cursor.execute(
                f"UPDATE txs SET {field} = ? WHERE addr = ? AND utime = ? AND {field} IS NULL{extra_cond}",
                (value, msg.addr, msg.utime),
            )
            updated = self.cursor.rowcount > 0
            fixed_count = 0
            # nullify pending if it arrived too close (<0.1s)
            if updated and pending_field:
                self.cursor.execute(
                    f"UPDATE txs SET {pending_field} = NULL WHERE addr = ? AND utime = ? AND {pending_field} IS NOT NULL AND (? - {pending_field}) < 0.1",
                    (msg.addr, msg.utime, value),
                )
                fixed_count = self.cursor.rowcount
            self.connection.commit()
            if fixed_count > 0:
                logger.warning(f"{self.dbstr}: Nullified {fixed_count} of {pending_field} for {msg.addr}:{msg.utime}")
            return updated
        else:
            if self.cursor_second is None or self.connection_second is None:
                raise RuntimeError("secondary database is not initialized")
            # for second db, insert then update with conditions
            self.cursor_second.execute(
                f"""INSERT INTO txs (addr, utime, msghash, is_found, {field}) 
                    VALUES (?, ?, ?, 0, ?)
                    ON CONFLICT(addr, utime) DO UPDATE SET {field} = ? 
                    WHERE {field} IS NULL{extra_cond}""",
                (msg.addr, msg.utime, msg.msghash, value, value),
            )
            updated = self.cursor_second.rowcount > 0
            # nullify pending if it arrived too close (<0.1s)
            if updated and pending_field:
                self.cursor_second.execute(
                    f"UPDATE txs SET {pending_field} = NULL WHERE addr = ? AND utime = ? AND {pending_field} IS NOT NULL AND (? - {pending_field}) < 0.1",
                    (msg.addr, msg.utime, value),
                )
            self.connection_second.commit()
            return updated

    def mark_streaming_found(self, msg: MsgInfo) -> None:
        """mark message as found in streaming mode"""
        if not self.dbname_second:
            self.cursor.execute(
                "UPDATE txs SET is_found = 1 WHERE addr = ? AND utime = ?",
                (msg.addr, msg.utime),
            )
            self.connection.commit()
        else:
            if self.cursor_second is None or self.connection_second is None:
                raise RuntimeError("secondary database is not initialized")
            self.cursor_second.execute(
                "UPDATE txs SET is_found = 1 WHERE addr = ? AND utime = ?",
                (msg.addr, msg.utime),
            )
            self.connection_second.commit()

    def get_streaming_missing_msgs(self) -> List[MsgInfo]:
        """get messages not yet fully tracked by streaming"""
        now = time.time()
        # for streaming we track until finalized_tx_in and finalized_action_in are set
        if self.dbname_second:
            is_found_filter = ""
        else:
            is_found_filter = "is_found = 0 AND"
        
        self.cursor.execute(
            f"SELECT addr, utime, msghash FROM txs WHERE {is_found_filter} utime > ?",
            (now - 1200,),
        )
        result = self.cursor.fetchall()
        msgs = []
        for addr, utime, msghash in result:
            msgs.append(MsgInfo(addr=addr, utime=utime, msghash=msghash))
        
        if self.dbname_second and self.cursor_second:
            self.cursor_second.execute(
                "SELECT msghash FROM txs WHERE is_found = 1 AND utime > ?",
                (now - 1200,),
            )
            found_hashes = {row[0] for row in self.cursor_second.fetchall()}
            msgs = [msg for msg in msgs if msg.msghash not in found_hashes]
        
        return msgs

    async def watch_transactions_streaming(self):
        """watch for transactions via websocket streaming api"""
        if not isinstance(self.client, TonCenterStreamingClient):
            raise RuntimeError("client is not TonCenterStreamingClient")
        
        reconnect_delay = 1
        max_reconnect_delay = 30
        
        while True:
            try:
                # connect to websocket
                if not await self.client.connect():
                    logger.warning(f"{self.dbstr}: connection failed, retrying in {reconnect_delay}s")
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                    continue
                
                reconnect_delay = 1  # reset on successful connect
                
                # get addresses to subscribe
                missing = self.get_streaming_missing_msgs()
                if not missing:
                    for _ in range(1000):
                        await asyncio.sleep(0.01)
                        missing = self.get_streaming_missing_msgs()
                        if missing:
                            break
                    if not missing:
                        logger.warning(f"{self.dbstr}: still no missing messages, sleeping and unsubscribing")
                        await asyncio.sleep(1)
                        await self.client.close()
                        continue
                
                addresses = list(set(m.addr for m in missing))
                
                if not await self.client.subscribe(addresses, ["transactions", "actions"]):
                    logger.error(f"{self.dbstr}: subscribe failed")
                    await self.client.close()
                    continue
                
                # track finalized state for each message
                msg_tracking: dict[str, dict[str, bool]] = {}
                for m in missing:
                    msg_tracking[m.msghash] = {
                        "finalized_tx": False,
                        "finalized_action": False,
                    }
                
                async def on_event(data: dict):
                    event_type = data.get("type")
                    finality = data.get("finality")
                    if not event_type or not finality:
                        return
                                        
                    # type is "transactions" or "actions", finality is "pending"/"confirmed"/"finalized"
                    field = None
                    if event_type == "transactions":
                        if finality == "pending":
                            field = "pending_tx_in"
                        elif finality == "confirmed":
                            field = "confirmed_tx_in"
                        elif finality == "finalized":
                            field = "finalized_tx_in"
                    elif event_type == "actions":
                        if finality == "pending":
                            field = "pending_action_in"
                        elif finality == "confirmed":
                            field = "confirmed_action_in"
                        elif finality == "finalized":
                            field = "finalized_action_in"
                    
                    if not field:
                        return
                    
                    # match by msghash in transactions or trace_external_hash_norm in actions
                    for m in self.get_streaming_missing_msgs():
                        matched = False
                        
                        if event_type == "transactions":
                            for tx in data.get("transactions", []):
                                tx_hash = tx.get("in_msg", {}).get("hash_norm")
                                if tx_hash == m.msghash:
                                    matched = True
                                    break
                        
                        elif event_type == "actions":
                            trace_hash = data.get("trace_external_hash_norm")
                            if trace_hash == m.msghash:
                                for action in data.get("actions", []):
                                    action_type = action.get("type")
                                    if action_type == self.target_action_type:
                                        matched = True
                                        break
                                else:
                                    logger.warning(f"{self.dbstr}: No action type {self.target_action_type} found in actions, but trace hash matches")
                        
                        if matched:
                            time_in = time.time() - m.utime
                            updated = self.update_streaming_field(m, field, time_in)
                            
                            # only log and track if actually updated (skip duplicates)
                            if not updated:
                                continue
                            
                            logger.info(
                                f"{self.dbstr}: {event_type}/{finality} for {m.msghash[:16]}... "
                                f"in {time_in:.3f}s"
                            )
                            
                            # add to tracking if not exists (new tx sent after connect)
                            if m.msghash not in msg_tracking:
                                msg_tracking[m.msghash] = {
                                    "finalized_tx": False,
                                    "finalized_action": False,
                                }
                            
                            # track finalized state
                            if field == "finalized_tx_in":
                                msg_tracking[m.msghash]["finalized_tx"] = True
                            elif field == "finalized_action_in":
                                msg_tracking[m.msghash]["finalized_action"] = True
                            
                            # mark as found when both finalized
                            if msg_tracking[m.msghash]["finalized_tx"] and msg_tracking[m.msghash]["finalized_action"]:
                                self.mark_streaming_found(m)
                                logger.success(f"{self.dbstr}: Found finalized tx {m.msghash}")
                
                # listen for events
                await self.client.listen(on_event)
                
            except Exception as e:
                logger.warning(f"{self.dbstr}: streaming error: {e}, reconnecting...")
            finally:
                await self.client.close()
            
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

    async def parse_and_add_msg(
        self, msg: Cell, found_at: float, blockutime: float, commited_at: float, addr: str
    ) -> bool:
        """Check msg validity and add it to found_tx_ids"""
        msg_slice = msg.begin_parse()
        msg_slice.skip_bits(512)  # signature
        msg_slice.skip_bits(32)  # seqno
        valid_until = msg_slice.load_uint(48)
        msg_sent_at = valid_until - VALID_UNTIL_TIMEOUT  # get sending time

        for i in self.get_missing_msgs():
            if int(i.utime) == msg_sent_at and i.addr == addr:
                self.insert_found_msg(i, blockutime, found_at, commited_at)
                return True
        return False

    def check_msg(
        self,
        msg_addr: str,
        msg_body: Cell,
        missing_msg_addr: str,
        missing_msg_utime: float,
    ) -> bool:
        body_slice = msg_body.begin_parse()
        body_slice.skip_bits(512)  # signature
        body_slice.skip_bits(32)  # seqno
        valid_until = body_slice.load_uint(48)
        msg_sent_at = valid_until - VALID_UNTIL_TIMEOUT  # get sending time
        return int(missing_msg_utime) == msg_sent_at and missing_msg_addr == msg_addr

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

    async def prepare_and_send_to_wallet(self, wdata: WalletInfo):
        """runs get method, builds code, packs message, sends it to wallet, writes to db"""
        try:
            t_seqno = time.time()
            seqno = await self.get_seqno(wdata.addr)
            t_seqno = time.time() - t_seqno
            logger.debug(f"{self.dbstr}: get_seqno took {int(t_seqno*1000)}ms")
        except Exception as e:
            if self.with_state_init: # Deploying with seqno 0
                logger.warning(f"{self.dbstr}: Failed to get seqno for {wdata.addr}: {e}. Deploying with seqno 0.")
                seqno = 0
            else:
                logger.error(f"{self.dbstr}: Failed to get seqno for {wdata.addr}: {e}")
                return

        # compile new code
        relative_path = "logger-c5.fif"
        full_path = os.path.join(os.path.dirname(__file__), relative_path)
        t_compile = time.time()
        new_code_hex = subprocess.check_output(
            ["fift", "-s", full_path, str(seqno + 1), wdata.pk_hex]
        ).decode()
        t_compile = time.time() - t_compile
        logger.debug(f"{self.dbstr}: compile took {int(t_compile*1000)}ms")

        # pack actions
        t_serialize = time.time()
        new_seqno_code = Cell.from_boc(new_code_hex)[0]
        # action_set_code#ad4de08e new_code:^Cell
        action_set_code = (
            begin_cell().store_uint(0xAD4DE08E, 32).store_ref(new_seqno_code).end_cell()
        )
        
        if self.extra_msg_boc:
            # build OutList with send_msg + set_code
            extra_msg_cell = Cell.from_boc(self.extra_msg_boc)[0]
            # action_send_msg#0ec3c86d mode:(## 8) out_msg:^(MessageRelaxed Any)
            action_send_msg = (
                begin_cell()
                .store_uint(0x0ec3c86d, 32)
                .store_uint(3, 8)  # mode = 3
                .store_ref(extra_msg_cell)
                .end_cell()
            )
            # OutList 1: prev=empty, action=send_msg
            out_list_1 = (
                begin_cell()
                .store_ref(begin_cell().end_cell())
                .store_slice(action_send_msg.to_slice())
                .end_cell()
            )
            # OutList 2: prev=OutList1, action=set_code
            actions = (
                begin_cell()
                .store_ref(out_list_1)
                .store_slice(action_set_code.to_slice())
                .end_cell()
            )
        else:
            # OutList 1: prev=empty, action=set_code only
            actions = (
                begin_cell()
                .store_ref(begin_cell().end_cell())
                .store_slice(action_set_code.to_slice())
                .end_cell()
            )

        # make a signature of `valid_until * seqno`
        valid_until = int(time.time()) + VALID_UNTIL_TIMEOUT
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

        state_init = None
        if self.with_state_init and seqno == 0:
            logger.warning(f"{self.dbstr}: Making state init for {wdata.addr} with seqno 0.")
            old_code_hex = subprocess.check_output(
                ["fift", "-s", full_path, str(seqno), wdata.pk_hex]
            ).decode()
            old_code = Cell.from_boc(old_code_hex)[0]
            state_init = StateInit(code=old_code, data=begin_cell().end_cell())

        # make msg
        addr = Address(wdata.addr)
        message = Wallet.create_external_msg(
            dest=addr,
            body=body,
            state_init=state_init,
        )

        # send msg and save normalized hash
        msg_cell = message.serialize()
        t_serialize = time.time() - t_serialize
        logger.debug(f"{self.dbstr}: serializing took {int(t_serialize*1000)}ms")
        
        t_sendboc = time.time()
        probably_hash = await self.sendboc(msg_cell.to_boc())
        sendboc_took = time.time() - t_sendboc
        logger.debug(f"{self.dbstr}: sendboc took {int(sendboc_took*1000)}ms")
        if isinstance(probably_hash, str):
            logger.critical(f"returned hash: {probably_hash}, local hash: {base64.urlsafe_b64encode(msg_cell.hash).decode()}")
            norm_hash_str = probably_hash
        else:
            # normalize like here https://github.com/tonkeeper/tongo/pull/313/files
            logger.critical(f"before normalization hash: {base64.urlsafe_b64encode(msg_cell.hash).decode()}")
            msg_cell = (begin_cell()
                        .store_cell(message.info.serialize())
                        .store_bool(False) # no state_init
                        .store_bool(True) # body in ref
                        .store_ref(message.body)
                        .end_cell())
            logger.critical(f"after normalization hash: {base64.urlsafe_b64encode(msg_cell.hash).decode()}")
            norm_hash_str = base64.urlsafe_b64encode(msg_cell.hash).decode()

        # add to db and log
        self.add_new_tx(MsgInfo(addr=wdata.addr, utime=t_sendboc, msghash=norm_hash_str), sendboc_took=sendboc_took)
        self.sent_count += 1
        logger.info(f"{self.dbstr}: Sent tx with seqno {seqno}")
        logger.debug(
            f"{self.dbstr}: Tx details - hash: {norm_hash_str}, valid_until: {valid_until}, addr: {wdata.addr}, utime: {t_sendboc:.6f}"
        )

    async def start_sending(self):
        """Txs sender. Sends them every `SEND_INTERVAL` seconds to
        all the wallets specified in `self.wallets`."""
        while self.sent_count < (self.to_send or 100000000):  # 400 years by default
            for wdata in self.wallets:
                try:
                    await self.prepare_and_send_to_wallet(wdata)
                except Exception as e:
                    logger.warning(
                        f"{self.dbstr}: Failed to send tx to wallet "
                        + f"{wdata.addr} error: {str(e)}"
                    )
            await asyncio.sleep(SEND_INTERVAL)

    async def watch_transactions(self):
        """Watches for sent tx to be shown up on wallets
        and adds them to found_tx_ids."""
        # streaming client has its own watch method
        if isinstance(self.client, TonCenterStreamingClient):
            return
        
        while True:
            try:
                # go for possible multiple addresses
                # it's rarely to have 1+ missings found in one request
                for missing in self.get_missing_msgs():
                    addr = missing.addr

                    if isinstance(self.client, LiteBalancer):
                        txs = await self.client.get_transactions(
                            addr, count=2, from_lt=0
                        )
                        for tx in txs:
                            if tx.in_msg is not None and tx.in_msg.is_external:
                                if not self.check_msg(
                                    addr, tx.in_msg.body, missing.addr, missing.utime
                                ):
                                    continue
                                found_at = time.time()
                                # get mc block time
                                commited_at = 0.0
                                try:
                                    shardblock = await self.client.lookup_block(
                                        0, -1, -1, lt=tx.lt
                                    )
                                    our_shard = shardblock[0].shard
                                    our_seqno = shardblock[0].seqno

                                    start_mc_seqno = (
                                        shardblock[1].info.min_ref_mc_seqno + 1
                                    )
                                    for seqno in range(
                                        start_mc_seqno, start_mc_seqno + 20
                                    ):
                                        try:
                                            mc_block = await self.client.lookup_block(
                                                wc=-1, shard=-1, seqno=seqno
                                            )
                                        except:
                                            break  # may have no some i-th block from future
                                        else:
                                            shards = (
                                                await self.client.get_all_shards_info(
                                                    mc_block[0]
                                                )
                                            )
                                            for shard in shards:
                                                if (
                                                    shard.shard == our_shard
                                                    and shard.seqno == our_seqno
                                                ):
                                                    commited_at = float(mc_block[
                                                        1
                                                    ].info.gen_utime)
                                                    break
                                except Exception as e:
                                    logger.warning(
                                        f"Failed to get mc block time for {missing.utime:.6f}:{addr}: {e}"
                                    )
                                self.insert_found_msg(
                                    missing, float(tx.now), found_at, commited_at
                                )

                    elif isinstance(self.client, TonCenterV3Client):
                        txs = await self.client.get_transaction_by_hash(missing.msghash)
                        if len(txs["transactions"]) > 0:
                            tx = txs["transactions"][0]
                            body_b64 = tx["in_msg"]["message_content"]["body"]
                            body = Cell.from_boc(body_b64)[0]
                            found_at = time.time()
                            # get mc block time
                            commited_at = 0.0
                            try:
                                blocks = await self.client.get_blocks(
                                    wc=-1, seqno=tx["mc_block_seqno"], limit=1
                                )
                                block = blocks["blocks"][0]
                                commited_at = float(block["gen_utime"])
                            except Exception as e:
                                logger.warning(
                                    f"Failed to get mc block time for {missing.utime:.6f}:{addr}: {e}"
                                )
                            self.insert_found_msg(
                                missing, float(tx["now"]), found_at, commited_at
                            )

                    elif isinstance(self.client, TonCenterClient):
                        txs = await self.client.get_transactions(
                            addr, limit=3, from_lt=0
                        )
                        for tx in txs:
                            if (
                                "in_msg" in tx
                                and tx["in_msg"]
                                # from nowhere:
                                and tx["in_msg"]["source"] == ""
                            ):
                                body_b64 = tx["in_msg"]["msg_data"]["body"]
                                body = Cell.from_boc(body_b64)[0]
                                if not self.check_msg(
                                    addr, body, missing.addr, missing.utime
                                ):
                                    continue
                                found_at = time.time()
                                # it's complicated to get mc block time
                                commited_at = 0.0
                                self.insert_found_msg(
                                    missing, float(tx["utime"]), found_at, commited_at
                                )

                    else:
                        txs = await self.client.blockchain.get_account_transactions(
                            addr, limit=3
                        )
                        for tx in txs.transactions:
                            if tx.in_msg is not None and tx.in_msg.source is None:
                                body = tx.in_msg.raw_body
                                if isinstance(body, str):
                                    body = Cell.from_boc(body)[0]
                                    if not self.check_msg(
                                        addr, body, missing.addr, missing.utime
                                    ):
                                        continue
                                    found_at = time.time()
                                    # get mc block time
                                    # await asyncio.sleep(1)  # rate limit
                                    commited_at = 0.0
                                    shardblock = tx.block
                                    blocks_after_tx = (
                                        await self.client.blockchain.get_reduced_blocks(
                                            tx.utime, tx.utime + 30
                                        )
                                    )
                                    for block in blocks_after_tx.blocks:
                                        if (
                                            block.workchain_id == -1
                                            and shardblock in block.shards_blocks
                                        ):
                                            commited_at = float(block.utime)
                                            break
                                    if commited_at == 0:
                                        logger.warning(
                                            f"{self.dbstr}: Failed to get mc block time for {missing.utime:.6f}:{addr}"
                                        )
                                    self.insert_found_msg(
                                        missing, float(tx.utime), found_at, commited_at
                                    )
                        # await asyncio.sleep(2)  # for tonapi rate limit

            except Exception as e:
                logger.warning(
                    f"{self.dbstr}: watch_transactions failed, retrying: {str(e)}"
                )
            await asyncio.sleep(0.2)

    async def printer(self):
        """Prints some stats about sent txs every 5 sec."""
        while True:
            missing_ids = self.get_missing_msgs()
            found = self.sent_count - len(missing_ids)
            rate = max(found, 0) / (self.sent_count or 1)
            logger.debug(
                f"{self.dbstr}: Found/sent: {found}/{self.sent_count}, success rate: {rate:.6f}"
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
        
        # choose watch method based on client type
        if isinstance(self.client, TonCenterStreamingClient):
            asyncio.create_task(self.watch_transactions_streaming())
        else:
            asyncio.create_task(self.watch_transactions())
        
        asyncio.create_task(self.printer())
        if not self.dbname_second:  # read sended txs from first
            await self.start_sending()
        else:
            while True:
                await asyncio.sleep(1)
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
