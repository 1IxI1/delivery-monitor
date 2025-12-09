import asyncio
import base64
import json
import time
from abc import ABC, abstractmethod
from typing import Callable, Optional

import aiohttp
import websockets
from loguru import logger
from tonsdk.boc import Cell
from tonsdk.provider import ToncenterClient, address_state, prepare_address
from tonsdk.utils import TonCurrencyEnum, from_nano
from tvm_valuetypes import serialize_tvm_stack


class AbstractTonClient(ABC):
    provider: ToncenterClient

    @abstractmethod
    async def _run(self, to_run, *, single_query=True):
        raise NotImplementedError

    async def get_address_information(
        self, address: str, currency_to_show: TonCurrencyEnum = TonCurrencyEnum.ton
    ):
        return (await self.get_addresses_information([address], currency_to_show))[0]

    async def get_addresses_information(
        self, addresses, currency_to_show: TonCurrencyEnum = TonCurrencyEnum.ton
    ):
        if not addresses:
            return []

        tasks = []
        for address in addresses:
            address = prepare_address(address)
            tasks.append(self.provider.raw_get_account_state(address))

        results = await self._run(tasks, single_query=False)

        for result in results:
            result["state"] = address_state(result)
            if "balance" in result:
                if int(result["balance"]) < 0:
                    result["balance"] = 0
                else:
                    result["balance"] = from_nano(
                        int(result["balance"]), currency_to_show
                    )

        return results

    async def seqno(self, addr: str):
        addr = prepare_address(addr)
        result = await self._run(self.provider.raw_run_method(addr, "seqno", []))

        if "@type" in result and result["@type"] == "smc.runResult":
            result["stack"] = serialize_tvm_stack(result["stack"])

        return int(result[0]["stack"][0][1], 16)

    async def send_boc(self, boc: Cell):
        return await self._run(self.provider.raw_send_message(boc))


class TonCenterClient(AbstractTonClient):
    def __init__(self, api_url: str, api_key: str):
        self.loop = asyncio.get_event_loop()
        self.provider = ToncenterClient(base_url=api_url, api_key=api_key)

    async def _run(self, to_run, *, single_query=True):
        return await self.__execute(to_run, single_query)

    async def __execute(self, to_run, single_query):
        timeout = aiohttp.ClientTimeout(total=5)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            if single_query:
                to_run = [to_run]

            tasks = []
            for task in to_run:
                tasks.append(task["func"](session, *task["args"], **task["kwargs"]))

            return await asyncio.gather(*tasks)

    async def call(self, contract_address: str, method: str, stack: list) -> dict:
        """
        Run contract's get method.

        Returns stack dictionary like:

        {'@extra': '1678643876',
         '@type': 'smc.runResult',
         'exit_code': 0,
         'gas_used': 3918,
         'stack': [['cell',
                    {'bytes': 'te6cckEBAQA...2C8Hn',
                     'object': {'data': {'b64': 'gAs4wlP...dUdIA==',
                                         'len': 267},
                                'refs': []}}]]}

        See examples/get_methods.py for more details.
        """
        query = self.provider.raw_run_method(contract_address, method, stack)

        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            r = await query["func"](session, *query["args"], **query["kwargs"])
            return r

    async def get_transactions(
        self,
        address: str,
        limit: int = 10,
        from_lt: int = 0,
        to_lt: int = 0,
        archival: bool = True,
    ):
        """
        Get transaction history of a given address.
        """
        q = self.provider.raw_get_account_state(address)
        async with aiohttp.ClientSession() as session:
            params = {
                "address": address,
                "limit": limit,
                "lt": from_lt,
                "to_lt": to_lt,
                "archival": archival,
            }
            params = {k: v for k, v in params.items() if v is not None}

            r = await q["func"](session, "getTransactions", params=params)
            return r

    async def lookup_block(
        self,
        wc: int = 0,
        shard: int | None = None,
        seqno: int | None = None,
        lt: int | None = None,
        unixtime: int | None = None,
    ):
        q = self.provider.raw_get_account_state("")  # just for getting base url
        async with aiohttp.ClientSession() as session:
            params = {
                "workchain": wc,
                "shard": shard,
                "seqno": seqno,
                "lt": lt,
                "unixtime": unixtime,
            }
            params = {k: v for k, v in params.items() if v is not None}

            r = await q["func"](session, "lookupBlock", params=params)
            return r

    async def get_shards(self, seqno: int):
        q = self.provider.raw_get_account_state("")  # just for getting base url
        async with aiohttp.ClientSession() as session:
            r = await q["func"](
                session,
                "shards",
                params={"seqno": seqno},
            )
            return r

    async def send(self, boc: bytes):
        q = self.provider.raw_send_message(boc)

        async with aiohttp.ClientSession() as session:
            r = await q["func"](session, *q["args"], **q["kwargs"])
            return r


class TonCenterV3Client(TonCenterClient):
    def __init__(self, api_url: str, api_key: str):
        super().__init__(api_url, api_key)
        self._last_ping_v3: Optional[float] = None

    async def get_transaction_by_hash(self, msg_hash: str):
        async with aiohttp.ClientSession() as session:
            params = {"msg_hash": msg_hash}
            r = await session.get(
                f"{self.provider.base_url}transactionsByMessage",
                params=params,
                headers={
                    "X-API-Key": self.provider.api_key,
                    "Content-Type": "application/json",
                    "accept": "application/json",
                },
            )
            response = await r.json()
            return response

    async def get_blocks(
        self,
        wc: int = 0,
        shard: int | None = None,
        seqno: int | None = None,
        limit: int = 10,
    ):
        """Only some of params yet implemented, just for this monitoring"""
        start_time = time.time()
        async with aiohttp.ClientSession() as session:
            params = {"workchain": wc, "shard": shard, "seqno": seqno, "limit": limit}
            params = {k: v for k, v in params.items() if v is not None}

            r = await session.get(
                f"{self.provider.base_url}blocks",
                headers={
                    "X-API-Key": self.provider.api_key,
                    "Content-Type": "application/json",
                    "accept": "application/json",
                },
                params=params,
            )
            result = await r.json()
            # measure ping RTT
            rtt = (time.time() - start_time) * 1000  # to ms
            self._last_ping_v3 = rtt
            return result
    
    def get_last_ping_v3(self) -> Optional[float]:
        """get last V3 API ping RTT in ms"""
        return self._last_ping_v3

    async def send(self, boc: bytes) -> str:
        """Returns message hash in base64 format"""
        serialized_boc = base64.b64encode(boc).decode()
        async with aiohttp.ClientSession() as session:
            r = await session.post(
                f"{self.provider.base_url}message",
                headers={
                    "X-API-Key": self.provider.api_key,
                    "Content-Type": "application/json",
                    "accept": "application/json",
                },
                json={"boc": serialized_boc},
            )
            resjson = await r.json()
            if "message_hash" not in resjson:
                raise Exception(f"Failed to send message: {resjson}")
            return resjson["message_hash_norm"]

    async def get_seqno(self, addr: str):
        async with aiohttp.ClientSession() as session:
            r = await session.post(
                f"{self.provider.base_url}runGetMethod",
                headers={
                    "X-API-Key": self.provider.api_key,
                    "Content-Type": "application/json",
                    "accept": "application/json",
                },
                json={"address": addr, "method": "seqno", "stack": []},
            )
            try:
                return int((await r.json())["stack"][0]["value"], 16)
            except Exception as e:
                return 0


class TonCenterStreamingClient:
    """websocket streaming client for toncenter api v2"""
    
    def __init__(self, api_key: str, testnet: bool = False):
        self.api_key = api_key
        self.testnet = testnet
        base = "testnet.toncenter.com" if testnet else "toncenter.com"
        self.ws_url = f"wss://{base}/api/streaming/v2/ws"
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._stop_ping = False
        self._subscribed_addresses: set[str] = set()
        self._last_ping_send_time: Optional[float] = None 
        self._last_ping_ws: Optional[float] = None
    
    async def connect(self) -> bool:
        """connect to websocket with api key"""
        try:
            ws_url_with_key = f"{self.ws_url}?api_key={self.api_key}"
            self.websocket = await websockets.connect(ws_url_with_key)
            self._stop_ping = False
            self._ping_task = asyncio.create_task(self._ping_loop())
            logger.debug(f"streaming: connected to {self.ws_url}")
            return True
        except Exception as e:
            logger.error(f"streaming: connection failed: {e}")
            return False
    
    async def _ping_loop(self):
        """send ping every 15 seconds to keep connection alive"""
        ping_id = 0
        while not self._stop_ping:
            try:
                await asyncio.sleep(15)
                if self.websocket and not self._stop_ping:
                    ping_id_str = f"ping-{ping_id}"
                    ping_msg = {"operation": "ping", "id": ping_id_str}
                    self._last_ping_send_time = time.time()
                    await self.websocket.send(json.dumps(ping_msg))
                    ping_id += 1
            except Exception as e:
                if not self._stop_ping:
                    logger.warning(f"streaming: ping failed: {e}")
                break
    
    def get_last_ping_ws(self) -> Optional[float]:
        """get last WS ping-pong RTT in ms"""
        return self._last_ping_ws
    
    async def close(self):
        """close websocket connection"""
        self._stop_ping = True
        if self._ping_task:
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass
        if self.websocket:
            await self.websocket.close()
            self.websocket = None
        self._subscribed_addresses.clear()
        self._last_ping_send_time = None
        logger.debug(f"streaming: connection closed")
    
    async def subscribe(self, addresses: list[str], types: list[str]) -> bool:
        """subscribe to addresses for given event types"""
        if not self.websocket:
            return False
        try:
            subscribe_msg = {
                "id": "sub-1",
                "operation": "subscribe",
                "addresses": addresses,
                "types": types,
                "min_finality": "pending",
                # "supported_action_types": ["v1"],
                "include_address_book": True,
                "include_metadata": False
            }
            await self.websocket.send(json.dumps(subscribe_msg))
            self._subscribed_addresses.update(addresses)
            logger.debug(f"streaming: subscribed to {len(addresses)} addresses")
            return True
        except Exception as e:
            logger.error(f"streaming: subscribe failed: {e}")
            return False
    
    async def unsubscribe(self, addresses: list[str]) -> bool:
        """unsubscribe from addresses"""
        if not self.websocket:
            return False
        try:
            unsubscribe_msg = {
                "operation": "unsubscribe",
                "addresses": addresses,
                "id": "unsub-1"
            }
            await self.websocket.send(json.dumps(unsubscribe_msg))
            self._subscribed_addresses -= set(addresses)
            logger.debug(f"streaming: unsubscribed from {len(addresses)} addresses")
            return True
        except Exception as e:
            logger.error(f"streaming: unsubscribe failed: {e}")
            return False
    
    async def listen(self, on_event: Callable, timeout: float = 300) -> bool:
        """
        listen for events and call callback for each one.
        on_event can be sync or async function.
        returns False if connection closed/error, True if timeout reached.
        """
        if not self.websocket:
            return False
        
        try:
            async for message in self.websocket:
                try:
                    data = json.loads(message)
                    
                    # skip status messages
                    if "status" in data:
                        status = data["status"]
                        if status == "subscribed":
                            logger.debug(f"streaming: subscribed")
                        elif status == "pong":
                            # measure ping-pong RTT from last sent ping
                            if self._last_ping_send_time:
                                rtt = (time.time() - self._last_ping_send_time) * 1000  # to ms
                                self._last_ping_ws = rtt
                                logger.debug(f"streaming: ping-pong RTT: {rtt:.2f}ms")
                        elif status == "unsubscribed":
                            logger.debug(f"streaming: unsubscribed")
                        continue
                    
                    # handle errors
                    if "error" in data:
                        logger.error(f"streaming: api error: {data['error']}")
                        continue
                    
                    # call event handler (supports both sync and async)
                    result = on_event(data)
                    if asyncio.iscoroutine(result):
                        await result
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"streaming: json decode error: {e}")
                except Exception as e:
                    logger.error(f"streaming: event handler error: {e}")
        
        except websockets.exceptions.ConnectionClosed:
            logger.warning(f"streaming: connection closed")
            return False
        except asyncio.TimeoutError:
            return True
        except Exception as e:
            logger.error(f"streaming: listen error: {e}")
            return False
        
        return False


__all__ = ["TonCenterClient", "TonCenterV3Client", "TonCenterStreamingClient"]
