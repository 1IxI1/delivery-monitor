import asyncio
from abc import ABC, abstractmethod

import aiohttp
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
            r = await q["func"](
                session,
                "getTransactions",
                params={
                    "address": address,
                    "limit": limit,
                    "lt": from_lt,
                    "to_lt": to_lt,
                    "archival": archival,
                },
            )
            return r

    async def send(self, boc: bytes):
        q = self.provider.raw_send_message(boc)

        async with aiohttp.ClientSession() as session:
            r = await q["func"](session, *q["args"], **q["kwargs"])
            return r


__all__ = ["TonCenterClient"]
