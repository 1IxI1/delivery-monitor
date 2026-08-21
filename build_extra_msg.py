"""Builds `extra_msg_boc` values for monitors.json.

The logger contract sends this internal message next to its set_code action, so
the trace gets a `jetton_transfer` action and the monitor can wait for it with
`"target_action_type": "jetton_transfer"`.

By default the message is a self-transfer of 1 unit: jetton balance stays the
same and excesses come back to the sender, so the only cost is network fees.

Examples:
    # mainnet PUB5 (Public Prepaid Jetton), as used by toncenter-streaming-mainnet:
    # w/prod-3.txt sends to w/prod-1.txt, which keeps every hop of the trace
    # cross-shard once the basechain splits
    python3 build_extra_msg.py \
        --owner EQAIbKPNrlKWAvnJwTgmSB153yRzhRsKm06jNKv2H0tK6iQ1 \
        --minter EQCgKV2KK3U6AcRpGj64KzGjz8h6aCIkaV9Mce_rpubewSl_ \
        --to EQBXQiDR589hhpT2TJGbdqVdK_JawD4r9VsAnsdnPXCsy4rh \
        --init-mode bare

    # the sender's own jetton wallet is not deployed until someone sends to it,
    # so the very first message has to carry state_init: run the monitor once
    # with this one, then switch back to the message above
    python3 build_extra_msg.py --owner ... --minter ... --with-state-init

    # testnet PUB4, sending 1 token to another owner
    python3 build_extra_msg.py --testnet \
        --owner kQDMo9xJIt6qMhYJg5luhIK18XkRJVKUPeOgrA0ORJoTgewC \
        --minter kQBECJWGkqPCLTugqIFQPK_s6NU-nfzkLG0mxdiu2hdCx7z6 \
        --to 0QD6a-uyjiAX8gRmtPi2ztdeBEliw6GrBa6dQBeM8wlQfJ5K

Send excesses to the destination (`--response <to>`) when you want the trace to
stay one hop shorter across shards, and keep them on the sender (the default)
when you want the excesses back and one more hop to measure.

Prepaid jetton wallets forward their ~855 byte code to the receiver on every
transfer so an absent receiver gets deployed. Once it exists, `--init-mode bare`
tells the sending wallet to address it directly and skip that forwarding.
"""

import argparse
import base64
import os
import sys

import requests
from pytoniq import Contract, begin_cell
from pytoniq_core import Address, Cell, StateInit

MAINNET_API_URL = "https://toncenter.com/api/v3/"
TESTNET_API_URL = "https://testnet.toncenter.com/api/v3/"

JETTON_TRANSFER_OP = 0x0F8A7EA5
# jetton-prepaid contracts: every wallet is born with this balance
PREPAID_BALANCE = 1000000
# jetton-prepaid contracts: first 2 bits of custom_payload pick how the receiving
# wallet is addressed, 0 is state_init (deploys it), 1 is a plain address
INIT_MODE_BARE = 1


def run_get_method(api_url: str, api_key: str, address: str, method: str, stack: list) -> list:
    """call a get method via toncenter v3, returns the resulting stack"""
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-API-Key"] = api_key
    r = requests.post(
        api_url + "runGetMethod",
        headers=headers,
        json={"address": address, "method": method, "stack": stack},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if data.get("exit_code") != 0:
        raise RuntimeError(f"{method} on {address} failed: exit_code {data.get('exit_code')}")
    return data["stack"]


def get_jetton_wallet(api_url: str, api_key: str, minter: Address, owner: Address) -> Address:
    """minter.get_wallet_address(owner)"""
    owner_slice = base64.b64encode(
        begin_cell().store_address(owner).end_cell().to_boc()
    ).decode()
    stack = run_get_method(
        api_url,
        api_key,
        minter.to_str(is_user_friendly=True, is_bounceable=True),
        "get_wallet_address",
        [{"type": "slice", "value": owner_slice}],
    )
    cell = Cell.one_from_boc(base64.b64decode(stack[0]["value"]))
    return cell.begin_parse().load_address()


def get_jetton_wallet_code(api_url: str, api_key: str, minter: Address) -> Cell:
    """jetton_wallet_code from minter.get_jetton_data()"""
    stack = run_get_method(
        api_url,
        api_key,
        minter.to_str(is_user_friendly=True, is_bounceable=True),
        "get_jetton_data",
        [],
    )
    return Cell.one_from_boc(base64.b64decode(stack[4]["value"]))


def prepaid_wallet_state_init(code: Cell, owner: Address, minter: Address) -> StateInit:
    """state_init of a not yet deployed prepaid jetton wallet

    WalletStorage { jettonBalance: coins, ownerAddress: address, minterAddress: address }
    """
    data = (
        begin_cell()
        .store_coins(PREPAID_BALANCE)
        .store_address(owner)
        .store_address(minter)
        .end_cell()
    )
    return StateInit(code=code, data=data)


def build_transfer_body(
    amount: int,
    destination: Address,
    response_destination: Address,
    forward_ton_amount: int,
    query_id: int,
    custom_payload: Cell = None,
) -> Cell:
    """transfer#0f8a7ea5, TEP-74"""
    body = (
        begin_cell()
        .store_uint(JETTON_TRANSFER_OP, 32)
        .store_uint(query_id, 64)
        .store_coins(amount)
        .store_address(destination)
        .store_address(response_destination)
    )
    if custom_payload is None:
        body.store_bool(False)
    else:
        body.store_bool(True).store_ref(custom_payload)
    return (
        body
        .store_coins(forward_ton_amount)
        .store_uint(0, 1)  # forward_payload, empty and in place
        .end_cell()
    )


def main():
    parser = argparse.ArgumentParser(
        description="Build an extra_msg_boc with a jetton transfer for monitors.json",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--owner", required=True, help="sender, i.e. the logger contract from the wallets file")
    parser.add_argument("--minter", required=True, help="jetton master address")
    parser.add_argument("--to", help="jetton receiver owner, defaults to --owner (self-transfer)")
    parser.add_argument("--response", help="where to send excesses, defaults to --owner")
    parser.add_argument("--amount", type=int, default=1, help="jetton units to send, default 1")
    parser.add_argument("--value", type=int, default=50000000, help="nanotons to attach, default 0.05 TON")
    parser.add_argument("--forward-ton", type=int, default=1, help="forward_ton_amount, default 1")
    parser.add_argument("--query-id", type=int, default=0, help="query_id, default 0")
    parser.add_argument(
        "--with-state-init",
        action="store_true",
        help="attach state_init of a prepaid jetton wallet to deploy it with the first message",
    )
    parser.add_argument(
        "--init-mode",
        choices=["auto", "bare"],
        default="auto",
        help="prepaid jettons: 'auto' forwards the wallet code so an absent receiver gets "
        "deployed, 'bare' skips it and needs the receiver's jetton wallet to exist",
    )
    parser.add_argument("--testnet", action="store_true", help="use testnet.toncenter.com")
    parser.add_argument("--api-key", default=os.environ.get("TONCENTER_API_KEY", ""), help="toncenter api key, or TONCENTER_API_KEY")
    args = parser.parse_args()

    api_url = TESTNET_API_URL if args.testnet else MAINNET_API_URL
    owner = Address(args.owner)
    minter = Address(args.minter)
    destination = Address(args.to) if args.to else owner
    response_destination = Address(args.response) if args.response else owner

    jetton_wallet = get_jetton_wallet(api_url, args.api_key, minter, owner)
    print(f"jetton wallet of {args.owner}:", jetton_wallet.to_str(is_user_friendly=True, is_bounceable=True), file=sys.stderr)

    state_init = None
    if args.with_state_init:
        code = get_jetton_wallet_code(api_url, args.api_key, minter)
        state_init = prepaid_wallet_state_init(code, owner, minter)
        deploys_to = Address((jetton_wallet.wc, state_init.serialize().hash))
        if deploys_to != jetton_wallet:
            raise SystemExit(
                f"state_init deploys to {deploys_to.to_str(is_user_friendly=True, is_bounceable=True)}, "
                f"not to the jetton wallet: this jetton is not a prepaid one"
            )
        print("state_init matches the jetton wallet address", file=sys.stderr)

    custom_payload = None
    if args.init_mode == "bare":
        custom_payload = begin_cell().store_uint(INIT_MODE_BARE, 2).end_cell()

    body = build_transfer_body(
        amount=args.amount,
        destination=destination,
        response_destination=response_destination,
        forward_ton_amount=args.forward_ton,
        query_id=args.query_id,
        custom_payload=custom_payload,
    )
    message = Contract.create_internal_msg(
        ihr_disabled=True,
        bounce=True,
        dest=jetton_wallet,
        src=None,  # type: ignore
        value=args.value,
        state_init=state_init,
        body=body,
    )
    print(message.serialize().to_boc().hex())


if __name__ == "__main__":
    main()
