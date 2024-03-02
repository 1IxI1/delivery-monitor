# TON External messages delivery monitor

This script sends a unique external message every 60 seconds to each of his wallets and then waits for them, saving the stats about ones delivered.

Of course, it can be run with different providers to check for any issues with Lite Servers.

## How to use

Deploy log-wallets (script is the same as for v3 wallets) and create `wallets.py` file with your new loggers:

```python
wallets = [
    {
        "addr": "EQAWgUoE0qsUDUJMJXH25H__mATCdGkhVDQtCw06nOqXoDJ9",
        "seed": "aaaaaaaaaa30d3d04bbf6e1bbbbbbbbbbbbbbbbbbbbbbb12bab1d0ae1deccccc",
        "wallet": None,
        "sk": None,
    },
]
```

Run:

```bash
cp .env.example .env 
pip install -r requirements.txt
python main.py 1000
```

This will send 1000 messages in total to your wallets. Not 1000 to each, but 1000 to all.

## What are log-wallets

Log-wallets are the same as for v3r2 wallets, but with
- no message sending ability
- no check for `subwallet_id`

And thus, you can provide any variable `subwallet_id` in the message and it **won't make any action**, except increasing seqno.

In the script, this variable is occupied by the timestamp of sending, making **every message unique**.

## Configuration

You may fill your `.env` in the following ways:

```bash
LOGDIR="log"
TESNET=true
PROVIDER="toncenter" # liteserver, tonapi
TONCENTER_API_URL="https://testnet.toncenter.com/api/v2/"
TONCENTER_API_KEY="3acfd04736431db1dbbe44a3b9921ee8b8ccb31c8373c947f5066a43afb0451b"
```

```bash
LOGDIR="log"
TESNET=true
PROVIDER="liteserver" # toncenter, tonapi
CONFIG="testnet-global.config.json"
```

```bash
LOGDIR="log"
TESNET=true
PROVIDER="tonapi" # liteserver, toncenter 
TONAPI_KEY="1234567890"
```
