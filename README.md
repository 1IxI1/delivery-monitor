# TON External messages delivery monitor

This script sends a unique external message every 60 seconds to each of his wallets and then waits for them, saving the stats about ones delivered.

Of course, it can be run with different providers to check for any issues with Lite Servers.

## How to use

Deploy log-wallets (script is the same as for v3 wallets).

Create `wallets.txt` file with your new loggers:

```
EQAWgUoE0qsUDUJMJXH25H__mATCdGkhVDQtCw06nOqXoDJ9 0309251779ebe8f2a4ccb10f4edcf2243e78764d63bb11c146fec926eb819512
EQBPXbJUj91i2jkcyghHLUbF27wxM1oLpLp99yeyAugDAEYv d2c97e8439ba407e448f8864961480483089cb8a83e2a8085d67b4feda6765e2
```

The second column here is the wallet's **seed** - 64 hex characters.

Add as many wallets as you want.

#### Run:

```bash
cp .env.example .env
pip install -r requirements.txt
python main.py 1000
```

This will send 1000 messages in total to your wallets. Not 1000 to each, but 1000 to all.

## What are log-wallets

Log-wallets are the same as v3r2 wallets, but with

-   no message sending ability
-   no check for `subwallet_id`

And thus, you can provide any variable `subwallet_id` in the message and contract **won't make any action**, except increasing seqno.

In the script, this variable is occupied by the timestamp of sending, making **every message unique**.

## Configuration

You may fill your `.env` in the following ways:

```bash
WALLETS="wallets-testnet.txt"
DBNAME="results" # file will be db/results.db
TESNET=true
PROVIDER="toncenter" # liteserver, tonapi
TONCENTER_API_URL="https://testnet.toncenter.com/api/v2/"
TONCENTER_API_KEY="3acfd04736431db1dbbe44a3b9921ee8b8ccb31c8373c947f5066a43afb0451b"
```

```bash
WALLETS="2-wallets-testnet.txt"
DBNAME="ls-results"
TESNET=true
PROVIDER="liteserver" # toncenter, tonapi
CONFIG="testnet-global.config.json"
```

```bash
WALLETS="wallets.txt"
DBNAME="ta-mainnet-results"
TESNET=true
PROVIDER="tonapi" # liteserver, toncenter
TONAPI_KEY="1234567890"
```

## Results database

In `db/` dir, in an **sqlite database** you will find all the transactions you've sent on wallets,
with their delivery times.

-   `utime` - timestamp of sending
-   `executed_in` - seconds between sending and block where the message was found
-   `found_in` - seconds between sending and time when script got the info from provider

`db/toncenter.db` example:

| addr                                               | utime      | is_found | executed_in | found_in |
| -------------------------------------------------- | ---------- | -------- | ----------- | -------- |
| EQBPXbJUj91i2jkcyghHLUbF27wxM1oLpLp99yeyAugDAEYv   | 1709637119 | true     | 3           | 12       |
| EQAlt8tyJ75FRhDMDgL0sX-x93PTE7n16rfmg_Cv4KQgwdz5   | 1709637120 | true     | 2           | 11       |
| EQCc7AtrG7xHT76LEVl2OqnkHN-BLJtCWquRVidvY5e8VyDk   | 1709637118 | true     | 2           | 14       |
| EQCznq_pKOQMnUpGWY4qb9SAzqc0NmtF0vGUdJLp4hXi0T8A   | 1709637110 | true     | 7           | 22       |
| EQAWgUoE0qsUDUJMJXH25H\_\_mATCdGkhVDQtCw06nOqXoDJ9 | 1709637182 | true     | 7           | 16       |
| EQAfX-dlqHKsGtAE6zeLvFcqjTTuXD7imNN4sI3Q5uwqle4D   | 1709637188 | true     | 1           | 11       |
