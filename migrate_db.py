#!/usr/bin/env python3
"""
migration script to add new streaming fields to existing databases.
run once before using new monitor version.
supports both SQLite and ClickHouse backends.
"""
import json
import os
from pathlib import Path

from db_backend import create_backend, SQLiteBackend, ClickHouseBackend

# columns to add (name, sqlite_type, clickhouse_type)
NEW_COLUMNS = [
    ("sendboc_took", "REAL", "Nullable(Float64)"),
    ("pending_tx_in", "REAL", "Nullable(Float64)"),
    ("pending_action_in", "REAL", "Nullable(Float64)"),
    ("confirmed_tx_in", "REAL", "Nullable(Float64)"),
    ("confirmed_action_in", "REAL", "Nullable(Float64)"),
    ("finalized_tx_in", "REAL", "Nullable(Float64)"),
    ("finalized_action_in", "REAL", "Nullable(Float64)"),
    ("streaming_to_v3_lag", "REAL", "Nullable(Float64)"),
    ("ping_ws", "REAL", "Nullable(Float64)"),
    ("ping_v3", "REAL", "Nullable(Float64)"),
]


def get_existing_columns_sqlite(cursor) -> set:
    """get existing columns from SQLite table"""
    cursor.execute("PRAGMA table_info(txs)")
    return {row[1] for row in cursor.fetchall()}


def get_existing_columns_clickhouse(client, table: str) -> set:
    """get existing columns from ClickHouse table"""
    try:
        result = client.execute(f"DESCRIBE TABLE {table}")
        if isinstance(result, list):
            return {row[0] for row in result}
    except Exception:
        # table might not exist
        pass
    return set()


def migrate_sqlite_db(db_path: str) -> bool:
    """migrate single SQLite database, returns True if migrated"""
    import sqlite3
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        existing = get_existing_columns_sqlite(cursor)
        added = []
        
        for col_name, sqlite_type, _ in NEW_COLUMNS:
            if col_name not in existing:
                cursor.execute(f"ALTER TABLE txs ADD COLUMN {col_name} {sqlite_type}")
                added.append(col_name)
        
        conn.commit()
        conn.close()
        
        if added:
            print(f"  ✅ {db_path}: added {len(added)} columns: {', '.join(added)}")
            return True
        else:
            print(f"  ⏭️  {db_path}: already up to date")
            return False
    
    except Exception as e:
        print(f"  ❌ {db_path}: error: {e}")
        return False


def migrate_clickhouse_table(db_config: dict, dbname: str, testnet: bool = False) -> bool:
    """migrate single ClickHouse table, returns True if migrated"""
    try:
        # create backend to get client
        db_config_copy = db_config.copy()
        if testnet:
            db_config_copy["clickhouse_database"] = db_config.get("clickhouse_database_testnet", "default")
        else:
            db_config_copy["clickhouse_database"] = db_config.get("clickhouse_database_mainnet", "default")
        
        backend = create_backend(dbname, db_config_copy)
        if not isinstance(backend, ClickHouseBackend):
            return False
        
        backend.init_db()  # this creates table if it doesn't exist with all columns
        table = backend.table
        
        # get existing columns
        existing = get_existing_columns_clickhouse(backend.client, table)
        if not existing:
            # table was just created by init_db with all columns, no migration needed
            backend.close()
            print(f"  ⏭️  ClickHouse {dbname} ({table}): table created with all columns")
            return False
        
        added = []
        
        for col_name, _, clickhouse_type in NEW_COLUMNS:
            if col_name not in existing:
                try:
                    # ClickHouse doesn't support IF NOT EXISTS, so we check manually
                    backend.client.execute(
                        f"ALTER TABLE {table} ADD COLUMN {col_name} {clickhouse_type}"
                    )
                    added.append(col_name)
                except Exception as e:
                    # column might already exist (race condition) or other error
                    error_msg = str(e).lower()
                    if "already exists" in error_msg or "duplicate" in error_msg:
                        pass  # column already exists, skip
                    else:
                        print(f"    ⚠️  failed to add {col_name}: {e}")
        
        backend.close()
        
        if added:
            print(f"  ✅ ClickHouse {dbname} ({table}): added {len(added)} columns: {', '.join(added)}")
            return True
        else:
            print(f"  ⏭️  ClickHouse {dbname} ({table}): already up to date")
            return False
    
    except Exception as e:
        print(f"  ❌ ClickHouse {dbname}: error: {e}")
        return False


def load_config() -> dict:
    """load db config from monitors.json"""
    config_path = os.environ.get("MONITORS_CONFIG", "monitors.json")
    try:
        with open(config_path, "r") as f:
            data = json.load(f)
        return {
            "db_backend": data.get("db_backend", "sqlite"),
            "clickhouse_host": data.get("clickhouse_host", "localhost"),
            "clickhouse_port": data.get("clickhouse_port", 9000),
            "clickhouse_user": data.get("clickhouse_user", "default"),
            "clickhouse_password": data.get("clickhouse_password", ""),
            "clickhouse_database_mainnet": data.get("clickhouse_database_mainnet", "default"),
            "clickhouse_database_testnet": data.get("clickhouse_database_testnet", "default"),
            "monitors": data.get("monitors", []),
        }
    except Exception as e:
        print(f"⚠️  Failed to load {config_path}: {e}")
        return {"db_backend": "sqlite", "monitors": []}


def main():
    config = load_config()
    backend_type = config.get("db_backend", "sqlite")
    
    migrated_count = 0
    total_count = 0
    
    # migrate SQLite databases
    db_dir = Path("db")
    if db_dir.exists():
        db_files = list(db_dir.glob("*.db"))
        if db_files:
            print(f"Found {len(db_files)} SQLite database(s) to check:\n")
            for db_path in sorted(db_files):
                total_count += 1
                if migrate_sqlite_db(str(db_path)):
                    migrated_count += 1
            print()
    
    # migrate ClickHouse tables if backend is ClickHouse
    if backend_type == "clickhouse":
        monitors = config.get("monitors", [])
        clickhouse_monitors = [
            m for m in monitors 
            if m.get("dbname") and (m.get("enabled") or True)  # migrate all, not just enabled
        ]
        
        if clickhouse_monitors:
            print(f"Found {len(clickhouse_monitors)} ClickHouse table(s) to check:\n")
            for monitor in clickhouse_monitors:
                dbname = monitor.get("dbname")
                if not dbname:
                    continue
                testnet = monitor.get("testnet", False)
                total_count += 1
                if migrate_clickhouse_table(config, dbname, testnet=testnet):
                    migrated_count += 1
            print()
    
    print(f"Done. Migrated {migrated_count}/{total_count} database(s)/table(s).")


if __name__ == "__main__":
    main()
