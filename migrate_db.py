#!/usr/bin/env python3
"""
migration script to add new streaming fields to existing databases.
run once before using new monitor version.
"""
import sqlite3
import os
from pathlib import Path

NEW_COLUMNS = [
    ("sendboc_took", "REAL"),
    ("pending_tx_in", "REAL"),
    ("pending_action_in", "REAL"),
    ("confirmed_tx_in", "REAL"),
    ("confirmed_action_in", "REAL"),
    ("finalized_tx_in", "REAL"),
    ("finalized_action_in", "REAL"),
    ("streaming_to_v3_lag", "REAL"),
]


def get_existing_columns(cursor) -> set:
    cursor.execute("PRAGMA table_info(txs)")
    return {row[1] for row in cursor.fetchall()}


def migrate_db(db_path: str) -> bool:
    """migrate single database, returns True if migrated"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        existing = get_existing_columns(cursor)
        added = []
        
        for col_name, col_type in NEW_COLUMNS:
            if col_name not in existing:
                cursor.execute(f"ALTER TABLE txs ADD COLUMN {col_name} {col_type}")
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


def main():
    db_dir = Path("db")
    if not db_dir.exists():
        print("No db/ directory found, nothing to migrate")
        return
    
    db_files = list(db_dir.glob("*.db"))
    if not db_files:
        print("No .db files found in db/")
        return
    
    print(f"Found {len(db_files)} database(s) to check:\n")
    
    migrated = 0
    for db_path in sorted(db_files):
        if migrate_db(str(db_path)):
            migrated += 1
    
    print(f"\nDone. Migrated {migrated}/{len(db_files)} database(s).")


if __name__ == "__main__":
    main()

