"""database backend abstraction for sqlite and clickhouse"""
import os
import sqlite3
import time
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple


class DatabaseBackend(ABC):
    """abstract db backend, implement for sqlite/clickhouse"""
    
    dbname: str
    
    @abstractmethod
    def init_db(self) -> None:
        pass
    
    @abstractmethod
    def add_new_tx(self, addr: str, utime: float, msghash: str, sendboc_took: Optional[float]) -> None:
        pass
    
    @abstractmethod
    def get_missing_msgs(self, include_found: bool = False) -> List[Tuple[str, float, str]]:
        """returns list of (addr, utime, msghash)"""
        pass
    
    @abstractmethod
    def make_found(self, addr: str, utime: float, msghash: str, 
                   executed_in: float, found_in: float, commited_in: Optional[float]) -> None:
        pass
    
    @abstractmethod
    def update_streaming_field(self, addr: str, utime: float, msghash: str,
                               field: str, value: float) -> Tuple[bool, int]:
        """returns (updated, fixed_count) where fixed_count is nullified pending fields"""
        pass
    
    @abstractmethod
    def mark_streaming_found(self, addr: str, utime: float) -> None:
        pass
    
    @abstractmethod
    def get_found_hashes(self, since: float) -> set:
        pass
    
    @abstractmethod
    def get_interval_stats(self, interval_sec: float) -> Optional[tuple]:
        """get aggregated stats for interval endpoint"""
        pass
    
    @abstractmethod
    def get_stats_for_interval(self, interval_sec: float, addr: Optional[str] = None) -> Optional[tuple]:
        """get aggregated stats for stats endpoint"""
        pass
    
    @abstractmethod
    def close(self) -> None:
        pass


class SQLiteBackend(DatabaseBackend):
    """sqlite3 implementation - the good old reliable one"""
    
    def __init__(self, dbname: str):
        self.dbname = dbname
        self.connection: sqlite3.Connection
        self.cursor: sqlite3.Cursor
    
    def init_db(self) -> None:
        os.makedirs("db/", exist_ok=True)
        self.connection = sqlite3.connect(f"db/{self.dbname}.db")
        self.cursor = self.connection.cursor()
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS txs (
                addr TEXT,
                utime REAL,
                msghash STRING,
                is_found BOOLEAN,
                executed_in REAL,
                found_in REAL,
                commited_in REAL,
                sendboc_took REAL,
                pending_tx_in REAL,
                pending_action_in REAL,
                confirmed_tx_in REAL,
                confirmed_action_in REAL,
                finalized_tx_in REAL,
                finalized_action_in REAL,
                PRIMARY KEY (addr, utime)
            )
        """)
        self.connection.commit()
    
    def add_new_tx(self, addr: str, utime: float, msghash: str, sendboc_took: Optional[float]) -> None:
        self.cursor.execute(
            "INSERT INTO txs (addr, utime, msghash, is_found, sendboc_took) VALUES (?, ?, ?, 0, ?)",
            (addr, utime, msghash, sendboc_took),
        )
        self.connection.commit()
    
    def get_missing_msgs(self, include_found: bool = False) -> List[Tuple[str, float, str]]:
        now = time.time()
        if include_found:
            is_found_filter = ""
        else:
            is_found_filter = "is_found = 0 AND"
        
        self.cursor.execute(
            f"SELECT addr, utime, msghash FROM txs WHERE {is_found_filter} utime > ?",
            (now - 1200,),
        )
        return self.cursor.fetchall()
    
    def make_found(self, addr: str, utime: float, msghash: str,
                   executed_in: float, found_in: float, commited_in: Optional[float]) -> None:
        self.cursor.execute(
            "UPDATE txs SET is_found = 1, executed_in = ?, found_in = ?, commited_in = ? WHERE addr = ? AND utime = ?",
            (executed_in, found_in, commited_in, addr, utime),
        )
        self.connection.commit()
    
    def update_streaming_field(self, addr: str, utime: float, msghash: str,
                               field: str, value: float) -> Tuple[bool, int]:
        # ordering conditions
        extra_cond = ""
        pending_field = None
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
        
        self.cursor.execute(
            f"UPDATE txs SET {field} = ? WHERE addr = ? AND utime = ? AND {field} IS NULL{extra_cond}",
            (value, addr, utime),
        )
        updated = self.cursor.rowcount > 0
        fixed_count = 0
        
        # nullify pending if it arrived too close (<0.1s)
        if updated and pending_field:
            self.cursor.execute(
                f"UPDATE txs SET {pending_field} = NULL WHERE addr = ? AND utime = ? AND {pending_field} IS NOT NULL AND (? - {pending_field}) < 0.1",
                (addr, utime, value),
            )
            fixed_count = self.cursor.rowcount
        self.connection.commit()
        return updated, fixed_count
    
    def mark_streaming_found(self, addr: str, utime: float) -> None:
        self.cursor.execute(
            "UPDATE txs SET is_found = 1 WHERE addr = ? AND utime = ?",
            (addr, utime),
        )
        self.connection.commit()
    
    def get_found_hashes(self, since: float) -> set:
        self.cursor.execute(
            "SELECT msghash FROM txs WHERE is_found = 1 AND utime > ?",
            (since,),
        )
        return {row[0] for row in self.cursor.fetchall()}
    
    def get_interval_stats(self, interval_sec: float) -> Optional[tuple]:
        now = time.time()
        self.cursor.execute(
            """
            SELECT COUNT(*), AVG(is_found),
                   AVG(executed_in), MIN(executed_in), MAX(executed_in),
                   AVG(executed_in*executed_in) - AVG(executed_in)*AVG(executed_in),
                   AVG(found_in), MIN(found_in), MAX(found_in),
                   AVG(found_in*found_in) - AVG(found_in)*AVG(found_in),
                   AVG(commited_in), MIN(commited_in), MAX(commited_in),
                   AVG(commited_in*commited_in) - AVG(commited_in)*AVG(commited_in),
                   AVG(sendboc_took),
                   AVG(pending_tx_in), AVG(pending_action_in),
                   AVG(confirmed_tx_in), AVG(confirmed_action_in),
                   AVG(finalized_tx_in), AVG(finalized_action_in)
            FROM txs WHERE utime >= ? AND utime <= ?
            ORDER BY utime DESC LIMIT 1""",
            (now - 60 - interval_sec, now - 60),
        )
        return self.cursor.fetchone()
    
    def get_stats_for_interval(self, interval_sec: float, addr: Optional[str] = None) -> Optional[tuple]:
        now = time.time()
        addr_appendix = ""
        if addr:
            addr_appendix = f"AND addr = '{addr}'"
        
        self.cursor.execute(
            f"""
            SELECT COUNT(*), AVG(is_found),
                   AVG(executed_in), MIN(executed_in), MAX(executed_in),
                   AVG(executed_in*executed_in) - AVG(executed_in)*AVG(executed_in),
                   AVG(found_in), MIN(found_in), MAX(found_in),
                   AVG(found_in*found_in) - AVG(found_in)*AVG(found_in),
                   AVG(commited_in), MIN(commited_in), MAX(commited_in),
                   AVG(commited_in*commited_in) - AVG(commited_in)*AVG(commited_in),
                   AVG(sendboc_took),
                   AVG(pending_tx_in), AVG(pending_action_in),
                   AVG(confirmed_tx_in), AVG(confirmed_action_in),
                   AVG(finalized_tx_in), AVG(finalized_action_in)
            FROM txs WHERE utime >= ? {addr_appendix}
            ORDER BY utime DESC LIMIT 1""",
            (now - interval_sec,),
        )
        return self.cursor.fetchone()
    
    def close(self) -> None:
        if self.connection:
            self.connection.close()


class ClickHouseBackend(DatabaseBackend):
    """clickhouse implementation - for when you need SPEED"""
    
    def __init__(self, dbname: str, host: str = "localhost", port: int = 9000,
                 user: str = "default", password: str = "", database: str = "default"):
        self.dbname = dbname
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self._client = None  # lazy init
    
    @property
    def client(self):
        """get client, init if needed"""
        if self._client is None:
            raise RuntimeError("ClickHouse client not initialized, call init_db() first")
        return self._client
    
    @property
    def table(self) -> str:
        """table name safe for clickhouse (dashes become underscores)"""
        return self.dbname.replace("-", "_")
    
    def init_db(self) -> None:
        from clickhouse_driver import Client
        self._client = Client(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )
        # ReplacingMergeTree for pseudo-updates
        self.client.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                addr String,
                utime Float64,
                msghash String,
                is_found UInt8 DEFAULT 0,
                executed_in Nullable(Float64),
                found_in Nullable(Float64),
                commited_in Nullable(Float64),
                sendboc_took Nullable(Float64),
                pending_tx_in Nullable(Float64),
                pending_action_in Nullable(Float64),
                confirmed_tx_in Nullable(Float64),
                confirmed_action_in Nullable(Float64),
                finalized_tx_in Nullable(Float64),
                finalized_action_in Nullable(Float64),
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (addr, utime)
        """)
    
    def add_new_tx(self, addr: str, utime: float, msghash: str, sendboc_took: Optional[float]) -> None:
        self.client.execute(
            f"INSERT INTO {self.table} (addr, utime, msghash, is_found, sendboc_took) VALUES",
            [(addr, utime, msghash, 0, sendboc_took)],
        )
    
    def get_missing_msgs(self, include_found: bool = False) -> List[Tuple[str, float, str]]:
        now = time.time()
        if include_found:
            is_found_filter = ""
        else:
            is_found_filter = "is_found = 0 AND"
        
        result = self.client.execute(
            f"SELECT addr, utime, msghash FROM {self.table} FINAL WHERE {is_found_filter} utime > %(since)s",
            {"since": now - 1200},
        )
        # cast to expected type
        if not isinstance(result, list):
            return []
        return [(str(r[0]), float(r[1]), str(r[2])) for r in result]
    
    def make_found(self, addr: str, utime: float, msghash: str,
                   executed_in: float, found_in: float, commited_in: Optional[float]) -> None:
        # insert new row - ReplacingMergeTree will dedupe by (addr, utime)
        self.client.execute(
            f"""INSERT INTO {self.table} 
                (addr, utime, msghash, is_found, executed_in, found_in, commited_in) VALUES""",
            [(addr, utime, msghash, 1, executed_in, found_in, commited_in)],
        )
    
    def _get_current_row(self, addr: str, utime: float) -> Optional[dict]:
        """fetch current row with all fields"""
        rows = self.client.execute(
            f"""SELECT msghash, is_found, executed_in, found_in, commited_in, sendboc_took,
                       pending_tx_in, pending_action_in, confirmed_tx_in, confirmed_action_in,
                       finalized_tx_in, finalized_action_in
                FROM {self.table} FINAL WHERE addr = %(addr)s AND utime = %(utime)s""",
            {"addr": addr, "utime": utime},
        )
        if not isinstance(rows, list) or not rows:
            return None
        row = rows[0]
        return {
            "msghash": row[0], "is_found": row[1], "executed_in": row[2],
            "found_in": row[3], "commited_in": row[4], "sendboc_took": row[5],
            "pending_tx_in": row[6], "pending_action_in": row[7],
            "confirmed_tx_in": row[8], "confirmed_action_in": row[9],
            "finalized_tx_in": row[10], "finalized_action_in": row[11],
        }
    
    def update_streaming_field(self, addr: str, utime: float, msghash: str,
                               field: str, value: float) -> Tuple[bool, int]:
        current = self._get_current_row(addr, utime)
        
        # check ordering conditions
        skip = False
        pending_field = None
        if field == "pending_tx_in":
            if current and (current.get("confirmed_tx_in") or current.get("finalized_tx_in")):
                skip = True
        elif field == "pending_action_in":
            if current and (current.get("confirmed_action_in") or current.get("finalized_action_in")):
                skip = True
        elif field == "confirmed_tx_in":
            if current and current.get("finalized_tx_in"):
                skip = True
            pending_field = "pending_tx_in"
        elif field == "confirmed_action_in":
            if current and current.get("finalized_action_in"):
                skip = True
            pending_field = "pending_action_in"
        elif field == "finalized_tx_in":
            pending_field = "pending_tx_in"
        elif field == "finalized_action_in":
            pending_field = "pending_action_in"
        
        # skip if field already set
        if current and current.get(field) is not None:
            skip = True
        
        if skip:
            return False, 0
        
        # build new row with merged values
        new_row = {
            "addr": addr, "utime": utime, "msghash": msghash,
            "is_found": current.get("is_found", 0) if current else 0,
            "executed_in": current.get("executed_in") if current else None,
            "found_in": current.get("found_in") if current else None,
            "commited_in": current.get("commited_in") if current else None,
            "sendboc_took": current.get("sendboc_took") if current else None,
            "pending_tx_in": current.get("pending_tx_in") if current else None,
            "pending_action_in": current.get("pending_action_in") if current else None,
            "confirmed_tx_in": current.get("confirmed_tx_in") if current else None,
            "confirmed_action_in": current.get("confirmed_action_in") if current else None,
            "finalized_tx_in": current.get("finalized_tx_in") if current else None,
            "finalized_action_in": current.get("finalized_action_in") if current else None,
        }
        new_row[field] = value
        
        # nullify pending if it arrived too close (<0.1s)
        fixed_count = 0
        if pending_field and new_row.get(pending_field) is not None:
            if (value - new_row[pending_field]) < 0.1:
                new_row[pending_field] = None
                fixed_count = 1
        
        self.client.execute(
            f"""INSERT INTO {self.table} 
                (addr, utime, msghash, is_found, executed_in, found_in, commited_in, sendboc_took,
                 pending_tx_in, pending_action_in, confirmed_tx_in, confirmed_action_in,
                 finalized_tx_in, finalized_action_in) VALUES""",
            [(new_row["addr"], new_row["utime"], new_row["msghash"], new_row["is_found"],
              new_row["executed_in"], new_row["found_in"], new_row["commited_in"], new_row["sendboc_took"],
              new_row["pending_tx_in"], new_row["pending_action_in"],
              new_row["confirmed_tx_in"], new_row["confirmed_action_in"],
              new_row["finalized_tx_in"], new_row["finalized_action_in"])],
        )
        return True, fixed_count
    
    def mark_streaming_found(self, addr: str, utime: float) -> None:
        current = self._get_current_row(addr, utime)
        if current:
            self.client.execute(
                f"""INSERT INTO {self.table} 
                    (addr, utime, msghash, is_found, executed_in, found_in, commited_in, sendboc_took,
                     pending_tx_in, pending_action_in, confirmed_tx_in, confirmed_action_in,
                     finalized_tx_in, finalized_action_in) VALUES""",
                [(addr, utime, current["msghash"], 1,
                  current["executed_in"], current["found_in"], current["commited_in"], current["sendboc_took"],
                  current["pending_tx_in"], current["pending_action_in"],
                  current["confirmed_tx_in"], current["confirmed_action_in"],
                  current["finalized_tx_in"], current["finalized_action_in"])],
            )
    
    def get_found_hashes(self, since: float) -> set:
        result = self.client.execute(
            f"SELECT msghash FROM {self.table} FINAL WHERE is_found = 1 AND utime > %(since)s",
            {"since": since},
        )
        if not isinstance(result, list):
            return set()
        return {str(row[0]) for row in result}
    
    def get_interval_stats(self, interval_sec: float) -> Optional[tuple]:
        now = time.time()
        result = self.client.execute(
            f"""
            SELECT count(), avg(is_found),
                   avg(executed_in), min(executed_in), max(executed_in),
                   varPop(executed_in),
                   avg(found_in), min(found_in), max(found_in),
                   varPop(found_in),
                   avg(commited_in), min(commited_in), max(commited_in),
                   varPop(commited_in),
                   avg(sendboc_took),
                   avg(pending_tx_in), avg(pending_action_in),
                   avg(confirmed_tx_in), avg(confirmed_action_in),
                   avg(finalized_tx_in), avg(finalized_action_in)
            FROM {self.table} FINAL
            WHERE utime >= %(from_time)s AND utime <= %(to_time)s""",
            {"from_time": now - 60 - interval_sec, "to_time": now - 60},
        )
        if not isinstance(result, list) or len(result) == 0:
            return None
        return tuple(result[0])
    
    def get_stats_for_interval(self, interval_sec: float, addr: Optional[str] = None) -> Optional[tuple]:
        now = time.time()
        addr_filter = ""
        params: dict = {"since": now - interval_sec}
        if addr:
            addr_filter = "AND addr = %(addr)s"
            params["addr"] = addr
        
        result = self.client.execute(
            f"""
            SELECT count(), avg(is_found),
                   avg(executed_in), min(executed_in), max(executed_in),
                   varPop(executed_in),
                   avg(found_in), min(found_in), max(found_in),
                   varPop(found_in),
                   avg(commited_in), min(commited_in), max(commited_in),
                   varPop(commited_in),
                   avg(sendboc_took),
                   avg(pending_tx_in), avg(pending_action_in),
                   avg(confirmed_tx_in), avg(confirmed_action_in),
                   avg(finalized_tx_in), avg(finalized_action_in)
            FROM {self.table} FINAL
            WHERE utime >= %(since)s {addr_filter}""",
            params,
        )
        if not isinstance(result, list) or len(result) == 0:
            return None
        return tuple(result[0])
    
    def close(self) -> None:
        if self._client:
            self._client.disconnect()


def create_backend(dbname: str, db_config: dict) -> DatabaseBackend:
    """factory function to create db backend from config"""
    backend_type = db_config.get("db_backend", "sqlite")
    
    if backend_type == "clickhouse":
        return ClickHouseBackend(
            dbname=dbname,
            host=db_config.get("clickhouse_host", "localhost"),
            port=db_config.get("clickhouse_port", 9000),
            user=db_config.get("clickhouse_user", "default"),
            password=db_config.get("clickhouse_password", ""),
            database=db_config.get("clickhouse_database", "default"),
        )
    else:
        return SQLiteBackend(dbname=dbname)

