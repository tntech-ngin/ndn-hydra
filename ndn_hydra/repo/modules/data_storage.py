import sqlite3
from typing import List

from ndn.storage import SqliteStorage
from ndn.encoding import Name, NonStrictName


# DataStorage adds batch remove packets method
# to the SqliteStorage
class DataStorage(SqliteStorage):
    def __init__(self, db_path: str, write_period: int = 10, initialize: bool = True):
        super().__init__(db_path, write_period, initialize)
        # self.conn.execute('PRAGMA journal_mode=wal')  # Enable WAL mode

    def remove_packets(self, names: List[NonStrictName]) -> int:
        if self.initialized is not True:
            raise self.UninitializedError("The storage is not initialized.")

        keys = [self._get_name_bytes_wo_tl(Name.normalize(name)) for name in names]
        if not keys:
            return 0

        total_deleted = 0
        batch_size = 1000
        try:
            self.conn.execute('BEGIN TRANSACTION')

            for i in range(0, len(keys), batch_size):
                batch_keys = keys[i:i + batch_size]
                placeholders = ', '.join('?' * len(batch_keys))
                query = f'DELETE FROM data WHERE key IN ({placeholders})'

                c = self.conn.cursor()
                c.execute(query, batch_keys)
                total_deleted += c.rowcount

            self.conn.commit()
        except sqlite3.Error as e:
            self.conn.rollback()
            print(f"SQLite error: {e}")

        finally:
            if c: c.close()

        try:
            self.conn.execute('VACUUM')
        except sqlite3.Error as e:
            print(f"SQLite error during VACUUM: {e}")

        return total_deleted
