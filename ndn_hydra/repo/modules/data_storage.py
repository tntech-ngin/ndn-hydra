import sqlite3
from typing import List

from ndn.storage import SqliteStorage
from ndn.encoding import Name, NonStrictName, parse_data

# DataStorage adds additional methods to the SqliteStorage
class DataStorage(SqliteStorage):
    def __init__(self, db_path: str, write_period: int = 10, initialize: bool = True):
        super().__init__(db_path, write_period, initialize)
        self.batch_size = 900
        # self.conn.execute('PRAGMA journal_mode=wal')  # Enable WAL mode

    def put_packet(self, name: NonStrictName, data: bytes, internal: bool = False) -> None:
        if not self.initialized:
            raise self.UninitializedError("The storage is not initialized.")
        if not internal:
            _, _, data, _ = parse_data(data)
        super().put_packet(name, data)

    def remove_packets(self, names: List[NonStrictName]) -> int:
        if not self.initialized:
            raise self.UninitializedError('The storage is not initialized.')

        keys = [self._get_name_bytes_wo_tl(Name.normalize(name)) for name in names]
        if not keys:
            return 0

        total_deleted = 0
        cursor = None

        try:
            self.conn.execute('BEGIN TRANSACTION')
            cursor = self.conn.cursor()

            for i in range(0, len(keys), self.batch_size):
                batch_keys = keys[i:i + self.batch_size]
                placeholders = ', '.join('?' * len(batch_keys))
                query = f'DELETE FROM data WHERE key IN ({placeholders})'

                cursor.execute(query, batch_keys)
                total_deleted += cursor.rowcount

            self.conn.commit()

        except sqlite3.Error as e:
            self.conn.rollback()
            print(f'SQLite error: {e}')

        finally:
            if cursor:
                cursor.close()

        try:
            self.conn.execute('VACUUM')

        except sqlite3.Error as e:
            print(f'SQLite error during VACUUM: {e}')

        # Close connection (remove_packets is performed with a separate connection)
        self.conn.close()

        return total_deleted
