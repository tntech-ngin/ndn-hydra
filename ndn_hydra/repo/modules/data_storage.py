import sqlite3
import time
from typing import List

from ndn.storage import SqliteStorage
from ndn.encoding import Name, NonStrictName

# DataStorage adds additional methods to the SqliteStorage
class DataStorage(SqliteStorage):
    def __init__(self, db_path: str, write_period: int = 10, initialize: bool = True):
        super().__init__(db_path, write_period, initialize)
        self.db_cache = {}
        self.db_cache_timeout = 15 * 60 # 15 minutes
        self.batch_size = 900
        # self.conn.execute('PRAGMA journal_mode=wal')  # Enable WAL mode


    def _prepare_cache(self, file_name: str, total_segments: int, can_be_prefix: bool = False, must_be_fresh: bool = False) -> None:
        try: 
            all_keys = [file_name + "/seg=" + str(i) for i in range(total_segments)]
            batch_keys_b = [self._get_name_bytes_wo_tl(Name.normalize(key)) for key in all_keys]
            cursor = self.conn.cursor()
            current_time = time.time()

            query_base = 'SELECT value FROM data WHERE '

            if must_be_fresh:
                query_base += f'(expire_time_ms > {int(time.time() * 1000)}) AND '

            for i in range(0, len(all_keys), self.batch_size):
                batch_keys = all_keys[i:i + self.batch_size]
                batch_keys_b_part = batch_keys_b[i:i + self.batch_size]
            
                if can_be_prefix:
                    conditions = ' OR '.join(['hex(key) LIKE ?'] * len(batch_keys_b_part))
                    order_by = 'CASE key ' + ' '.join([f'WHEN hex(key) LIKE ? THEN {j}' for j in range(len(batch_keys_b_part))]) + ' END'
                    query = f"{query_base}{conditions} ORDER BY {order_by}"
                    params = tuple(key.hex() + '%' for key in batch_keys_b_part) + tuple(key.hex() + '%' for key in batch_keys_b_part)
                else:
                    conditions = 'key IN ({})'.format(', '.join('?' * len(batch_keys_b_part)))
                    order_by = 'CASE key ' + ' '.join([f'WHEN key = ? THEN {j}' for j in range(len(batch_keys_b_part))]) + ' END'
                    query = f"{query_base}{conditions} ORDER BY {order_by}"
                    params = tuple(batch_keys_b_part) + tuple(batch_keys_b_part)
                cursor.execute(query, params)

                for j, row in enumerate(cursor.fetchall()):
                    self.db_cache[batch_keys[j]] = (row[0] if row else None, current_time + self.db_cache_timeout)

        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
        finally:
            cursor.close()


    def get_packet(self, segment_comp: str, total_segments: int, file_name: str, can_be_prefix: bool = False, must_be_fresh: bool = False) -> List[bytes]:
        key = file_name + segment_comp

        # If in cache, return from cache
        if val := self.db_cache.get(key):
            return val[0]

        # Prepare cache when segment 0 is requested
        if segment_comp == "/seg=0":
            # TODO: Prepare cache only if we have sufficient memory in the system
            self._prepare_cache(file_name, total_segments, can_be_prefix, must_be_fresh)
            # aio.get_event_loop().run_in_executor(None, self._prepare_cache, file_name, total_segments, can_be_prefix, must_be_fresh)
                
        # Fallback to parent to fetch if not in cache. This will be slighly slower
        return super().get_packet(key, can_be_prefix, must_be_fresh)


    def remove_expired_cache(self) -> None:
        current_time = time.time()
        self.db_cache = {k: v for k, v in self.db_cache.items() if v[1] > current_time}


    def remove_packets(self, names: List[NonStrictName]) -> int:
        if not self.initialized:
            raise self.UninitializedError("The storage is not initialized.")

        keys = [self._get_name_bytes_wo_tl(Name.normalize(name)) for name in names]
        if not keys:
            return 0

        total_deleted = 0

        try:
            self.conn.execute('BEGIN TRANSACTION')

            for i in range(0, len(keys), self.batch_size):
                batch_keys = keys[i:i + self.batch_size]
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
