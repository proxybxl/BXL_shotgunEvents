"""
MariaDB logging for plugin/thread executions.

File logging is unchanged. This module writes one row per (event, plugin)
worker-thread run into a MEMORY buffer table and periodically copies complete
rows (including full plugin output) to an InnoDB archive table.
"""

import datetime
import logging
import queue
import threading
import traceback

# Must match VARCHAR(4096) on plugin_event_log_mem in sql/plugin_event_log.sql
MEMORY_OUTPUT_MAX_CHARS = 4096

_INSERT_MEM_SQL = (
    "INSERT INTO plugin_event_log_mem "
    "(event_id, plugin_name, started_at, duration_us, completed_at, plugin_output) "
    "VALUES (%s, %s, %s, %s, %s, %s)"
)

_INSERT_DISK_SQL = (
    "INSERT INTO plugin_event_log "
    "(event_id, plugin_name, started_at, duration_us, completed_at, plugin_output) "
    "VALUES (%s, %s, %s, %s, %s, %s)"
)

_TRUNCATE_MEM_SQL = "TRUNCATE TABLE plugin_event_log_mem"

_FLUSH_MEM_TO_DISK_SQL = (
    "INSERT INTO plugin_event_log "
    "(event_id, plugin_name, started_at, duration_us, completed_at, plugin_output) "
    "SELECT event_id, plugin_name, started_at, duration_us, completed_at, plugin_output "
    "FROM plugin_event_log_mem"
)


class OutputCaptureHandler(logging.Handler):
    """
    Capture log records emitted on a plugin logger (and its children via
    propagate) for the duration of one plugin worker-thread event.
    """

    def __init__(self):
        super().__init__()
        self.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        self._lock = threading.Lock()
        self._buffer = []
        self._active = False

    def emit(self, record):
        if not self._active:
            return
        try:
            msg = self.format(record)
        except Exception:
            return
        with self._lock:
            if self._active:
                self._buffer.append(msg)

    def begin(self):
        with self._lock:
            self._buffer = []
            self._active = True

    def finish(self):
        with self._lock:
            self._active = False
            output = "\n".join(self._buffer)
            self._buffer = []
            return output or None


def _naive_utc(dt):
    """Return a timezone-naive datetime in UTC for MariaDB DATETIME columns."""
    if dt is None:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return dt


def _truncate_output(output, max_chars):
    if output is None:
        return None
    if max_chars is not None and len(output) > max_chars:
        return output[:max_chars]
    return output


class DatabaseLogger(object):
    """
    Non-blocking logger that records plugin/thread executions to MariaDB.

    Plugin worker threads call L{log_plugin_run}, which only enqueues a row.
    A dedicated writer thread batch-inserts into the MEMORY buffer table. A
    flusher thread (or L{flush_to_disk} on shutdown) copies complete rows to
    the InnoDB archive and truncates the buffer.

    Database failures never propagate to callers.
    """

    def __init__(self, config, logger, connect=None):
        """
        @param config: Daemon config providing database_log settings.
        @type config: L{configparser.ConfigParser} (duck-typed)
        @param logger: Logger used for DatabaseLogger diagnostics (file log).
        @type logger: L{logging.Logger}
        @param connect: Optional callable returning a DB connection. Defaults
            to opening a MySQLdb connection from config.
        """
        self._logger = logger
        self._connect_factory = connect

        self._host = config.get("database_log", "host")
        self._port = (
            config.getint("database_log", "port")
            if config.has_option("database_log", "port")
            else 3306
        )
        self._database = config.get("database_log", "database")
        self._user = config.get("database_log", "user")
        self._password = (
            config.get("database_log", "password")
            if config.has_option("database_log", "password")
            else ""
        )
        self._flush_interval = (
            config.getint("database_log", "flush_interval_seconds")
            if config.has_option("database_log", "flush_interval_seconds")
            else 900
        )
        self._batch_size = (
            config.getint("database_log", "batch_size")
            if config.has_option("database_log", "batch_size")
            else 100
        )
        self._queue_max = (
            config.getint("database_log", "queue_maxsize")
            if config.has_option("database_log", "queue_maxsize")
            else 50000
        )
        self._max_output_chars = (
            config.getint("database_log", "max_output_chars")
            if config.has_option("database_log", "max_output_chars")
            else 1048576
        )
        self._flush_in_daemon = True
        if config.has_option("database_log", "flush_in_daemon"):
            self._flush_in_daemon = config.getboolean("database_log", "flush_in_daemon")

        self._queue = queue.Queue(maxsize=self._queue_max)
        self._wakeup = object()
        self._archive = []
        self._db_lock = threading.Lock()
        self._stop = threading.Event()
        self._conn = None
        self._dropped = 0
        self._writer_thread = None
        self._flush_thread = None

    def start(self):
        """Open the DB connection and start writer/flusher threads."""
        self._conn = self._open_connection()
        self._writer_thread = threading.Thread(
            target=self._writer_loop, name="DatabaseLogger-writer", daemon=True
        )
        self._writer_thread.start()
        if self._flush_in_daemon:
            self._flush_thread = threading.Thread(
                target=self._flush_loop, name="DatabaseLogger-flush", daemon=True
            )
            self._flush_thread.start()
        self._logger.info(
            "Database logging started (host=%s db=%s flush_interval=%ss).",
            self._host,
            self._database,
            self._flush_interval,
        )

    def attach_capture(self, plugin_logger):
        """
        Attach an L{OutputCaptureHandler} to C{plugin_logger} and return it.

        @param plugin_logger: The plugin's logger.
        @type plugin_logger: L{logging.Logger}
        @return: The capture handler to begin/finish around each event.
        @rtype: L{OutputCaptureHandler}
        """
        handler = OutputCaptureHandler()
        plugin_logger.addHandler(handler)
        return handler

    def log_plugin_run(
        self,
        event_id,
        plugin_name,
        started_at,
        duration_us,
        completed_at,
        plugin_output=None,
    ):
        """
        Enqueue one plugin/thread execution. Never blocks the caller for I/O.

        @param event_id: Shotgun event id.
        @param plugin_name: Plugin file stem (no path, no .py).
        @param started_at: When the plugin worker began this event.
        @param duration_us: Execution duration in microseconds.
        @param completed_at: When the plugin worker finished this event.
        @param plugin_output: Captured logger output, or None.
        """
        try:
            row = (
                int(event_id),
                plugin_name,
                _naive_utc(started_at),
                int(duration_us),
                _naive_utc(completed_at),
                _truncate_output(plugin_output, self._max_output_chars),
            )
            self._queue.put_nowait(row)
        except queue.Full:
            self._dropped += 1
            if self._dropped == 1 or self._dropped % 1000 == 0:
                self._logger.warning(
                    "Database log queue full; dropped %d plugin run(s).",
                    self._dropped,
                )
        except Exception:
            self._logger.error(
                "Failed to enqueue plugin run for event %s plugin %s.\n\n%s",
                event_id,
                plugin_name,
                traceback.format_exc(),
            )

    def shutdown(self):
        """Stop threads, drain the queue, flush remaining rows to disk."""
        self._stop.set()
        try:
            self._queue.put_nowait(self._wakeup)
        except queue.Full:
            pass
        if self._writer_thread is not None:
            self._writer_thread.join(timeout=30)
        if self._flush_thread is not None:
            self._flush_thread.join(timeout=30)
        try:
            self._drain_queue_to_memory()
            if self._flush_in_daemon:
                self.flush_to_disk()
        except Exception:
            self._logger.error(
                "Error while shutting down database logging.\n\n%s",
                traceback.format_exc(),
            )
        finally:
            self._close_connection()
        if self._dropped:
            self._logger.warning(
                "Database logging dropped %d plugin run(s) because the queue was full.",
                self._dropped,
            )
        self._logger.info("Database logging stopped.")

    def flush_to_disk(self):
        """
        Copy buffered rows to the InnoDB archive and truncate the MEMORY table.

        Full plugin output is taken from the in-process archive (not the
        truncated MEMORY VARCHAR) so LONGTEXT on disk is complete.
        """
        with self._db_lock:
            rows = self._archive
            self._archive = []
            if not rows:
                self._truncate_memory()
                return
            try:
                self._executemany(_INSERT_DISK_SQL, rows, chunk_size=500)
                self._truncate_memory()
                self._logger.debug(
                    "Flushed %d plugin run(s) to plugin_event_log.", len(rows)
                )
            except Exception:
                # Put rows back so the next flush can retry. Leave MEMORY as-is.
                self._archive = rows + self._archive
                self._logger.error(
                    "Failed to flush plugin event log to disk.\n\n%s",
                    traceback.format_exc(),
                )
                raise

    def _open_connection(self):
        if self._connect_factory is not None:
            return self._connect_factory()
        try:
            import MySQLdb
        except ImportError:
            raise ImportError(
                "MySQLdb is required for database logging. "
                "Install mysqlclient or disable [database_log] in the config."
            )
        conn = MySQLdb.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            passwd=self._password,
            db=self._database,
            charset="utf8mb4",
            use_unicode=True,
        )
        conn.autocommit(True)
        return conn

    def _close_connection(self):
        conn = self._conn
        self._conn = None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def _reconnect(self):
        self._close_connection()
        self._conn = self._open_connection()
        return self._conn

    def _writer_loop(self):
        while not self._stop.is_set():
            batch = self._collect_batch(timeout=1.0)
            if batch:
                self._write_batch(batch)
        batch = self._collect_batch(timeout=0)
        if batch:
            self._write_batch(batch)

    def _flush_loop(self):
        while not self._stop.wait(self._flush_interval):
            try:
                self._drain_queue_to_memory()
                self.flush_to_disk()
            except Exception:
                self._logger.error(
                    "Periodic database log flush failed.\n\n%s",
                    traceback.format_exc(),
                )

    def _collect_batch(self, timeout):
        batch = []
        try:
            if timeout:
                item = self._queue.get(timeout=timeout)
            else:
                item = self._queue.get_nowait()
            if item is not self._wakeup:
                batch.append(item)
        except queue.Empty:
            return batch
        limit = max(1, self._batch_size)
        while len(batch) < limit:
            try:
                item = self._queue.get_nowait()
                if item is not self._wakeup:
                    batch.append(item)
            except queue.Empty:
                break
        return batch

    def _write_batch(self, batch):
        mem_rows = [self._memory_row(row) for row in batch]
        try:
            with self._db_lock:
                self._executemany(_INSERT_MEM_SQL, mem_rows)
                self._archive.extend(batch)
        except Exception:
            self._logger.error(
                "Failed to insert %d plugin run(s) into plugin_event_log_mem.\n\n%s",
                len(batch),
                traceback.format_exc(),
            )
            # Keep the full rows so a later flush still has something to archive
            # even if MEMORY insert failed (e.g. table full). Try an emergency
            # flush; if that also fails the rows stay in _archive.
            with self._db_lock:
                self._archive.extend(batch)
            if self._flush_in_daemon:
                try:
                    self.flush_to_disk()
                except Exception:
                    pass

    def _drain_queue_to_memory(self):
        while True:
            batch = self._collect_batch(timeout=0)
            if not batch:
                return
            self._write_batch(batch)

    def _memory_row(self, row):
        event_id, plugin_name, started_at, duration_us, completed_at, output = row
        return (
            event_id,
            plugin_name,
            started_at,
            duration_us,
            completed_at,
            _truncate_output(output, MEMORY_OUTPUT_MAX_CHARS),
        )

    def _truncate_memory(self):
        self._execute(_TRUNCATE_MEM_SQL)

    def _execute(self, sql, params=None):
        self._run_with_retry(lambda cursor: cursor.execute(sql, params or ()))

    def _executemany(self, sql, rows, chunk_size=None):
        if not rows:
            return
        chunks = [rows]
        if chunk_size:
            chunks = [rows[i : i + chunk_size] for i in range(0, len(rows), chunk_size)]

        def _run(cursor):
            for chunk in chunks:
                cursor.executemany(sql, chunk)

        self._run_with_retry(_run)

    def _run_with_retry(self, fn):
        last_err = None
        for attempt in range(2):
            try:
                conn = self._conn
                if conn is None:
                    conn = self._reconnect()
                cursor = conn.cursor()
                try:
                    fn(cursor)
                finally:
                    cursor.close()
                return
            except Exception as err:
                last_err = err
                try:
                    self._reconnect()
                except Exception:
                    pass
        raise last_err


def flush_memory_table_sql():
    """SQL used when MariaDB itself copies the MEMORY buffer to InnoDB."""
    return _FLUSH_MEM_TO_DISK_SQL
