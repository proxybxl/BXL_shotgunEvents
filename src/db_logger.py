"""
MariaDB logging for plugin/thread executions.

File logging is unchanged. Every invoked callback run is stored as a compact
sample in a MEMORY table. Once a minute those samples are aggregated per
plugin into plugin_run_stats (run count, error count, min/avg/max duration)
and the buffer is truncated.

A row is written to plugin_event_log only when a callback errors, or when
the plugin has called L{Plugin.enableDatabaseEventLog}.
"""

import datetime
import logging
import queue
import threading
import time
import traceback

# MySQL/MariaDB ER_RECORD_FILE_FULL: MEMORY table exceeded max_heap_table_size.
_ERROR_TABLE_FULL = 1114

_INSERT_MEM_SQL = (
    "INSERT INTO plugin_event_log_mem "
    "(plugin_name, duration_us, had_error) "
    "VALUES (%s, %s, %s)"
)

_INSERT_EVENT_SQL = (
    "INSERT INTO plugin_event_log "
    "(event_id, plugin_name, started_at, duration_us, completed_at, "
    "had_error, log_reason, plugin_output) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
)

_TRUNCATE_MEM_SQL = "TRUNCATE TABLE plugin_event_log_mem"

_AGGREGATE_MEM_SQL = (
    "INSERT INTO plugin_run_stats ("
    "period_start, plugin_name, run_count, error_count, "
    "duration_us_min, duration_us_max, duration_us_sum, duration_us_avg"
    ") "
    "SELECT "
    "%s, plugin_name, COUNT(*), COALESCE(SUM(had_error), 0), "
    "MIN(duration_us), MAX(duration_us), SUM(duration_us), "
    "ROUND(AVG(duration_us)) "
    "FROM plugin_event_log_mem "
    "GROUP BY plugin_name "
    "ON DUPLICATE KEY UPDATE "
    "duration_us_avg = ROUND("
    "(duration_us_sum + VALUES(duration_us_sum)) "
    "/ (run_count + VALUES(run_count))"
    "), "
    "run_count = run_count + VALUES(run_count), "
    "error_count = error_count + VALUES(error_count), "
    "duration_us_min = LEAST(duration_us_min, VALUES(duration_us_min)), "
    "duration_us_max = GREATEST(duration_us_max, VALUES(duration_us_max)), "
    "duration_us_sum = duration_us_sum + VALUES(duration_us_sum)"
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


def _is_table_full(err):
    args = getattr(err, "args", ())
    if args and args[0] == _ERROR_TABLE_FULL:
        return True
    return "is full" in str(err).lower()


def _period_start(dt=None):
    if dt is None:
        dt = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    else:
        dt = _naive_utc(dt)
    return dt.replace(second=0, microsecond=0)


class DatabaseLogger(object):
    """
    Non-blocking logger for per-minute plugin stats and sparse event logs.

    Plugin worker threads call L{log_plugin_run}, which only enqueues. A
    writer thread inserts compact samples into MEMORY and event rows into
    InnoDB. A flusher aggregates MEMORY into plugin_run_stats each minute.

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
            else 60
        )
        self._batch_size = (
            config.getint("database_log", "batch_size")
            if config.has_option("database_log", "batch_size")
            else 1000
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
        self._flush_row_threshold = (
            config.getint("database_log", "flush_row_threshold")
            if config.has_option("database_log", "flush_row_threshold")
            else 250000
        )

        self._queue = queue.Queue(maxsize=self._queue_max)
        self._wakeup = object()
        self._db_lock = threading.Lock()
        self._stop = threading.Event()
        self._conn = None
        self._dropped = 0
        self._mem_rows = 0
        self._window_started_at = None
        self._writer_thread = None

    def start(self):
        """Start the writer thread. The MySQL connection is opened on that thread."""
        if self._writer_thread is not None and self._writer_thread.is_alive():
            return
        self._stop.clear()
        self._window_started_at = _period_start()
        self._writer_thread = threading.Thread(
            target=self._writer_loop, name="DatabaseLogger-writer", daemon=True
        )
        self._writer_thread.start()
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
        had_error=False,
        event_log=False,
    ):
        """
        Enqueue one invoked plugin run. Never blocks the caller for I/O.

        A compact timing sample is always queued for the per-minute stats
        buffer. An event-log row is queued only when C{had_error} or
        C{event_log} is true.
        """
        try:
            duration_us = int(duration_us)
            had_error = 1 if had_error else 0
            event_row = None
            if had_error or event_log:
                reason = "error" if had_error else "plugin"
                event_row = (
                    int(event_id),
                    plugin_name,
                    _naive_utc(started_at),
                    duration_us,
                    _naive_utc(completed_at),
                    had_error,
                    reason,
                    _truncate_output(plugin_output, self._max_output_chars),
                )
            self._queue.put_nowait(
                {
                    "stat": (plugin_name, duration_us, had_error),
                    "event": event_row,
                }
            )
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
        """Stop the writer thread, which drains the queue and flushes stats."""
        self._stop.set()
        try:
            self._queue.put_nowait(self._wakeup)
        except queue.Full:
            pass
        if self._writer_thread is not None:
            self._writer_thread.join(timeout=30)
        if self._dropped:
            self._logger.warning(
                "Database logging dropped %d plugin run(s) because the queue was full.",
                self._dropped,
            )
        self._logger.info("Database logging stopped.")

    def flush_to_disk(self):
        """
        Aggregate MEMORY samples into plugin_run_stats and truncate the buffer.
        """
        with self._db_lock:
            period_start = self._window_started_at or _period_start()
            sample_count = self._mem_rows
            try:
                self._execute(_AGGREGATE_MEM_SQL, (period_start,))
                self._truncate_memory()
                self._mem_rows = 0
                self._window_started_at = _period_start()
                if sample_count:
                    self._logger.info(
                        "Aggregated %d plugin run sample(s) into "
                        "plugin_run_stats for %s.",
                        sample_count,
                        period_start,
                    )
            except Exception:
                self._logger.error(
                    "Failed to aggregate plugin event log to plugin_run_stats.\n\n%s",
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
        # MySQLdb connections are not thread-safe and do not survive fork().
        # Open the connection on this thread and do all SQL here.
        try:
            self._conn = self._open_connection()
        except Exception:
            self._logger.error(
                "Database logger writer could not connect to MariaDB.\n\n%s",
                traceback.format_exc(),
            )
            return
        self._logger.info("Database logger writer connected.")
        next_flush = time.monotonic() + self._flush_interval
        while not self._stop.is_set():
            try:
                remaining = next_flush - time.monotonic()
                if self._flush_in_daemon and remaining <= 0:
                    self.flush_to_disk()
                    next_flush = time.monotonic() + self._flush_interval
                    continue
                timeout = 1.0
                if self._flush_in_daemon:
                    timeout = min(1.0, max(0.05, remaining))
                batch = self._collect_batch(timeout=timeout)
                if batch:
                    self._write_batch(batch)
                    if (
                        self._flush_in_daemon
                        and self._mem_rows >= self._flush_row_threshold
                    ):
                        self.flush_to_disk()
                        next_flush = time.monotonic() + self._flush_interval
            except Exception:
                self._logger.error(
                    "Database logger writer error; continuing.\n\n%s",
                    traceback.format_exc(),
                )
                try:
                    self._reconnect()
                except Exception:
                    pass
        batch = self._collect_batch(timeout=0)
        if batch:
            try:
                self._write_batch(batch)
            except Exception:
                self._logger.error(
                    "Database logger writer error while stopping.\n\n%s",
                    traceback.format_exc(),
                )
        try:
            if self._flush_in_daemon:
                self.flush_to_disk()
        except Exception:
            self._logger.error(
                "Database logger flush failed while stopping.\n\n%s",
                traceback.format_exc(),
            )
        finally:
            self._close_connection()

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
        if not batch:
            return
        stats = []
        events = []
        for item in batch:
            stats.append(item["stat"])
            if item.get("event"):
                events.append(item["event"])
        try:
            with self._db_lock:
                if stats:
                    self._executemany(_INSERT_MEM_SQL, stats)
                    self._mem_rows += len(stats)
                if events:
                    self._executemany(_INSERT_EVENT_SQL, events, chunk_size=100)
        except Exception as err:
            if _is_table_full(err) and self._flush_in_daemon:
                self._recover_table_full(stats, events)
                return
            self._logger.error(
                "Failed to insert plugin log batch (%d stats, %d events).\n\n%s",
                len(stats),
                len(events),
                traceback.format_exc(),
            )
            if self._flush_in_daemon:
                try:
                    self.flush_to_disk()
                except Exception:
                    pass

    def _recover_table_full(self, stats, events):
        try:
            self.flush_to_disk()
            with self._db_lock:
                if stats:
                    self._executemany(_INSERT_MEM_SQL, stats)
                    self._mem_rows += len(stats)
                if events:
                    self._executemany(_INSERT_EVENT_SQL, events, chunk_size=100)
            self._logger.warning(
                "plugin_event_log_mem was full (MySQL error 1114); aggregated "
                "stats to plugin_run_stats and continued."
            )
        except Exception:
            self._logger.error(
                "plugin_event_log_mem is full and recovery failed.\n\n%s",
                traceback.format_exc(),
            )

    def _drain_queue(self):
        while True:
            batch = self._collect_batch(timeout=0)
            if not batch:
                return
            self._write_batch(batch)

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
                if _is_table_full(err):
                    raise
                try:
                    self._reconnect()
                except Exception:
                    pass
        raise last_err


def flush_memory_table_sql():
    """SQL used when MariaDB itself aggregates the MEMORY buffer to stats."""
    return _AGGREGATE_MEM_SQL
