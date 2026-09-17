"""
Unit tests for MariaDB plugin stats and event logging.
"""

import datetime
import logging
import threading
import unittest
from unittest import mock

import db_logger


class FakeCursor(object):
    def __init__(self, connection):
        self.connection = connection
        self.closed = False

    def execute(self, sql, params=None):
        self.connection.statements.append((sql, params))

    def executemany(self, sql, rows):
        self.connection.statements.append((sql, list(rows)))

    def close(self):
        self.closed = True


class FakeConnection(object):
    def __init__(self):
        self.statements = []
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


class FailingConnection(FakeConnection):
    def __init__(self, fail_sql_substrings=None, fail_times=1):
        super().__init__()
        self.fail_sql_substrings = fail_sql_substrings or []
        self.fail_times = fail_times
        self._failures = 0

    def cursor(self):
        return FailingCursor(self)


class FailingCursor(FakeCursor):
    def execute(self, sql, params=None):
        self._maybe_fail(sql)
        super().execute(sql, params)

    def executemany(self, sql, rows):
        self._maybe_fail(sql)
        super().executemany(sql, rows)

    def _maybe_fail(self, sql):
        for needle in self.connection.fail_sql_substrings:
            if needle in sql and self.connection._failures < self.connection.fail_times:
                self.connection._failures += 1
                raise RuntimeError("simulated db error")


class TableFullError(Exception):
    def __init__(self):
        super().__init__(1114, "The table 'plugin_event_log_mem' is full")


class TableFullConnection(FakeConnection):
    def __init__(self):
        super().__init__()
        self.mem_inserts = 0

    def cursor(self):
        return TableFullCursor(self)


class TableFullCursor(FakeCursor):
    def executemany(self, sql, rows):
        if sql == db_logger._INSERT_MEM_SQL:
            self.connection.mem_inserts += 1
            if self.connection.mem_inserts == 1:
                raise TableFullError()
        super().executemany(sql, rows)


def _config(**overrides):
    values = {
        "host": "localhost",
        "port": "3306",
        "database": "shotgun_events",
        "user": "shotgun_events",
        "password": "secret",
        "flush_interval_seconds": "60",
        "batch_size": "10",
        "queue_maxsize": "100",
        "max_output_chars": "1048576",
        "flush_in_daemon": "true",
        "flush_row_threshold": "250000",
    }
    values.update(overrides)

    cfg = mock.MagicMock()
    cfg.has_option.side_effect = lambda section, option: option in values
    cfg.get.side_effect = lambda section, option: values[option]
    cfg.getint.side_effect = lambda section, option: int(values[option])
    cfg.getboolean.side_effect = lambda section, option: values[option] in (
        "true",
        "True",
        "1",
        "yes",
        "on",
    )
    return cfg


class TestOutputCaptureHandler(unittest.TestCase):
    def test_captures_logger_and_child_output_only_while_active(self):
        logger = logging.getLogger("plugin.capture_test")
        logger.handlers = []
        logger.setLevel(logging.INFO)
        logger.propagate = False
        child = logging.getLogger("plugin.capture_test.callback")
        child.setLevel(logging.INFO)
        child.propagate = True

        handler = db_logger.OutputCaptureHandler()
        logger.addHandler(handler)

        logger.info("before")
        child.info("before child")
        handler.begin()
        logger.info("during")
        child.info("during child")
        output = handler.finish()
        logger.info("after")

        self.assertIn("during", output)
        self.assertIn("during child", output)
        self.assertNotIn("before", output)
        self.assertNotIn("after", output)
        self.assertIsNone(handler.finish())

        logger.removeHandler(handler)

    def test_finish_returns_none_when_empty(self):
        handler = db_logger.OutputCaptureHandler()
        handler.begin()
        self.assertIsNone(handler.finish())


class TestDatabaseLogger(unittest.TestCase):
    def setUp(self):
        self.conn = FakeConnection()
        self.logger = logging.getLogger("test.db_logger")
        self.logger.handlers = []
        self.logger.addHandler(logging.NullHandler())

    def _make_logger(self, **overrides):
        dbl = db_logger.DatabaseLogger(
            _config(**overrides),
            self.logger,
            connect=lambda: self.conn,
        )
        dbl.start()
        return dbl

    def _wait_for_writer(self, dbl, predicate, timeout=2.0):
        deadline = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        while datetime.datetime.now() < deadline:
            if predicate():
                return
            threading.Event().wait(0.01)
        self.fail("timed out waiting for database writer")

    def test_successful_run_writes_stats_not_event_log(self):
        dbl = self._make_logger()
        try:
            started = datetime.datetime(2026, 9, 16, 8, 0, 0)
            completed = datetime.datetime(2026, 9, 16, 8, 0, 0, 5000)
            dbl.log_plugin_run(
                42, "datestamp", started, 5000, completed, "should not persist"
            )
            self._wait_for_writer(
                dbl,
                lambda: any(
                    sql == db_logger._INSERT_MEM_SQL for sql, _ in self.conn.statements
                ),
            )
            mem = [
                params
                for sql, params in self.conn.statements
                if sql == db_logger._INSERT_MEM_SQL
            ]
            events = [
                params
                for sql, params in self.conn.statements
                if sql == db_logger._INSERT_EVENT_SQL
            ]
            self.assertEqual(len(mem), 1)
            self.assertEqual(mem[0][0], ("datestamp", 5000, 0))
            self.assertEqual(events, [])
        finally:
            dbl.shutdown()

    def test_error_writes_stats_and_event_log(self):
        dbl = self._make_logger()
        try:
            started = datetime.datetime(2026, 9, 16, 8, 0, 0)
            completed = datetime.datetime(2026, 9, 16, 8, 0, 0, 12)
            dbl.log_plugin_run(
                7,
                "calc_field",
                started,
                12,
                completed,
                "traceback here",
                had_error=True,
            )
            self._wait_for_writer(
                dbl,
                lambda: any(
                    sql == db_logger._INSERT_EVENT_SQL for sql, _ in self.conn.statements
                ),
            )
            events = [
                params
                for sql, params in self.conn.statements
                if sql == db_logger._INSERT_EVENT_SQL
            ]
            self.assertEqual(len(events), 1)
            row = events[0][0]
            self.assertEqual(row[0], 7)
            self.assertEqual(row[1], "calc_field")
            self.assertEqual(row[5], 1)
            self.assertEqual(row[6], "error")
            self.assertEqual(row[7], "traceback here")
        finally:
            dbl.shutdown()

    def test_opt_in_event_log_uses_plugin_reason(self):
        dbl = self._make_logger()
        try:
            started = datetime.datetime(2026, 9, 16, 8, 0, 0)
            completed = datetime.datetime(2026, 9, 16, 8, 0, 0, 9)
            dbl.log_plugin_run(
                3,
                "logArgs",
                started,
                9,
                completed,
                "event dict",
                event_log=True,
            )
            self._wait_for_writer(
                dbl,
                lambda: any(
                    sql == db_logger._INSERT_EVENT_SQL for sql, _ in self.conn.statements
                ),
            )
            events = [
                params
                for sql, params in self.conn.statements
                if sql == db_logger._INSERT_EVENT_SQL
            ]
            self.assertEqual(events[0][0][6], "plugin")
            self.assertEqual(events[0][0][5], 0)
        finally:
            dbl.shutdown()

    def test_flush_aggregates_memory_into_stats(self):
        dbl = self._make_logger()
        try:
            dbl.log_plugin_run(
                1,
                "a",
                datetime.datetime(2026, 9, 16, 8, 0, 0),
                10,
                datetime.datetime(2026, 9, 16, 8, 0, 0, 10),
            )
            self._wait_for_writer(dbl, lambda: dbl._mem_rows >= 1)
            dbl.flush_to_disk()
            aggregates = [
                (sql, params)
                for sql, params in self.conn.statements
                if sql == db_logger._AGGREGATE_MEM_SQL
            ]
            truncates = [
                sql
                for sql, _ in self.conn.statements
                if sql == db_logger._TRUNCATE_MEM_SQL
            ]
            self.assertEqual(len(aggregates), 1)
            self.assertTrue(aggregates[0][1][0])
            self.assertGreaterEqual(len(truncates), 1)
            self.assertEqual(dbl._mem_rows, 0)
        finally:
            dbl.shutdown()

    def test_writes_continue_after_flush(self):
        dbl = self._make_logger()
        try:
            started = datetime.datetime(2026, 9, 16, 8, 0, 0)
            completed = datetime.datetime(2026, 9, 16, 8, 0, 0, 10)
            dbl.log_plugin_run(1, "a", started, 10, completed)
            self._wait_for_writer(dbl, lambda: dbl._mem_rows >= 1)
            dbl.flush_to_disk()
            self.assertEqual(dbl._mem_rows, 0)
            dbl.log_plugin_run(2, "a", started, 20, completed)
            self._wait_for_writer(dbl, lambda: dbl._mem_rows >= 1)
            mem = [
                params
                for sql, params in self.conn.statements
                if sql == db_logger._INSERT_MEM_SQL
            ]
            self.assertEqual(len(mem), 2)
            self.assertEqual(mem[1][0], ("a", 20, 0))
        finally:
            dbl.shutdown()

    def test_queue_full_drops_without_raising(self):
        dbl = db_logger.DatabaseLogger(
            _config(queue_maxsize="1"),
            self.logger,
            connect=lambda: self.conn,
        )
        started = datetime.datetime(2026, 9, 16, 8, 0, 0)
        completed = datetime.datetime(2026, 9, 16, 8, 0, 0, 1)
        dbl.log_plugin_run(1, "a", started, 1, completed)
        dbl.log_plugin_run(2, "b", started, 1, completed)
        self.assertGreaterEqual(dbl._dropped, 1)

    def test_naive_utc_converts_aware_datetime(self):
        aware = datetime.datetime(2026, 9, 16, 8, 0, 0, tzinfo=datetime.timezone.utc)
        naive = db_logger._naive_utc(aware)
        self.assertIsNone(naive.tzinfo)
        self.assertEqual(naive, datetime.datetime(2026, 9, 16, 8, 0, 0))

    def test_attach_capture_adds_handler(self):
        dbl = self._make_logger()
        try:
            plugin_logger = logging.getLogger("plugin.attach_test")
            plugin_logger.handlers = []
            handler = dbl.attach_capture(plugin_logger)
            self.assertIsInstance(handler, db_logger.OutputCaptureHandler)
            self.assertIn(handler, plugin_logger.handlers)
            plugin_logger.removeHandler(handler)
        finally:
            dbl.shutdown()

    def test_flush_failure_raises(self):
        self.conn = FailingConnection(
            fail_sql_substrings=["INSERT INTO plugin_run_stats"], fail_times=99
        )
        dbl = self._make_logger()
        try:
            dbl.log_plugin_run(
                1,
                "a",
                datetime.datetime(2026, 9, 16, 8, 0, 0),
                10,
                datetime.datetime(2026, 9, 16, 8, 0, 0, 10),
            )
            self._wait_for_writer(dbl, lambda: dbl._mem_rows >= 1)
            with self.assertRaises(RuntimeError):
                dbl.flush_to_disk()
        finally:
            dbl._flush_in_daemon = False
            dbl.shutdown()

    def test_table_full_aggregates_then_retries(self):
        self.conn = TableFullConnection()
        dbl = self._make_logger()
        try:
            dbl.log_plugin_run(
                9,
                "datestamp",
                datetime.datetime(2026, 9, 16, 8, 0, 0),
                50,
                datetime.datetime(2026, 9, 16, 8, 0, 0, 50),
            )
            deadline = datetime.datetime.now() + datetime.timedelta(seconds=2)
            while datetime.datetime.now() < deadline:
                mem_inserts = [
                    sql
                    for sql, _ in self.conn.statements
                    if sql == db_logger._INSERT_MEM_SQL
                ]
                aggregates = [
                    sql
                    for sql, _ in self.conn.statements
                    if sql == db_logger._AGGREGATE_MEM_SQL
                ]
                if len(mem_inserts) >= 1 and aggregates:
                    self.assertGreaterEqual(self.conn.mem_inserts, 2)
                    return
                threading.Event().wait(0.01)
            self.fail("timed out waiting for table-full recovery")
        finally:
            dbl.shutdown()


class TestSqlHelpers(unittest.TestCase):
    def test_flush_sql_aggregates_memory_to_stats(self):
        sql = db_logger.flush_memory_table_sql()
        self.assertIn("INSERT INTO plugin_run_stats", sql)
        self.assertIn("FROM plugin_event_log_mem", sql)
        self.assertIn("GROUP BY plugin_name", sql)


if __name__ == "__main__":
    unittest.main()
