-- plugin_event_log.sql
-- MariaDB schema for shotgunEventDaemon plugin stats and event logging.
--
-- plugin_event_log_mem   MEMORY buffer of compact per-run samples (no output)
-- plugin_run_stats       InnoDB per-plugin per-minute aggregates
-- plugin_event_log       InnoDB event log: errors, plus plugins that opt in
--
-- Every run that actually invokes a callback is inserted into the MEMORY
-- table (plugin name, duration, error flag only). Once a minute the daemon
-- aggregates that buffer into plugin_run_stats (run count, error count,
-- min/avg/max duration) and truncates the buffer.
--
-- plugin_event_log is NOT a full trace. A row is written only when a
-- callback raises, or when the plugin calls reg.enableDatabaseEventLog().
--
-- Apply:
--   mysql -u root -p < sql/plugin_event_log.sql
-- Existing installs (old per-run archive schema):
--   mysql -u root -p shotgun_events < sql/plugin_event_log_recreate_mem.sql
--
-- Daemon account (TRUNCATE on the MEMORY table requires DROP):
--   CREATE USER 'shotgun_events'@'localhost' IDENTIFIED BY 'change_me';
--   GRANT SELECT, INSERT, DELETE, DROP ON shotgun_events.plugin_event_log_mem
--     TO 'shotgun_events'@'localhost';
--   GRANT SELECT, INSERT, UPDATE ON shotgun_events.plugin_run_stats
--     TO 'shotgun_events'@'localhost';
--   GRANT SELECT, INSERT ON shotgun_events.plugin_event_log
--     TO 'shotgun_events'@'localhost';
--   FLUSH PRIVILEGES;
--
-- Compact samples are ~300 bytes/row. At ~2000 runs/sec a one-minute buffer
-- is ~40MB. 512MB max_heap_table_size is ample:
--   SET GLOBAL max_heap_table_size = 512M;
--   SET GLOBAL tmp_table_size = 512M;
-- Recreate plugin_event_log_mem after changing the heap cap.

CREATE DATABASE IF NOT EXISTS shotgun_events
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE shotgun_events;

-- ---------------------------------------------------------------------------
-- Hot buffer: one compact row per invoked callback run. No plugin_output.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS plugin_event_log_mem (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    plugin_name     VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    duration_us     BIGINT UNSIGNED NOT NULL,
    had_error       TINYINT(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    INDEX idx_plugin_name (plugin_name)
) ENGINE=MEMORY
  DEFAULT CHARSET=ascii
  COLLATE=ascii_bin;

-- ---------------------------------------------------------------------------
-- Per-plugin statistics for each flush window (default: one minute).
-- UNIQUE (period_start, plugin_name) so a mid-window emergency flush can
-- merge into the same minute instead of duplicating it.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS plugin_run_stats (
    id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    period_start        DATETIME NOT NULL,
    plugin_name         VARCHAR(255) NOT NULL,
    run_count           INT UNSIGNED NOT NULL,
    error_count         INT UNSIGNED NOT NULL DEFAULT 0,
    duration_us_min     BIGINT UNSIGNED NOT NULL,
    duration_us_max     BIGINT UNSIGNED NOT NULL,
    duration_us_sum     BIGINT UNSIGNED NOT NULL,
    duration_us_avg     BIGINT UNSIGNED NOT NULL,
    archived_at         DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE KEY uq_period_plugin (period_start, plugin_name),
    INDEX idx_plugin_period (plugin_name, period_start)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- Event log: errors always, plus plugins that called enableDatabaseEventLog().
-- log_reason is 'error' or 'plugin'.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS plugin_event_log (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    event_id        BIGINT NOT NULL,
    plugin_name     VARCHAR(255) NOT NULL,
    started_at      DATETIME(6) NOT NULL,
    duration_us     BIGINT UNSIGNED NOT NULL,
    completed_at    DATETIME(6) NOT NULL,
    had_error       TINYINT(1) NOT NULL DEFAULT 0,
    log_reason      VARCHAR(16) NOT NULL,
    plugin_output   LONGTEXT NULL,
    archived_at     DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    INDEX idx_event_id (event_id),
    INDEX idx_plugin_name (plugin_name),
    INDEX idx_started_at (started_at),
    INDEX idx_had_error (had_error),
    INDEX idx_plugin_started (plugin_name, started_at)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;

-- Optional server-side aggregate. Leave DISABLED; the daemon flushes.
DELIMITER $$

CREATE EVENT IF NOT EXISTS ev_flush_plugin_event_log
ON SCHEDULE EVERY 1 MINUTE
STARTS CURRENT_TIMESTAMP
ON COMPLETION PRESERVE
DISABLE
DO
BEGIN
    INSERT INTO plugin_run_stats (
        period_start,
        plugin_name,
        run_count,
        error_count,
        duration_us_min,
        duration_us_max,
        duration_us_sum,
        duration_us_avg
    )
    SELECT
        DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%d %H:%i:00'),
        plugin_name,
        COUNT(*),
        COALESCE(SUM(had_error), 0),
        MIN(duration_us),
        MAX(duration_us),
        SUM(duration_us),
        ROUND(AVG(duration_us))
    FROM plugin_event_log_mem
    GROUP BY plugin_name
    ON DUPLICATE KEY UPDATE
        duration_us_avg = ROUND(
            (duration_us_sum + VALUES(duration_us_sum))
            / (run_count + VALUES(run_count))
        ),
        run_count = run_count + VALUES(run_count),
        error_count = error_count + VALUES(error_count),
        duration_us_min = LEAST(duration_us_min, VALUES(duration_us_min)),
        duration_us_max = GREATEST(duration_us_max, VALUES(duration_us_max)),
        duration_us_sum = duration_us_sum + VALUES(duration_us_sum);

    TRUNCATE TABLE plugin_event_log_mem;
END$$

DELIMITER ;
