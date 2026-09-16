-- plugin_event_log.sql
-- MariaDB schema for shotgunEventDaemon plugin/thread execution logging.
--
-- Two tables are used so the daemon can absorb hundreds of writes per second
-- without waiting on disk:
--   plugin_event_log_mem  - MEMORY engine, hot buffer (lost on server restart)
--   plugin_event_log      - InnoDB engine, durable archive
--
-- The daemon inserts into the MEMORY table continuously and, every 15 minutes
-- (and on shutdown), copies complete rows -- including full plugin output --
-- into the InnoDB table, then truncates the MEMORY table.
--
-- MEMORY tables cannot store TEXT/BLOB. plugin_output is therefore VARCHAR
-- on the buffer table (truncated) and LONGTEXT on the archive table. The
-- daemon keeps the full output in process memory and writes that to InnoDB
-- on flush, so the archive is not limited by the MEMORY VARCHAR size.
--
-- Apply as a user that can create tables, for example:
--   mysql -u root -p < sql/plugin_event_log.sql
--
-- Daemon account (TRUNCATE on the MEMORY table requires DROP):
--   CREATE USER 'shotgun_events'@'localhost' IDENTIFIED BY 'change_me';
--   GRANT SELECT, INSERT, DELETE, DROP ON shotgun_events.plugin_event_log_mem
--     TO 'shotgun_events'@'localhost';
--   GRANT SELECT, INSERT ON shotgun_events.plugin_event_log
--     TO 'shotgun_events'@'localhost';
--   FLUSH PRIVILEGES;
--
-- Raise the MEMORY cap if the 15-minute buffer can grow large
-- (hundreds of writes/sec * 900s). 256MB is a reasonable starting point:
--   SET GLOBAL max_heap_table_size = 268435456;
--   SET GLOBAL tmp_table_size = 268435456;
-- Persist those in my.cnf / MariaDB config so they survive restart.

CREATE DATABASE IF NOT EXISTS shotgun_events
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE shotgun_events;

-- ---------------------------------------------------------------------------
-- Hot buffer. No extra indexes: inserts are the hot path.
-- DATETIME(6) keeps microsecond start/end times.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS plugin_event_log_mem (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    event_id        BIGINT NOT NULL,
    plugin_name     VARCHAR(255) NOT NULL,
    started_at      DATETIME(6) NOT NULL,
    duration_us     BIGINT UNSIGNED NOT NULL COMMENT 'Plugin/thread execution duration in microseconds',
    completed_at    DATETIME(6) NOT NULL,
    plugin_output   VARCHAR(4096) NULL COMMENT 'Truncated logger output; full text is written to plugin_event_log on flush',
    PRIMARY KEY (id)
) ENGINE=MEMORY
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- Durable archive. Full plugin output is stored as LONGTEXT.
-- archived_at is when the row landed on disk (flush time), not plugin start.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS plugin_event_log (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    event_id        BIGINT NOT NULL,
    plugin_name     VARCHAR(255) NOT NULL,
    started_at      DATETIME(6) NOT NULL,
    duration_us     BIGINT UNSIGNED NOT NULL COMMENT 'Plugin/thread execution duration in microseconds',
    completed_at    DATETIME(6) NOT NULL,
    plugin_output   LONGTEXT NULL,
    archived_at     DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    INDEX idx_event_id (event_id),
    INDEX idx_plugin_name (plugin_name),
    INDEX idx_started_at (started_at),
    INDEX idx_plugin_started (plugin_name, started_at)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- Optional server-side flush. Leave DISABLED: the daemon already flushes
-- every 15 minutes and preserves full LONGTEXT output. Enable this instead
-- of (not in addition to) the daemon flush only if you want MariaDB to copy
-- the MEMORY buffer itself. Rows copied this way have VARCHAR-truncated
-- plugin_output.
--
-- To use it:
--   SET GLOBAL event_scheduler = ON;
--   ALTER EVENT ev_flush_plugin_event_log ENABLE;
-- And set database_log.flush_in_daemon = false in shotgunEventDaemon.conf
-- so the daemon does not also flush (which would duplicate rows).
-- ---------------------------------------------------------------------------
DELIMITER $$

CREATE EVENT IF NOT EXISTS ev_flush_plugin_event_log
ON SCHEDULE EVERY 15 MINUTE
STARTS CURRENT_TIMESTAMP
ON COMPLETION PRESERVE
DISABLE
DO
BEGIN
    INSERT INTO plugin_event_log (
        event_id,
        plugin_name,
        started_at,
        duration_us,
        completed_at,
        plugin_output
    )
    SELECT
        event_id,
        plugin_name,
        started_at,
        duration_us,
        completed_at,
        plugin_output
    FROM plugin_event_log_mem;

    TRUNCATE TABLE plugin_event_log_mem;
END$$

DELIMITER ;
