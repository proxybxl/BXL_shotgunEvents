-- Recreate the MEMORY buffer and ensure stats/event-log tables exist.
--
-- Required after the per-run archive schema (rows with event_id/output in
-- MEMORY). Safe while the daemon is stopped. Unflushed MEMORY rows are
-- discarded; plugin_event_log / plugin_run_stats on disk are kept.
--
-- Recreate after raising max_heap_table_size (e.g. 512M); existing MEMORY
-- tables keep the old heap cap until they are dropped and created again.
--
--   mysql -u root -p shotgun_events < sql/plugin_event_log_recreate_mem.sql

USE shotgun_events;

DROP TABLE IF EXISTS plugin_event_log_mem;

CREATE TABLE plugin_event_log_mem (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    plugin_name     VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    duration_us     BIGINT UNSIGNED NOT NULL,
    had_error       TINYINT(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    INDEX idx_plugin_name (plugin_name)
) ENGINE=MEMORY
  DEFAULT CHARSET=ascii
  COLLATE=ascii_bin;

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

-- Older plugin_event_log rows stay. Add columns used by error/opt-in logging.
ALTER TABLE plugin_event_log
    ADD COLUMN IF NOT EXISTS had_error TINYINT(1) NOT NULL DEFAULT 0 AFTER completed_at;

ALTER TABLE plugin_event_log
    ADD COLUMN IF NOT EXISTS log_reason VARCHAR(16) NOT NULL DEFAULT 'plugin' AFTER had_error;

CREATE TABLE IF NOT EXISTS plugin_event_queue (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    plugin_name     VARCHAR(255) NOT NULL,
    event_id        BIGINT NOT NULL,
    event_type      VARCHAR(255) NULL,
    attribute_name  VARCHAR(255) NULL,
    entity_type     VARCHAR(64) NULL,
    entity_id       BIGINT NULL,
    entity_name     VARCHAR(255) NULL,
    project_id      BIGINT NULL,
    project_name    VARCHAR(255) NULL,
    status          ENUM('processing', 'pending') NOT NULL,
    pending_count   INT UNSIGNED NOT NULL DEFAULT 0,
    queued_at       DATETIME(6) NOT NULL,
    started_at      DATETIME(6) NULL,
    reported_at     DATETIME(6) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_plugin_event (plugin_name, event_id),
    INDEX idx_status (status),
    INDEX idx_plugin_status (plugin_name, status),
    INDEX idx_reported_at (reported_at)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;

-- Shotgun/FPT event creation time, for the dashboard's "since generated"
-- latency column - distinct from queued_at (when this daemon enqueued it).
ALTER TABLE plugin_event_queue
    ADD COLUMN IF NOT EXISTS created_at DATETIME(6) NULL AFTER event_id;
