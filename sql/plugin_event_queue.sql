-- Additive migration: live plugin work-queue snapshot.
-- Safe to run on existing installs that already have plugin_run_stats.
--
--   mysql -u root -p shotgun_events < sql/plugin_event_queue.sql
--
-- Grant the daemon account write access if it was created before this table:
--   GRANT SELECT, INSERT, DELETE ON shotgun_events.plugin_event_queue
--     TO 'shotgun_events'@'localhost';
--   FLUSH PRIVILEGES;

USE shotgun_events;

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
