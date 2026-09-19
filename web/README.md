# Plugin Stats Dashboard

A small PHP dashboard for the daemon's MariaDB logging (see
[`sql/plugin_event_log.sql`](../sql/plugin_event_log.sql) and
[`sql/plugin_event_queue.sql`](../sql/plugin_event_queue.sql)):

- **Plugin Stats** charts `plugin_run_stats`: run count, and min/max/avg
  duration (shown in milliseconds, converted from the microseconds stored
  in the table).
- **Event Queue** shows the live `plugin_event_queue` snapshot: the event
  each plugin is currently processing, and the events still waiting on
  that plugin's worker.

## Requirements

- PHP 7.4+ with the `pdo_mysql` extension
- Network access to the MariaDB instance used by the daemon's
  `[database_log]` config

## Setup

1. Edit [`config.php`](config.php) with your database connection details
   (these should match the `[database_log]` section of
   `shotgunEventDaemon.conf`), or set the equivalent
   `SG_DASHBOARD_DB_*` environment variables on the web server.
2. Serve this directory with PHP's built-in server for a quick look:

   ```bash
   php -S localhost:8000 -t web
   ```

   Or point your web server's document root at `web/`.
3. Open `dashboard.php` in a browser.

## Usage

- **Plugin Stats / Event Queue** tabs switch between historical run
  metrics and the live work queue.
- **Metric** selects which column to chart: run count, or min/max/avg
  duration (in ms).
- **Time Range** limits the window of `period_start` rows queried.
- **Plugins** is populated dynamically from the distinct `plugin_name`
  values already recorded in `plugin_run_stats` — no need to hardcode the
  plugin list. Toggle individual plugins or use All/None.
- **Auto-refresh** re-queries stats every 30 seconds, and the queue every
  5 seconds.

Existing installs need the queue table:

```bash
mysql -u root -p shotgun_events < sql/plugin_event_queue.sql
```

## Files

- `config.php` — DB connection settings
- `db.php` — PDO connection helper
- `api.php` — JSON endpoints (`?action=plugins`, `?action=stats`, `?action=queue`)
- `dashboard.php` / `dashboard.js` — the chart and queue UI (Chart.js via CDN)
