<?php
// Database connection settings for the plugin stats dashboard.
// Match these to the [database_log] section of shotgunEventDaemon.conf.
// Override any of them with environment variables of the same name if you
// don't want credentials sitting in this file.

define('DB_HOST', getenv('SG_DASHBOARD_DB_HOST') ?: 'localhost');
define('DB_PORT', (int) (getenv('SG_DASHBOARD_DB_PORT') ?: 3306));
define('DB_NAME', getenv('SG_DASHBOARD_DB_NAME') ?: 'shotgun_events');
define('DB_USER', getenv('SG_DASHBOARD_DB_USER') ?: 'shotgun_events');
define('DB_PASSWORD', getenv('SG_DASHBOARD_DB_PASSWORD') ?: 'change_me');
