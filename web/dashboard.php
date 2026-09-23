<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Shotgun Event Daemon &mdash; Plugin Stats</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
<style>
    :root {
        --bg: #14171c;
        --panel: #1c2027;
        --border: #2c313a;
        --text: #e4e6eb;
        --text-muted: #8a919e;
        --accent: #4da3ff;
        --stale: #ff6b6b;
        --processing: #f5a524;
        --pending: #8a919e;
    }
    * {
        box-sizing: border-box;
    }
    body {
        font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
        margin: 24px;
        color: var(--text);
        background: var(--bg);
    }
    h1 {
        font-size: 20px;
        margin: 0;
        color: var(--text);
    }
    .page-header {
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        justify-content: space-between;
        gap: 16px;
        margin-bottom: 16px;
    }
    .view-tabs {
        display: flex;
        gap: 4px;
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 6px;
        padding: 4px;
    }
    .view-tab {
        border: none;
        background: transparent;
        color: var(--text-muted);
        padding: 6px 12px;
        border-radius: 4px;
        font-weight: 600;
    }
    .view-tab:hover {
        color: var(--text);
        border-color: transparent;
    }
    .view-tab.active {
        background: #2a313c;
        color: var(--text);
    }
    .view-panel[hidden] {
        display: none;
    }
    h2.section-title {
        font-size: 14px;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        color: var(--text-muted);
        margin: 20px 0 10px;
    }
    .summary-cards {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        margin-bottom: 16px;
    }
    .summary-card {
        min-width: 140px;
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 6px;
        padding: 12px 16px;
    }
    .summary-card .label {
        font-size: 11px;
        text-transform: uppercase;
        color: var(--text-muted);
        margin-bottom: 4px;
    }
    .summary-card .value {
        font-size: 22px;
        font-weight: 650;
    }
    .queue-table-wrap {
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 6px;
        overflow-x: auto;
    }
    table.queue-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 13px;
    }
    table.queue-table th,
    table.queue-table td {
        text-align: left;
        padding: 8px 10px;
        border-bottom: 1px solid var(--border);
        vertical-align: top;
    }
    table.queue-table th {
        color: var(--text-muted);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.03em;
        font-weight: 600;
    }
    table.queue-table tr:last-child td {
        border-bottom: none;
    }
    table.queue-table tr.processing {
        background: rgba(245, 165, 36, 0.08);
    }
    .badge {
        display: inline-block;
        padding: 2px 7px;
        border-radius: 999px;
        font-size: 11px;
        font-weight: 650;
        text-transform: uppercase;
        letter-spacing: 0.03em;
    }
    .badge.processing {
        color: #1c2027;
        background: var(--processing);
    }
    .badge.pending {
        color: var(--text);
        background: #2a313c;
    }
    .empty-state {
        color: var(--text-muted);
        padding: 20px 16px;
    }
    .more-note {
        color: var(--text-muted);
        font-size: 12px;
        padding: 8px 10px 12px;
    }
    .controls {
        display: flex;
        flex-wrap: wrap;
        align-items: flex-start;
        gap: 24px;
        margin-bottom: 20px;
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 6px;
        padding: 16px;
    }
    .control-group {
        display: flex;
        flex-direction: column;
        gap: 6px;
    }
    .control-group label.title {
        font-size: 12px;
        font-weight: 600;
        text-transform: uppercase;
        color: var(--text-muted);
    }
    #pluginCheckboxes {
        display: flex;
        flex-wrap: wrap;
        gap: 4px 14px;
        max-width: 640px;
        max-height: 120px;
        overflow-y: auto;
    }
    .plugin-toggle {
        font-size: 13px;
        display: flex;
        align-items: center;
        gap: 4px;
        white-space: nowrap;
        color: var(--text);
    }
    .plugin-buttons {
        display: flex;
        gap: 8px;
    }
    select, button {
        font-size: 13px;
        padding: 4px 8px;
        background: var(--panel);
        color: var(--text);
        border: 1px solid var(--border);
        border-radius: 4px;
    }
    select:focus, button:focus {
        outline: 1px solid var(--accent);
    }
    button {
        cursor: pointer;
    }
    button:hover {
        border-color: var(--accent);
    }
    input[type="checkbox"] {
        accent-color: var(--accent);
    }
    em {
        color: var(--text-muted);
    }
    #chartContainer {
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 6px;
        padding: 16px;
    }
    #statsChart {
        max-height: 480px;
    }
    #status, #queueStatus {
        font-size: 12px;
        color: var(--text-muted);
        margin-top: 8px;
    }
    #status.stale, #queueStatus.stale {
        color: var(--stale);
        font-weight: 600;
    }
</style>
</head>
<body>
<div class="page-header">
    <h1>Shotgun Event Daemon</h1>
    <nav class="view-tabs" aria-label="Dashboard views">
        <button type="button" class="view-tab active" data-view="stats">Plugin Stats</button>
        <button type="button" class="view-tab" data-view="queue">Event Queue</button>
    </nav>
</div>

<section id="statsView" class="view-panel">
<div class="controls">
    <div class="control-group">
        <label class="title" for="metric">Metric</label>
        <select id="metric">
            <option value="run_count">Run Count</option>
            <option value="duration_us_avg" selected>Avg Duration (ms)</option>
            <option value="duration_us_min">Min Duration (ms)</option>
            <option value="duration_us_max">Max Duration (ms)</option>
        </select>
    </div>

    <div class="control-group">
        <label class="title" for="range">Time Range</label>
        <select id="range">
            <option value="1h">Last hour</option>
            <option value="6h">Last 6 hours</option>
            <option value="24h" selected>Last 24 hours</option>
            <option value="7d">Last 7 days</option>
            <option value="30d">Last 30 days</option>
        </select>
    </div>

    <div class="control-group">
        <label class="title">Plugins</label>
        <div class="plugin-buttons">
            <button type="button" id="selectAllPlugins">All</button>
            <button type="button" id="selectNonePlugins">None</button>
        </div>
        <div id="pluginCheckboxes"><em>Loading plugins&hellip;</em></div>
    </div>

    <div class="control-group">
        <label class="title">&nbsp;</label>
        <button type="button" id="refreshBtn">Refresh</button>
        <label class="plugin-toggle"><input type="checkbox" id="autoRefresh"> Auto-refresh (30s)</label>
    </div>
</div>

<div id="chartContainer">
    <canvas id="statsChart"></canvas>
    <div id="status"></div>
</div>
</section>

<section id="queueView" class="view-panel" hidden>
    <div class="controls">
        <div class="control-group">
            <label class="title" for="queuePluginFilter">Plugin</label>
            <select id="queuePluginFilter">
                <option value="">All plugins</option>
            </select>
        </div>
        <div class="control-group">
            <label class="title">&nbsp;</label>
            <button type="button" id="queueRefreshBtn">Refresh</button>
            <label class="plugin-toggle"><input type="checkbox" id="queueAutoRefresh" checked> Auto-refresh (5s)</label>
        </div>
    </div>

    <div class="summary-cards">
        <div class="summary-card">
            <div class="label">Processing</div>
            <div class="value" id="queueProcessingCount">0</div>
        </div>
        <div class="summary-card">
            <div class="label">Pending</div>
            <div class="value" id="queuePendingCount">0</div>
        </div>
        <div class="summary-card">
            <div class="label">Busy plugins</div>
            <div class="value" id="queuePluginCount">0</div>
        </div>
    </div>

    <h2 class="section-title">Currently processing</h2>
    <div class="queue-table-wrap" id="queueProcessingWrap"></div>

    <h2 class="section-title">Pending</h2>
    <div class="queue-table-wrap" id="queuePendingWrap"></div>
    <div id="queueStatus"></div>
</section>

<script src="dashboard.js"></script>
</body>
</html>
