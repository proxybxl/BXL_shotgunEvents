<?php
declare(strict_types=1);

require_once __DIR__ . '/db.php';

header('Content-Type: application/json');

// Duration columns are stored in microseconds; the dashboard shows milliseconds.
function metric_definitions(): array
{
    return [
        'run_count' => ['column' => 'run_count', 'label' => 'Run Count', 'unit' => '', 'scale' => 1.0],
        'duration_us_min' => ['column' => 'duration_us_min', 'label' => 'Min Duration', 'unit' => 'ms', 'scale' => 0.001],
        'duration_us_max' => ['column' => 'duration_us_max', 'label' => 'Max Duration', 'unit' => 'ms', 'scale' => 0.001],
        'duration_us_avg' => ['column' => 'duration_us_avg', 'label' => 'Avg Duration', 'unit' => 'ms', 'scale' => 0.001],
    ];
}

function range_definitions(): array
{
    return [
        '1h' => 1,
        '6h' => 6,
        '24h' => 24,
        '7d' => 24 * 7,
        '30d' => 24 * 30,
    ];
}

function respond_error(int $status, string $message): void
{
    http_response_code($status);
    echo json_encode(['error' => $message]);
    exit;
}

$action = $_GET['action'] ?? '';

try {
    $pdo = get_pdo();
} catch (Throwable $e) {
    respond_error(500, 'Database connection failed.');
    exit;
}

if ($action === 'plugins') {
    $stmt = $pdo->query('SELECT DISTINCT plugin_name FROM plugin_run_stats ORDER BY plugin_name ASC');
    echo json_encode(['plugins' => $stmt->fetchAll(PDO::FETCH_COLUMN)]);
    exit;
}

if ($action === 'stats') {
    $metrics = metric_definitions();
    $ranges = range_definitions();

    $metricKey = (string) ($_GET['metric'] ?? 'run_count');
    if (!isset($metrics[$metricKey])) {
        respond_error(400, 'Unknown metric.');
    }
    $metric = $metrics[$metricKey];

    $rangeKey = (string) ($_GET['range'] ?? '24h');
    if (!isset($ranges[$rangeKey])) {
        respond_error(400, 'Unknown range.');
    }
    $hours = $ranges[$rangeKey];

    $pluginsParam = trim((string) ($_GET['plugins'] ?? ''));
    $requestedPlugins = $pluginsParam === ''
        ? []
        : array_values(array_filter(array_map('trim', explode(',', $pluginsParam)), fn($v) => $v !== ''));

    $sql = 'SELECT period_start, plugin_name, ' . $metric['column'] . ' AS value '
        . 'FROM plugin_run_stats '
        . 'WHERE period_start >= (UTC_TIMESTAMP() - INTERVAL :hours HOUR)';

    if ($requestedPlugins) {
        $placeholders = [];
        foreach (array_keys($requestedPlugins) as $i) {
            $placeholders[] = ":p{$i}";
        }
        $sql .= ' AND plugin_name IN (' . implode(',', $placeholders) . ')';
    }
    $sql .= ' ORDER BY period_start ASC';

    $stmt = $pdo->prepare($sql);
    $stmt->bindValue(':hours', $hours, PDO::PARAM_INT);
    foreach ($requestedPlugins as $i => $name) {
        $stmt->bindValue(":p{$i}", $name, PDO::PARAM_STR);
    }
    $stmt->execute();
    $rows = $stmt->fetchAll();

    $labelSet = [];
    $byPlugin = [];
    foreach ($rows as $row) {
        $label = $row['period_start'];
        $labelSet[$label] = true;
        $byPlugin[$row['plugin_name']][$label] = round(((float) $row['value']) * $metric['scale'], 3);
    }
    $labels = array_keys($labelSet);
    sort($labels);

    $series = [];
    foreach ($byPlugin as $pluginName => $values) {
        $data = [];
        foreach ($labels as $label) {
            $data[] = $values[$label] ?? null;
        }
        $series[] = ['plugin' => $pluginName, 'data' => $data];
    }

    echo json_encode([
        'metric' => $metricKey,
        'label' => $metric['label'],
        'unit' => $metric['unit'],
        'labels' => $labels,
        'series' => $series,
    ]);
    exit;
}

if ($action === 'queue') {
    try {
        $stmt = $pdo->query(
            'SELECT plugin_name, event_id, event_type, attribute_name, '
            . 'entity_type, entity_id, entity_name, project_id, project_name, '
            . 'status, pending_count, queued_at, started_at, reported_at '
            . 'FROM plugin_event_queue '
            . 'ORDER BY FIELD(status, \'processing\', \'pending\'), '
            . 'plugin_name ASC, event_id ASC'
        );
        $rows = $stmt->fetchAll();
    } catch (Throwable $e) {
        respond_error(500, 'Queue table is missing. Apply sql/plugin_event_queue.sql.');
    }

    $processing = [];
    $pending = [];
    $pendingByPlugin = [];
    $plugins = [];
    $reportedAt = null;
    foreach ($rows as $row) {
        $plugin = (string) $row['plugin_name'];
        $plugins[$plugin] = true;
        $pendingByPlugin[$plugin] = max(
            $pendingByPlugin[$plugin] ?? 0,
            (int) $row['pending_count']
        );
        if ($row['reported_at'] !== null && ($reportedAt === null || $row['reported_at'] > $reportedAt)) {
            $reportedAt = $row['reported_at'];
        }
        if ($row['status'] === 'processing') {
            $processing[] = $row;
        } else {
            $pending[] = $row;
        }
    }

    echo json_encode([
        'processing' => $processing,
        'pending' => $pending,
        'reported_at' => $reportedAt,
        'counts' => [
            'processing' => count($processing),
            'pending' => array_sum($pendingByPlugin),
            'plugins' => count($plugins),
        ],
    ]);
    exit;
}

respond_error(400, 'Unknown action.');
