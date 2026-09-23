(() => {
    const metricSelect = document.getElementById('metric');
    const rangeSelect = document.getElementById('range');
    const pluginCheckboxes = document.getElementById('pluginCheckboxes');
    const autoRefreshCheckbox = document.getElementById('autoRefresh');
    const refreshBtn = document.getElementById('refreshBtn');
    const statusEl = document.getElementById('status');
    const selectAllBtn = document.getElementById('selectAllPlugins');
    const selectNoneBtn = document.getElementById('selectNonePlugins');
    const viewTabs = Array.from(document.querySelectorAll('.view-tab'));
    const statsView = document.getElementById('statsView');
    const queueView = document.getElementById('queueView');
    const queuePluginFilter = document.getElementById('queuePluginFilter');
    const queueAutoRefreshCheckbox = document.getElementById('queueAutoRefresh');
    const queueRefreshBtn = document.getElementById('queueRefreshBtn');
    const queueStatusEl = document.getElementById('queueStatus');
    const queueProcessingWrap = document.getElementById('queueProcessingWrap');
    const queuePendingWrap = document.getElementById('queuePendingWrap');

    const HUE_STEP = 47;
    const CHART_TEXT_COLOR = '#c4c9d1';
    const CHART_GRID_COLOR = 'rgba(255, 255, 255, 0.08)';

    let chart = null;
    let autoRefreshTimer = null;
    let queueRefreshTimer = null;
    let knownPlugins = [];
    let currentView = 'stats';
    let lastQueueData = null;

    function colorForIndex(i) {
        const hue = (i * HUE_STEP) % 360;
        return `hsl(${hue}, 70%, 62%)`;
    }

    function selectedPlugins() {
        return Array.from(pluginCheckboxes.querySelectorAll('input[type=checkbox]:checked'))
            .map((cb) => cb.value);
    }

    function setStatus(text, stale = false) {
        statusEl.textContent = text;
        statusEl.classList.toggle('stale', stale);
    }

    function formatAge(ms) {
        const totalMinutes = Math.round(ms / 60000);
        if (totalMinutes < 1) return 'just now';
        if (totalMinutes < 60) return `${totalMinutes}m ago`;
        const hours = Math.floor(totalMinutes / 60);
        const minutes = totalMinutes % 60;
        return `${hours}h ${minutes}m ago`;
    }

    async function loadPlugins() {
        try {
            const res = await fetch('api.php?action=plugins');
            const data = await res.json();
            knownPlugins = data.plugins || [];
        } catch (err) {
            setStatus('Failed to load plugin list.');
            return;
        }

        pluginCheckboxes.innerHTML = '';
        if (knownPlugins.length === 0) {
            pluginCheckboxes.innerHTML = '<em>No plugin data yet.</em>';
            return;
        }

        knownPlugins.forEach((name) => {
            const label = document.createElement('label');
            label.className = 'plugin-toggle';

            const cb = document.createElement('input');
            cb.type = 'checkbox';
            cb.value = name;
            cb.checked = true;
            cb.addEventListener('change', loadStats);

            label.appendChild(cb);
            label.appendChild(document.createTextNode(name));
            pluginCheckboxes.appendChild(label);
        });
    }

    function formatLabel(raw, rangeKey) {
        const d = new Date(raw.replace(' ', 'T') + 'Z');
        if (rangeKey === '7d' || rangeKey === '30d') {
            return d.toLocaleString(undefined, {
                month: '2-digit',
                day: '2-digit',
                hour: '2-digit',
                minute: '2-digit',
            });
        }
        return d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' });
    }

    async function loadStats() {
        const metric = metricSelect.value;
        const range = rangeSelect.value;
        const plugins = selectedPlugins();

        const params = new URLSearchParams({ action: 'stats', metric, range });
        if (knownPlugins.length && plugins.length !== knownPlugins.length) {
            params.set('plugins', plugins.join(','));
        }

        setStatus('Loading…');
        let data;
        try {
            const res = await fetch(`api.php?${params.toString()}`);
            if (!res.ok) {
                setStatus('Failed to load stats.');
                return;
            }
            data = await res.json();
        } catch (err) {
            setStatus('Failed to load stats.');
            return;
        }

        renderChart(data, range);
        const labels = data.labels || [];
        const pointCount = labels.length;

        if (pointCount === 0) {
            setStatus(`No data in this range · checked ${new Date().toLocaleTimeString()}`);
            return;
        }

        const latestDate = new Date(labels[labels.length - 1].replace(' ', 'T') + 'Z');
        const ageMs = Date.now() - latestDate.getTime();
        const isStale = ageMs > 5 * 60 * 1000;
        setStatus(
            `${pointCount} data point(s) · latest data: ${latestDate.toLocaleString()} `
            + `(${formatAge(ageMs)}) · checked ${new Date().toLocaleTimeString()}`,
            isStale
        );
    }

    function renderChart(data, rangeKey) {
        const labels = (data.labels || []).map((l) => formatLabel(l, rangeKey));
        const datasets = (data.series || []).map((s, i) => ({
            label: s.plugin,
            data: s.data,
            borderColor: colorForIndex(i),
            backgroundColor: colorForIndex(i),
            spanGaps: true,
            tension: 0.2,
            pointRadius: 1,
            borderWidth: 1.5,
        }));

        const yTitle = data.unit ? `${data.label} (${data.unit})` : data.label;

        if (chart) {
            chart.data.labels = labels;
            chart.data.datasets = datasets;
            chart.options.scales.y.title.text = yTitle;
            chart.update();
            return;
        }

        const ctx = document.getElementById('statsChart').getContext('2d');
        chart = new Chart(ctx, {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                animation: false,
                interaction: { mode: 'nearest', axis: 'x', intersect: false },
                scales: {
                    x: {
                        ticks: { autoSkip: true, maxTicksLimit: 20, color: CHART_TEXT_COLOR },
                        grid: { color: CHART_GRID_COLOR },
                    },
                    y: {
                        beginAtZero: true,
                        title: { display: true, text: yTitle, color: CHART_TEXT_COLOR },
                        ticks: { color: CHART_TEXT_COLOR },
                        grid: { color: CHART_GRID_COLOR },
                    },
                },
                plugins: {
                    legend: { position: 'bottom', labels: { color: CHART_TEXT_COLOR } },
                    tooltip: { titleColor: '#fff', bodyColor: '#fff', backgroundColor: 'rgba(20, 23, 28, 0.95)' },
                },
            },
        });
    }

    function setupAutoRefresh() {
        if (autoRefreshTimer) {
            clearInterval(autoRefreshTimer);
            autoRefreshTimer = null;
        }
        if (currentView === 'stats' && autoRefreshCheckbox.checked) {
            autoRefreshTimer = setInterval(loadStats, 30000);
        }
    }

    function setQueueStatus(text, stale = false) {
        queueStatusEl.textContent = text;
        queueStatusEl.classList.toggle('stale', stale);
    }

    function formatUtc(raw) {
        if (!raw) return '—';
        const d = new Date(String(raw).replace(' ', 'T') + 'Z');
        if (Number.isNaN(d.getTime())) return raw;
        return d.toLocaleString();
    }

    function formatElapsed(raw) {
        if (!raw) return '—';
        const d = new Date(String(raw).replace(' ', 'T') + 'Z');
        if (Number.isNaN(d.getTime())) return '—';
        const seconds = Math.max(0, Math.round((Date.now() - d.getTime()) / 1000));
        if (seconds < 60) return `${seconds}s`;
        const minutes = Math.floor(seconds / 60);
        const rem = seconds % 60;
        if (minutes < 60) return `${minutes}m ${rem}s`;
        const hours = Math.floor(minutes / 60);
        return `${hours}h ${minutes % 60}m`;
    }

    function entityLabel(row) {
        if (!row.entity_type && !row.entity_id && !row.entity_name) return '—';
        const parts = [];
        if (row.entity_type) parts.push(row.entity_type);
        if (row.entity_id) parts.push(`#${row.entity_id}`);
        const name = row.entity_name ? ` — ${row.entity_name}` : '';
        return `${parts.join(' ')}${name}`.trim() || '—';
    }

    function projectLabel(row) {
        if (!row.project_id && !row.project_name) return '—';
        if (row.project_name && row.project_id) return `${row.project_name} (#${row.project_id})`;
        return row.project_name || `#${row.project_id}`;
    }

    function selectedQueuePlugin() {
        return queuePluginFilter.value;
    }

    function updateQueuePluginFilter(data) {
        const names = new Set();
        (data.processing || []).forEach((row) => names.add(row.plugin_name));
        (data.pending || []).forEach((row) => names.add(row.plugin_name));
        const current = queuePluginFilter.value;
        queuePluginFilter.innerHTML = '<option value="">All plugins</option>';
        Array.from(names).sort().forEach((name) => {
            const option = document.createElement('option');
            option.value = name;
            option.textContent = name;
            queuePluginFilter.appendChild(option);
        });
        if (current && names.has(current)) {
            queuePluginFilter.value = current;
        }
    }

    function renderQueueTable(rows, emptyText) {
        if (!rows.length) {
            return `<div class="empty-state">${emptyText}</div>`;
        }
        const body = rows.map((row) => {
            const status = row.status === 'processing' ? 'processing' : 'pending';
            return `<tr class="${status}">
                <td><span class="badge ${status}">${status}</span></td>
                <td>${escapeHtml(row.plugin_name || '')}</td>
                <td>${escapeHtml(String(row.event_id ?? ''))}</td>
                <td>${escapeHtml(row.event_type || '—')}</td>
                <td>${escapeHtml(row.attribute_name || '—')}</td>
                <td>${escapeHtml(entityLabel(row))}</td>
                <td>${escapeHtml(projectLabel(row))}</td>
                <td>${escapeHtml(formatUtc(row.queued_at))}</td>
                <td>${escapeHtml(status === 'processing' ? formatElapsed(row.started_at) : formatElapsed(row.queued_at))}</td>
            </tr>`;
        }).join('');
        return `<table class="queue-table">
            <thead>
                <tr>
                    <th>Status</th>
                    <th>Plugin</th>
                    <th>Event</th>
                    <th>Type</th>
                    <th>Attribute</th>
                    <th>Entity</th>
                    <th>Project</th>
                    <th>Queued</th>
                    <th>${rows[0] && rows[0].status === 'processing' ? 'Running' : 'Waiting'}</th>
                </tr>
            </thead>
            <tbody>${body}</tbody>
        </table>`;
    }

    function escapeHtml(value) {
        return String(value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function applyQueueFilter(rows) {
        const plugin = selectedQueuePlugin();
        if (!plugin) return rows;
        return rows.filter((row) => row.plugin_name === plugin);
    }

    function renderQueue(data) {
        lastQueueData = data;
        updateQueuePluginFilter(data);
        const processing = applyQueueFilter(data.processing || []);
        const pending = applyQueueFilter(data.pending || []);
        const counts = data.counts || {};

        const filteredPlugin = selectedQueuePlugin();
        if (filteredPlugin) {
            const pendingTotal = Number(
                (processing[0] && processing[0].pending_count)
                || (pending[0] && pending[0].pending_count)
                || pending.length
            );
            document.getElementById('queueProcessingCount').textContent = String(processing.length);
            document.getElementById('queuePendingCount').textContent = String(pendingTotal);
            document.getElementById('queuePluginCount').textContent = (
                processing.length || pending.length
            ) ? '1' : '0';
        } else {
            document.getElementById('queueProcessingCount').textContent = String(counts.processing ?? processing.length);
            document.getElementById('queuePendingCount').textContent = String(counts.pending ?? pending.length);
            document.getElementById('queuePluginCount').textContent = String(counts.plugins ?? 0);
        }

        queueProcessingWrap.innerHTML = renderQueueTable(processing, 'Nothing is currently processing.');
        let pendingHtml = renderQueueTable(pending, 'The queue is empty.');
        const shownByPlugin = {};
        pending.forEach((row) => {
            shownByPlugin[row.plugin_name] = (shownByPlugin[row.plugin_name] || 0) + 1;
        });
        const pendingTotals = {};
        (data.processing || []).concat(data.pending || []).forEach((row) => {
            pendingTotals[row.plugin_name] = Number(row.pending_count || 0);
        });
        const uniqueNotes = Object.keys(shownByPlugin)
            .filter((name) => (pendingTotals[name] || 0) > shownByPlugin[name])
            .map((name) => `${name}: showing ${shownByPlugin[name]} of ${pendingTotals[name]} pending`);
        if (uniqueNotes.length) {
            pendingHtml += `<div class="more-note">${uniqueNotes.map(escapeHtml).join(' · ')}</div>`;
        }
        queuePendingWrap.innerHTML = pendingHtml;

        const reportedAt = data.reported_at;
        if (!reportedAt && processing.length === 0 && pending.length === 0) {
            setQueueStatus(`Queue is empty · checked ${new Date().toLocaleTimeString()}`);
            return;
        }
        const latest = reportedAt ? new Date(String(reportedAt).replace(' ', 'T') + 'Z') : null;
        const ageMs = latest ? Date.now() - latest.getTime() : 0;
        const isStale = latest && ageMs > 15000;
        setQueueStatus(
            `Last report: ${latest ? latest.toLocaleString() : 'unknown'} `
            + `(${latest ? formatAge(ageMs) : 'n/a'}) · checked ${new Date().toLocaleTimeString()}`,
            Boolean(isStale)
        );
    }

    async function loadQueue() {
        setQueueStatus('Loading…');
        try {
            const res = await fetch('api.php?action=queue');
            const data = await res.json();
            if (!res.ok) {
                setQueueStatus(data.error || 'Failed to load queue.');
                return;
            }
            renderQueue(data);
        } catch (err) {
            setQueueStatus('Failed to load queue.');
        }
    }

    function setupQueueAutoRefresh() {
        if (queueRefreshTimer) {
            clearInterval(queueRefreshTimer);
            queueRefreshTimer = null;
        }
        if (currentView === 'queue' && queueAutoRefreshCheckbox.checked) {
            queueRefreshTimer = setInterval(loadQueue, 5000);
        }
    }

    function setView(view) {
        currentView = view;
        statsView.hidden = view !== 'stats';
        queueView.hidden = view !== 'queue';
        viewTabs.forEach((tab) => {
            tab.classList.toggle('active', tab.dataset.view === view);
        });
        if (history.replaceState) {
            history.replaceState(null, '', view === 'queue' ? '#queue' : '#stats');
        }
        setupAutoRefresh();
        setupQueueAutoRefresh();
        if (view === 'queue') {
            loadQueue();
        } else {
            loadStats();
        }
    }

    metricSelect.addEventListener('change', loadStats);
    rangeSelect.addEventListener('change', loadStats);
    refreshBtn.addEventListener('click', loadStats);
    autoRefreshCheckbox.addEventListener('change', setupAutoRefresh);
    selectAllBtn.addEventListener('click', () => {
        pluginCheckboxes.querySelectorAll('input[type=checkbox]').forEach((cb) => { cb.checked = true; });
        loadStats();
    });
    selectNoneBtn.addEventListener('click', () => {
        pluginCheckboxes.querySelectorAll('input[type=checkbox]').forEach((cb) => { cb.checked = false; });
        loadStats();
    });
    viewTabs.forEach((tab) => {
        tab.addEventListener('click', () => setView(tab.dataset.view));
    });
    queueRefreshBtn.addEventListener('click', loadQueue);
    queueAutoRefreshCheckbox.addEventListener('change', setupQueueAutoRefresh);
    function rerenderQueue() {
        if (lastQueueData) renderQueue(lastQueueData);
    }
    queuePluginFilter.addEventListener('change', rerenderQueue);
    queuePluginFilter.addEventListener('input', rerenderQueue);

    (async function init() {
        await loadPlugins();
        if (location.hash === '#queue') {
            setView('queue');
            return;
        }
        await loadStats();
    })();
})();
