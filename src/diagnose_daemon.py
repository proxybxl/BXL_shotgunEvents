#
# Copyright (c) 2026 Boxel Studio.
#
# :Module: diagnose_daemon.py
# :Description: Standalone, read-only diagnostic for the ShotGrid event
#               daemon. Reads shotgunEventDaemon.conf next to this file,
#               inspects OS processes and pidfiles (detecting duplicate
#               daemon/supervisor instances), validates host/FQDN site
#               resolution against ShotGrid's BXL_Trigger_hosts config,
#               compares the on-disk eventIdFile checkpoint against the
#               latest EventLogEntry in ShotGrid, queries the daemon's
#               MariaDB logging tables (using mariadb or MySQLdb), and
#               scans daemon/plugin/trigger log files for recent errors.
#
#               Usage (same interpreter the daemon itself runs under):
#                 /srv/bxlpython3/bin/python \
#                     /srv/shotgunEvents-master/src/diagnose_daemon.py
#               Optional flags:
#                 --minutes (stats window, default 60)
#                 --hours (error window, default 24)
#                 --top (rows shown per table, default 10)
#                 --plugin (inspect a specific plugin, e.g.
#                           BXL_version_download_vendor)
#                 --test-plugin (dry-run registration/site check for
#                                --plugin without sending Slack alerts)
#
# :Author: Fidel Moreno Miranda <fidel.moreno@boxelstudio.com>
#

# Standard modules
import argparse
import configparser
import datetime
import glob
import json
import os
import pickle
import socket
import subprocess
import sys
import time
import traceback

# Daemon's own vendored dependencies (shotgun_api3, mariadb/MySQLdb if present).
# Matches the sys.path setup shotgunEventDaemon.py itself uses - this
# script is meant to live in the same directory on the server.
_PACKAGES_DIR = "/srv/shotgunEvents-master/src/site-packages"
if _PACKAGES_DIR not in sys.path and os.path.isdir(_PACKAGES_DIR):
    sys.path.append(_PACKAGES_DIR)

_TRIGGERS_DIR = "/srv/shotgunEvents-master/src/bxl_triggers"
if _TRIGGERS_DIR not in sys.path and os.path.isdir(_TRIGGERS_DIR):
    sys.path.append(_TRIGGERS_DIR)

CONF_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "shotgunEventDaemon.conf"
)

DAEMON_PROCESS_MARKERS = ("shotgunEventDaemon.py", "supervisor.py")
SUPERVISOR_PID_FILE = "/var/log/shotgunEventDaemon/shotgunEventSupervisor.pid"
SUPERVISOR_LOG_FILE = "/var/log/shotgunEventDaemon/shotgunEventSupervisor.log"
TRIGGERS_LOG_ROOT = "/srv/shotgunEvents-master/src/logs"

DEFAULT_SITE_MAP = {
    "Boxel": ["boxel.lan", "No domain found (Localhost/Workgroup)"],
    "Centergrid": ["centergrid.boxel.lan"],
}


def _banner(title):
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def _fmt_ms(duration_us):
    if duration_us is None:
        return "n/a"
    # MariaDB/MySQLdb returns DECIMAL columns (and AVG()/division results) as
    # decimal.Decimal, which can't be divided by a plain float.
    return f"{float(duration_us) / 1000.0:.1f} ms"


def _fmt_seconds(seconds):
    if seconds is None:
        return "n/a"
    seconds = int(seconds)
    if seconds < 0:
        return f"{seconds}s"
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days:
        return f"{days}d{hours:02d}h{minutes:02d}m"
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def read_config(conf_path):
    config = configparser.ConfigParser(os.environ)
    read_files = config.read(conf_path)
    if not read_files:
        raise SystemExit(f"Could not read config file: {conf_path}")
    return config


def _pid_is_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _read_pid_file(path):
    if not path or not os.path.exists(path):
        return None, "missing"
    try:
        with open(path, "r") as fh:
            raw = fh.read().strip()
        pid = int(raw)
        return pid, ("alive" if _pid_is_alive(pid) else "DEAD (stale pidfile)")
    except Exception as exc:
        return None, f"unreadable ({exc})"


def _get_fqdn_domain():
    fqdn = socket.getfqdn()
    if "." in fqdn:
        return fqdn, fqdn.split(".", 1)[1]
    return fqdn, "No domain found (Localhost/Workgroup)"


def check_os_snapshot(config):
    """Load average, memory, pidfiles, and daemon-related processes via ps."""
    _banner("OS & Process Snapshot")

    print(f"Python executable: {sys.executable} ({sys.version.split()[0]})")
    hostname = socket.gethostname()
    fqdn, domain = _get_fqdn_domain()
    default_site = "Unknown"
    for site_name, domains in DEFAULT_SITE_MAP.items():
        if domain in domains:
            default_site = site_name
            break
    print(f"Host: hostname={hostname!r} fqdn={fqdn!r} domain={domain!r}")
    print(f"Default site resolution (from domain): {default_site!r} "
          f"(ENV SITE={os.environ.get('SITE')!r})")

    try:
        with open("/proc/loadavg") as fh:
            load_parts = fh.read().split()
        print(f"Load average (1m/5m/15m): "
              f"{load_parts[0]} / {load_parts[1]} / {load_parts[2]}")
    except Exception as exc:
        print(f"Could not read /proc/loadavg: {exc}")

    try:
        mem_info = {}
        with open("/proc/meminfo") as fh:
            for line in fh:
                key, _, rest = line.partition(":")
                mem_info[key.strip()] = rest.strip()
        total_kb = int(mem_info.get("MemTotal", "0 kB").split()[0])
        avail_kb = int(mem_info.get("MemAvailable", "0 kB").split()[0])
        if total_kb:
            used_pct = 100.0 * (total_kb - avail_kb) / total_kb
            print(f"Memory: {used_pct:.1f}% used "
                  f"({(total_kb - avail_kb) // 1024} MB / {total_kb // 1024} MB)")
    except Exception as exc:
        print(f"Could not read /proc/meminfo: {exc}")

    # Check pidfiles
    daemon_pid_path = config.get("daemon", "pidFile", fallback=None)
    daemon_pid, daemon_pid_status = _read_pid_file(daemon_pid_path)
    sup_pid, sup_pid_status = _read_pid_file(SUPERVISOR_PID_FILE)
    print("\nPID files:")
    print(f"  Daemon pidFile ({daemon_pid_path}): "
          f"pid={daemon_pid} ({daemon_pid_status})")
    print(f"  Supervisor pidFile ({SUPERVISOR_PID_FILE}): "
          f"pid={sup_pid} ({sup_pid_status})")

    try:
        result = subprocess.run(
            ["ps", "-eo", "pid,ppid,etimes,pcpu,pmem,stat,cmd"],
            capture_output=True, text=True, timeout=10)
        matched_lines = []
        daemon_pids = []
        supervisor_pids = []
        for line in result.stdout.splitlines():
            for marker in DAEMON_PROCESS_MARKERS:
                if marker in line and "diagnose_daemon.py" not in line:
                    matched_lines.append(line)
                    parts = line.split(None, 6)
                    if len(parts) >= 7:
                        try:
                            pid_int = int(parts[0])
                            cmd_str = parts[6]
                            if "shotgunEventDaemon.py" in cmd_str:
                                daemon_pids.append(pid_int)
                            elif "supervisor.py" in cmd_str:
                                supervisor_pids.append(pid_int)
                        except ValueError:
                            pass
                    break
        if matched_lines:
            print("\nDaemon-related processes (pid, ppid, uptime, %cpu, %mem, stat, cmd):")
            for line in matched_lines:
                parts = line.split(None, 6)
                if len(parts) < 7:
                    print(f"  {line.strip()}")
                    continue
                pid, ppid, etimes, pcpu, pmem, stat, cmd = parts
                try:
                    uptime = _fmt_seconds(int(etimes))
                except ValueError:
                    uptime = etimes
                print(f"  pid={pid:<8} ppid={ppid:<8} uptime={uptime:<10} "
                      f"cpu={pcpu:>5}% mem={pmem:>5}% stat={stat:<4} {cmd}")

            if len(daemon_pids) > 1:
                print(f"\n  [WARNING] MULTIPLE shotgunEventDaemon.py PROCESSES RUNNING: "
                      f"{daemon_pids}!")
                print("            Two daemons racing over the same eventIdFile can "
                      "cause missed/duplicate events or stale state overwrites.")
                if daemon_pid in daemon_pids:
                    orphans = [p for p in daemon_pids if p != daemon_pid]
                    print(f"            PID in pidFile is {daemon_pid}; "
                          f"orphan/conflicting daemon PID(s): {orphans}")
            if len(supervisor_pids) > 1:
                print(f"  [WARNING] MULTIPLE supervisor.py PROCESSES RUNNING: "
                      f"{supervisor_pids}!")
                if sup_pid in supervisor_pids:
                    orphans = [p for p in supervisor_pids if p != sup_pid]
                    print(f"            PID in supervisor pidFile is {sup_pid}; "
                          f"orphan/conflicting supervisor PID(s): {orphans}")
        else:
            print("\n  [WARNING] No process matching "
                  f"{DAEMON_PROCESS_MARKERS} found via `ps`. "
                  "Check manually with `ps aux | grep shotgunEvent`.")
    except Exception as exc:
        print(f"Could not run `ps`: {exc}")


def check_sg_and_site_config(config):
    """Time a ShotGrid round trip, fetch latest EventLogEntry id, and check BXL_Trigger_hosts.

    Returns:
        tuple: (sg_instance_or_None, latest_event_id_or_None)
    """
    _banner("ShotGrid API & Site Routing Check")
    try:
        import shotgun_api3
        server = config.get("shotgun", "server")
        name = config.get("shotgun", "name")
        key = config.get("shotgun", "key")
        proxy = config.get("shotgun", "proxy_server", fallback="").strip() or None
        sg = shotgun_api3.Shotgun(server, name, key, http_proxy=proxy)

        start = time.monotonic()
        sg.info()
        elapsed_ms = (time.monotonic() - start) * 1000
        print(f"sg.info() round trip to {server}: {elapsed_ms:.1f} ms")
    except Exception:
        print(f"Could not complete the SG round-trip check:\n"
              f"{traceback.format_exc()}")
        return None, None

    latest_event_id = None
    try:
        latest_event = sg.find_one(
            "EventLogEntry",
            filters=[],
            fields=["id", "event_type", "attribute_name", "created_at"],
            order=[{"column": "id", "direction": "desc"}],
        )
        if latest_event:
            latest_event_id = latest_event["id"]
            print(f"Latest EventLogEntry in ShotGrid: id={latest_event_id} "
                  f"type={latest_event.get('event_type')} "
                  f"attr={latest_event.get('attribute_name')} "
                  f"created_at={latest_event.get('created_at')}")
    except Exception as exc:
        print(f"Could not query latest EventLogEntry: {exc}")

    # Check BXL_Trigger_hosts in CustomNonProjectEntity03
    try:
        host_cfg = sg.find_one(
            "CustomNonProjectEntity03",
            [["code", "is", "BXL_Trigger_hosts"]],
            ["code", "sg_data"],
        )
        _fqdn, domain = _get_fqdn_domain()
        if host_cfg and host_cfg.get("sg_data"):
            map_options = json.loads(host_cfg["sg_data"])
            matched_site = None
            for site_name, hosts in map_options.items():
                if isinstance(hosts, list) and domain in hosts:
                    matched_site = site_name
                    break
            print(f"BXL_Trigger_hosts config in SG: {map_options}")
            print(f"Current domain {domain!r} resolves to SG site: "
                  f"{matched_site!r}")
            if matched_site is None:
                print("  [WARNING] Current host domain does NOT match any site in "
                      "BXL_Trigger_hosts! Triggers checking site_execution will "
                      "refuse to register/execute on this server.")
        else:
            print("BXL_Trigger_hosts not found in CustomNonProjectEntity03 "
                  "(using default fallback map).")
    except Exception as exc:
        print(f"Could not check BXL_Trigger_hosts in SG: {exc}")

    return sg, latest_event_id


def check_event_id_file(config, latest_sg_event_id=None, target_plugin=None, top=10):
    """Inspect the daemon's on-disk eventIdFile pickle (read-only).

    Works even when [database_log] / MariaDB is unavailable, showing
    each plugin's last processed event ID, lag behind ShotGrid's latest
    EventLogEntry, and any pending retry backlog IDs.
    """
    _banner("On-Disk Checkpoint & Backlog (eventIdFile)")
    event_id_file = config.get("daemon", "eventIdFile", fallback=None)
    if not event_id_file:
        print("No eventIdFile configured in [daemon].")
        return

    if not os.path.exists(event_id_file):
        print(f"[WARNING] eventIdFile does not exist: {event_id_file}")
        return

    try:
        st = os.stat(event_id_file)
        mtime_dt = datetime.datetime.fromtimestamp(
            st.st_mtime, tz=datetime.timezone.utc
        )
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        age_s = (now_utc - mtime_dt).total_seconds()
        print(f"File: {event_id_file} (size={st.st_size} bytes)")
        print(f"Last saved (UTC): {mtime_dt.isoformat()} "
              f"({_fmt_seconds(age_s)} ago)")
        if age_s > 300:
            print(f"  [WARNING] eventIdFile has not been updated in "
                  f"{_fmt_seconds(age_s)}! Daemon loop may be stuck or stopped.")
    except Exception as exc:
        print(f"Could not stat {event_id_file}: {exc}")

    try:
        with open(event_id_file, "rb") as fh:
            data = pickle.load(fh)
    except Exception as exc:
        print(f"[WARNING] Could not unpickle {event_id_file}: {exc}")
        return

    if not isinstance(data, dict):
        print(f"Legacy or non-dict state in eventIdFile: {data!r}")
        return

    configured_paths = [
        s.strip()
        for s in config.get("plugins", "paths", fallback="").split(",")
        if s.strip()
    ]

    for col_path, plugin_states in data.items():
        in_conf = col_path in configured_paths
        print(f"\nCollection: {col_path} "
              f"({'active in conf' if in_conf else 'NOT in current conf'})")
        if not isinstance(plugin_states, dict) or not plugin_states:
            print("  (no plugin states recorded)")
            continue

        # Check which .py files currently exist on disk in this collection
        disk_plugins = set()
        if os.path.isdir(col_path):
            disk_plugins = {
                os.path.splitext(fn)[0]
                for fn in os.listdir(col_path)
                if fn.endswith(".py") and not fn.startswith(".")
            }

        rows = []
        for plugin_name, state in plugin_states.items():
            last_id = None
            backlog = {}
            if isinstance(state, tuple) and len(state) == 2:
                last_id, backlog = state
            elif isinstance(state, int):
                last_id = state
            if not isinstance(backlog, dict):
                backlog = {}
            lag = (
                (latest_sg_event_id - last_id)
                if (latest_sg_event_id is not None and isinstance(last_id, int))
                else None
            )
            on_disk = plugin_name in disk_plugins if disk_plugins else True
            rows.append((plugin_name, last_id, lag, backlog, on_disk))

        valid_ids = [r[1] for r in rows if isinstance(r[1], int)]
        max_id = max(valid_ids) if valid_ids else None
        min_id = min(valid_ids) if valid_ids else None
        print(f"  Tracked plugins in state file: {len(rows)} "
              f"(on disk now: {len(disk_plugins)})")
        if max_id is not None:
            head_lag = (
                f"{latest_sg_event_id - max_id} behind SG latest ({latest_sg_event_id})"
                if latest_sg_event_id is not None
                else "n/a"
            )
            print(f"  Cursor range across plugins: min={min_id}, max={max_id} "
                  f"(max cursor is {head_lag})")

        # Identify plugins on disk that are missing from state file
        if disk_plugins:
            missing_from_state = sorted(disk_plugins - set(plugin_states.keys()))
            if missing_from_state:
                print(f"  Plugins on disk but not in state file: "
                      f"{missing_from_state}")

        # Show plugins lagging behind max_id or with backlog
        lagging = [
            r for r in rows
            if r[4] and (
                r[3]
                or (max_id is not None and isinstance(r[1], int) and (max_id - r[1]) > 0)
            )
        ]
        lagging.sort(
            key=lambda r: (
                (max_id - r[1]) if (max_id is not None and isinstance(r[1], int)) else -1,
                len(r[3]),
            ),
            reverse=True,
        )
        if lagging:
            print(f"\n  Plugins behind max cursor ({max_id}) or with retry backlog "
                  f"(showing up to {top}):")
            print(f"  {'plugin':<38} {'lastEventId':>12} {'behind_max':>11} "
                  f"{'behind_SG':>10} {'backlog':>8}")
            for pname, lid, lag_sg, bk, _on_disk in lagging[:top]:
                behind_max = (max_id - lid) if (max_id and isinstance(lid, int)) else "n/a"
                lag_str = str(lag_sg) if lag_sg is not None else "n/a"
                print(f"  {pname:<38} {str(lid):>12} {str(behind_max):>11} "
                      f"{lag_str:>10} {len(bk):>8}")
        else:
            print("  All on-disk plugins in this collection are at the same "
                  f"event cursor ({max_id}) with 0 backlog.")

        if target_plugin:
            matched = [
                r for r in rows if target_plugin.lower() in r[0].lower()
            ]
            for pname, lid, lag_sg, bk, on_disk in matched:
                behind_max = (max_id - lid) if (max_id and isinstance(lid, int)) else "n/a"
                print(f"\n  [Target Plugin State] {pname} (on_disk={on_disk}):")
                print(f"    lastEventId={lid} | behind_max={behind_max} | "
                      f"behind_SG={lag_sg} | backlog_count={len(bk)}")
                if bk:
                    sample_items = list(bk.items())[:10]
                    print(f"    backlog sample (event_id: expires_at): {sample_items}")


def _open_db_connection(config):
    """Open a DB connection using mariadb (used by db_logger.py) or MySQLdb/pymysql."""
    host = config.get("database_log", "host")
    port = (config.getint("database_log", "port")
            if config.has_option("database_log", "port") else 3306)
    database = config.get("database_log", "database")
    user = config.get("database_log", "user")
    password = (config.get("database_log", "password")
                if config.has_option("database_log", "password") else "")

    import_errors = []
    try:
        import mariadb
        conn = mariadb.connect(
            host=host, port=port, user=user, password=password, database=database
        )
        return conn, "mariadb"
    except ImportError as exc:
        import_errors.append(f"mariadb ({exc})")

    try:
        import MySQLdb
        conn = MySQLdb.connect(
            host=host, port=port, user=user, passwd=password, db=database,
            charset="utf8mb4", use_unicode=True
        )
        return conn, "MySQLdb"
    except ImportError as exc:
        import_errors.append(f"MySQLdb ({exc})")

    try:
        import pymysql
        conn = pymysql.connect(
            host=host, port=port, user=user, password=password, database=database,
            charset="utf8mb4"
        )
        return conn, "pymysql"
    except ImportError as exc:
        import_errors.append(f"pymysql ({exc})")

    raise ImportError(
        "No MySQL/MariaDB driver available in this Python environment. "
        f"Tried: {', '.join(import_errors)}"
    )


def check_db_backlog(cursor, top, target_plugin=None):
    """Live per-plugin backlog, straight from plugin_event_queue."""
    _banner("Live Backlog (plugin_event_queue)")
    cursor.execute(
        "SELECT plugin_name, status, event_id, event_type, entity_type, "
        "entity_name, pending_count, queued_at, started_at, reported_at "
        "FROM plugin_event_queue "
        "ORDER BY pending_count DESC, started_at ASC")
    rows = cursor.fetchall()
    if not rows:
        print("No rows - every plugin's queue is currently empty.")
        return

    by_plugin = {}
    for row in rows:
        (plugin_name, status, event_id, event_type, entity_type,
         entity_name, pending_count, queued_at, started_at,
         reported_at) = row
        by_plugin.setdefault(plugin_name, {
            "pending_count": pending_count,
            "processing": None,
            "reported_at": reported_at,
        })
        if status == "processing":
            by_plugin[plugin_name]["processing"] = {
                "event_id": event_id,
                "event_type": event_type,
                "entity_type": entity_type,
                "entity_name": entity_name,
                "started_at": started_at,
            }

    ranked = sorted(
        by_plugin.items(), key=lambda kv: kv[1]["pending_count"],
        reverse=True)[:top]

    now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    for plugin_name, info in ranked:
        pending = info["pending_count"]
        reported_age = (
            (now - info["reported_at"]).total_seconds()
            if info["reported_at"] else None)
        print(f"\n{plugin_name}: pending_count={pending} "
              f"(snapshot age: {_fmt_seconds(reported_age)})")
        proc = info["processing"]
        if proc and proc["started_at"]:
            running_for = (now - proc["started_at"]).total_seconds()
            print(f"  currently processing event {proc['event_id']} "
                  f"({proc['event_type']} on {proc['entity_type']} "
                  f"{proc['entity_name']!r}) - running for "
                  f"{_fmt_seconds(running_for)}")
        elif pending:
            print("  nothing actively processing, but events are queued "
                  "(worker may be idle-waiting or just picked up the next one)")


def check_db_stats(cursor, minutes, top, target_plugin=None):
    """Per-plugin timing over the last N minutes, from plugin_run_stats."""
    _banner(f"Plugin Timing - last {minutes} minute(s) (plugin_run_stats)")
    cursor.execute(
        "SELECT plugin_name, SUM(run_count), SUM(error_count), "
        "MIN(duration_us_min), MAX(duration_us_max), "
        "SUM(duration_us_sum) / NULLIF(SUM(run_count), 0) "
        "FROM plugin_run_stats "
        "WHERE period_start >= UTC_TIMESTAMP() - INTERVAL %s MINUTE "
        "GROUP BY plugin_name "
        "ORDER BY MAX(duration_us_max) DESC",
        (minutes,))
    rows = cursor.fetchall()
    if not rows:
        print("No plugin_run_stats rows in this window - either very "
              "quiet, or the daemon hasn't flushed stats yet "
              "(flush_interval_seconds in the conf).")
        return

    print(f"{'plugin':<38} {'runs':>7} {'errors':>7} "
          f"{'avg':>10} {'max':>10}")
    for plugin_name, run_count, error_count, _dmin, dmax, avg in rows[:top]:
        print(f"{plugin_name:<38} {int(run_count or 0):>7} "
              f"{int(error_count or 0):>7} "
              f"{_fmt_ms(avg):>10} {_fmt_ms(dmax):>10}")

    if target_plugin:
        matched = [
            r for r in rows if target_plugin.lower() in r[0].lower()
        ]
        if matched:
            print(f"\nTarget plugin ({target_plugin}) stats in window:")
            for plugin_name, run_count, error_count, _dmin, dmax, avg in matched:
                print(f"  {plugin_name:<36} runs={int(run_count or 0)} "
                      f"errors={int(error_count or 0)} "
                      f"avg={_fmt_ms(avg)} max={_fmt_ms(dmax)}")
        else:
            print(f"\nTarget plugin ({target_plugin}) has 0 recorded runs in "
                  f"plugin_run_stats over the last {minutes} minute(s).")


def check_db_errors(cursor, hours, top, target_plugin=None):
    """Recent error rows from plugin_event_log."""
    _banner(f"Recent Errors - last {hours} hour(s) (plugin_event_log)")
    cursor.execute(
        "SELECT event_id, plugin_name, started_at, duration_us, "
        "log_reason, plugin_output "
        "FROM plugin_event_log "
        "WHERE had_error = 1 "
        "AND started_at >= UTC_TIMESTAMP() - INTERVAL %s HOUR "
        "ORDER BY started_at DESC "
        "LIMIT %s",
        (hours, top))
    rows = cursor.fetchall()
    if not rows:
        print("No errored plugin runs in this window.")
    else:
        for (event_id, plugin_name, started_at, duration_us,
             log_reason, plugin_output) in rows:
            print(f"  {started_at}  event={event_id:<10} "
                  f"plugin={plugin_name:<32} duration={_fmt_ms(duration_us):<10} "
                  f"reason={log_reason}")
            if plugin_output:
                tail_lines = [
                    ln.strip() for ln in plugin_output.strip().splitlines()
                    if ln.strip()
                ][-3:]
                for ln in tail_lines:
                    print(f"    | {ln[:120]}")

    if target_plugin:
        cursor.execute(
            "SELECT event_id, plugin_name, started_at, duration_us, "
            "had_error, log_reason, plugin_output "
            "FROM plugin_event_log "
            "WHERE plugin_name LIKE %s "
            "AND started_at >= UTC_TIMESTAMP() - INTERVAL %s HOUR "
            "ORDER BY started_at DESC "
            "LIMIT %s",
            (f"%{target_plugin}%", hours, top),
        )
        t_rows = cursor.fetchall()
        print(f"\nRecent plugin_event_log entries matching {target_plugin!r} "
              f"(last {hours}h): {len(t_rows)}")
        for (event_id, plugin_name, started_at, duration_us,
             had_error, log_reason, plugin_output) in t_rows:
            print(f"  {started_at}  event={event_id} plugin={plugin_name} "
                  f"had_error={had_error} duration={_fmt_ms(duration_us)} "
                  f"reason={log_reason}")
            if plugin_output:
                for ln in plugin_output.strip().splitlines()[-8:]:
                    print(f"    | {ln}")


def check_database_sections(config, minutes, hours, top, target_plugin=None):
    if not config.has_section("database_log") or not (
            config.has_option("database_log", "enabled")
            and config.getboolean("database_log", "enabled")):
        _banner("Database-backed Diagnostics")
        print("[database_log] is not enabled in this conf file - "
              "skipping DB backlog/timing/error checks.")
        return

    try:
        conn, driver_name = _open_db_connection(config)
        print(f"\n(Connected to MariaDB/MySQL via {driver_name})")
    except ImportError as exc:
        _banner("Database-backed Diagnostics")
        print(f"Database driver not importable in {sys.executable}: {exc}")
        print("Tip: check if another virtualenv on this server (e.g. "
              "/srv/bxlpy313/bin/python3) has the 'mariadb' package installed.")
        return
    except Exception as exc:
        _banner("Database-backed Diagnostics")
        print(f"Could not connect to {config.get('database_log', 'host')}: {exc}")
        return

    try:
        cursor = conn.cursor()
        try:
            check_db_backlog(cursor, top, target_plugin=target_plugin)
        except Exception:
            print(f"Backlog check failed:\n{traceback.format_exc()}")
        try:
            check_db_stats(cursor, minutes, top, target_plugin=target_plugin)
        except Exception:
            print(f"Stats check failed:\n{traceback.format_exc()}")
        try:
            check_db_errors(cursor, hours, top, target_plugin=target_plugin)
        except Exception:
            print(f"Error check failed:\n{traceback.format_exc()}")
        cursor.close()
    finally:
        conn.close()


def _tail_file(path, max_lines=20, max_bytes=65536):
    """Read the last max_lines lines from a file without reading the whole file."""
    if not path or not os.path.isfile(path):
        return []
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as fh:
            if size > max_bytes:
                fh.seek(size - max_bytes)
            data = fh.read().decode("utf-8", errors="replace")
        lines = data.splitlines()
        return lines[-max_lines:]
    except Exception as exc:
        return [f"<could not read {path}: {exc}>"]


def _scan_file_for_patterns(path, patterns, max_matches=15, max_bytes=262144):
    """Scan the tail of a log file (up to max_bytes) for lines matching patterns."""
    if not path or not os.path.isfile(path):
        return []
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as fh:
            if size > max_bytes:
                fh.seek(size - max_bytes)
            data = fh.read().decode("utf-8", errors="replace")
        matches = []
        for line in data.splitlines():
            if any(pat in line for pat in patterns):
                matches.append(line)
        return matches[-max_matches:]
    except Exception as exc:
        return [f"<error scanning {path}: {exc}>"]


def check_log_files(config, top=10):
    """Inspect engine log, supervisor log, and recent BaseTrigger error logs."""
    _banner("Daemon & Trigger Log File Diagnostics")

    log_dir = config.get("daemon", "logPath", fallback="/var/log/shotgunEventDaemon")
    log_file_name = config.get("daemon", "logFile", fallback="shotgunEventDaemon")
    engine_log_path = os.path.join(log_dir, log_file_name)

    # 1. Main engine log
    if os.path.isfile(engine_log_path):
        st = os.stat(engine_log_path)
        mtime_dt = datetime.datetime.fromtimestamp(
            st.st_mtime, tz=datetime.timezone.utc
        )
        print(f"Engine log: {engine_log_path} "
              f"(size={st.st_size // 1024} KB, last modified UTC={mtime_dt.isoformat()})")
        error_patterns = (
            "CRITICAL",
            "ERROR",
            "Error running register callback",
            "Could not load the plugin",
            "Unhandled exception",
            "deactivated",
            "Shutdown watchdog",
            "Unable to connect to SG",
        )
        err_lines = _scan_file_for_patterns(
            engine_log_path, error_patterns, max_matches=top
        )
        if err_lines:
            print(f"  Recent error/critical lines in {log_file_name} (last 256 KB):")
            for ln in err_lines:
                print(f"    {ln}")
        else:
            print("  No CRITICAL/ERROR lines in the last 256 KB of engine log.")

        load_lines = _scan_file_for_patterns(
            engine_log_path,
            ("Loading plugin", "Reloading plugin", "Last event id"),
            max_matches=5,
        )
        if load_lines:
            print("  Recent plugin load/reload activity:")
            for ln in load_lines:
                print(f"    {ln}")
    else:
        print(f"Engine log not found at {engine_log_path}")

    # 2. Supervisor log
    if os.path.isfile(SUPERVISOR_LOG_FILE):
        st = os.stat(SUPERVISOR_LOG_FILE)
        mtime_dt = datetime.datetime.fromtimestamp(
            st.st_mtime, tz=datetime.timezone.utc
        )
        print(f"\nSupervisor log: {SUPERVISOR_LOG_FILE} "
              f"(last modified UTC={mtime_dt.isoformat()})")
        for ln in _tail_file(SUPERVISOR_LOG_FILE, max_lines=8):
            print(f"    {ln}")

    # 3. BaseTrigger background_failures and error_*.txt / .log files
    if os.path.isdir(TRIGGERS_LOG_ROOT):
        bg_dir = os.path.join(TRIGGERS_LOG_ROOT, "background_failures")
        if os.path.isdir(bg_dir):
            bg_files = sorted(glob.glob(os.path.join(bg_dir, "*.log")))
            if bg_files:
                latest_bg = bg_files[-1]
                print(f"\nLatest background_failures log: {latest_bg}")
                for ln in _tail_file(latest_bg, max_lines=10):
                    print(f"    {ln}")

        # Look for recent error_*.txt or trigger logs modified in last 24h
        cutoff = time.time() - 86400
        recent_error_txts = []
        recent_trigger_logs = []
        for root, _dirs, files in os.walk(TRIGGERS_LOG_ROOT):
            for fn in files:
                fpath = os.path.join(root, fn)
                try:
                    mtime = os.path.getmtime(fpath)
                except OSError:
                    continue
                if mtime < cutoff:
                    continue
                if fn.startswith("error_") and fn.endswith(".txt"):
                    recent_error_txts.append((mtime, fpath))
                elif fn.endswith(".log") and "background_failures" not in root:
                    recent_trigger_logs.append((mtime, fpath))

        if recent_error_txts:
            recent_error_txts.sort(reverse=True)
            print(f"\nUnsent/leftover error_*.txt reports (last 24h): "
                  f"{len(recent_error_txts)}")
            for mtime, fpath in recent_error_txts[:5]:
                ts = datetime.datetime.fromtimestamp(
                    mtime, tz=datetime.timezone.utc
                ).isoformat()
                print(f"  {ts}  {fpath}")
        if recent_trigger_logs:
            recent_trigger_logs.sort(reverse=True)
            print(f"\nRecently active BaseTrigger logs (last 24h, top {min(top, len(recent_trigger_logs))}):")
            for mtime, fpath in recent_trigger_logs[:top]:
                ts = datetime.datetime.fromtimestamp(
                    mtime, tz=datetime.timezone.utc
                ).isoformat()
                errs = _scan_file_for_patterns(
                    fpath, ("ERROR", "CRITICAL", "Traceback"), max_matches=3
                )
                err_tag = f" [has {len(errs)} error line(s)]" if errs else ""
                print(f"  {ts}  {fpath}{err_tag}")
                for eln in errs:
                    print(f"      | {eln[:120]}")


def check_target_plugin(config, sg, plugin_query, test_plugin=False):
    """Deep inspection of a specific plugin (logs, registration, trigger logs)."""
    _banner(f"Targeted Plugin Inspection: {plugin_query}")

    log_dir = config.get("daemon", "logPath", fallback="/var/log/shotgunEventDaemon")
    plugin_Paths = [
        s.strip()
        for s in config.get("plugins", "paths", fallback="").split(",")
        if s.strip()
    ]

    # Locate matching plugin .py file(s)
    matched_files = []
    for pdir in plugin_Paths:
        if not os.path.isdir(pdir):
            continue
        for fn in sorted(os.listdir(pdir)):
            if fn.endswith(".py") and plugin_query.lower() in fn.lower():
                matched_files.append(os.path.join(pdir, fn))

    if not matched_files:
        print(f"No plugin .py file matching {plugin_query!r} found in {plugin_Paths}")
    else:
        for pfile in matched_files:
            pname = os.path.splitext(os.path.basename(pfile))[0]
            st = os.stat(pfile)
            mtime_dt = datetime.datetime.fromtimestamp(
                st.st_mtime, tz=datetime.timezone.utc
            )
            print(f"Plugin file: {pfile} (mtime UTC={mtime_dt.isoformat()})")

            # Check per-plugin daemon log: /var/log/shotgunEventDaemon/plugin.<pname>
            plog = os.path.join(log_dir, f"plugin.{pname}")
            if os.path.isfile(plog):
                pst = os.stat(plog)
                pmtime = datetime.datetime.fromtimestamp(
                    pst.st_mtime, tz=datetime.timezone.utc
                )
                print(f"\nDaemon plugin log: {plog} "
                      f"(size={pst.st_size} bytes, mtime UTC={pmtime.isoformat()})")
                for ln in _tail_file(plog, max_lines=15):
                    print(f"  {ln}")
            else:
                print(f"\nDaemon plugin log not found: {plog}")

            # Check engine log mentions of this plugin
            engine_log = os.path.join(
                log_dir, config.get("daemon", "logFile", fallback="shotgunEventDaemon")
            )
            mentions = _scan_file_for_patterns(
                engine_log, (pname,), max_matches=15
            )
            if mentions:
                print(f"\nRecent mentions of {pname} in main engine log:")
                for ln in mentions:
                    print(f"  {ln}")

            # Optional read-only dry-run of plugin's trigger class
            if test_plugin:
                _dry_run_plugin_registration(pfile, sg)


def _dry_run_plugin_registration(plugin_file, sg):
    """Inspect the plugin's registerCallbacks / trigger instance without side effects."""
    print(f"\n--- Dry-run registration check for {plugin_file} ---")
    try:
        with open(plugin_file, "r", encoding="utf-8") as fh:
            source = fh.read()
        # Find trigger instantiation in plugin file
        print("Plugin source loaded successfully.")
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "_diag_plugin_mod", plugin_file
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        # Look for BaseTrigger subclasses in imported modules or module attrs
        from bxl_triggers.bxl_triggers.base_trigger import BaseTrigger
        trigger_classes = []
        for attr_val in vars(mod).values():
            if isinstance(attr_val, type) and issubclass(attr_val, BaseTrigger) and attr_val is not BaseTrigger:
                trigger_classes.append(attr_val)
            elif hasattr(attr_val, "__dict__"):
                for sub_val in vars(attr_val).values():
                    if (
                        isinstance(sub_val, type)
                        and issubclass(sub_val, BaseTrigger)
                        and sub_val is not BaseTrigger
                    ):
                        trigger_classes.append(sub_val)

        for trig_cls in trigger_classes:
            print(f"Found Trigger class: {trig_cls.__module__}.{trig_cls.__name__}")
            trig = trig_cls()
            print(f"  script_name: {trig.script_name!r}")
            print(f"  script_key found: {bool(trig.script_key)}")
            print(f"  site_execution: {trig.site_execution!r}")
            print(f"  deadline_execution: {trig.deadline_execution!r}")
            print(f"  scope: {trig.get_scope()!r}")
            reg_ok = trig.should_register_on_this_host(sg=sg)
            print(f"  should_register_on_this_host(): {reg_ok!r} "
                  f"(resolved current_site={trig.current_site!r})")

            # Check BaseTrigger log files for this class
            cls_log_dir = os.path.join(TRIGGERS_LOG_ROOT, trig_cls.__name__)
            if os.path.isdir(cls_log_dir):
                log_files = sorted(glob.glob(os.path.join(cls_log_dir, "*")))
                print(f"  Trigger log directory ({cls_log_dir}): "
                      f"{len(log_files)} file(s)")
                for lf in log_files[-3:]:
                    lst = os.stat(lf)
                    lmtime = datetime.datetime.fromtimestamp(
                        lst.st_mtime, tz=datetime.timezone.utc
                    )
                    print(f"    {lf} (size={lst.st_size}B, mtime UTC={lmtime.isoformat()})")
                    for ln in _tail_file(lf, max_lines=10):
                        print(f"      | {ln}")
            else:
                print(f"  Trigger log directory does not exist yet: {cls_log_dir}")
    except Exception:
        print(f"Dry-run check encountered an error:\n{traceback.format_exc()}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--minutes", type=int, default=60,
        help="Lookback window for plugin_run_stats (default 60)."
    )
    parser.add_argument(
        "--hours", type=int, default=24,
        help="Lookback window for plugin_event_log errors (default 24)."
    )
    parser.add_argument(
        "--top", type=int, default=10,
        help="Max rows shown per table (default 10)."
    )
    parser.add_argument(
        "--plugin", type=str, default=None,
        help="Filter/inspect a specific plugin name (e.g. BXL_version_download_vendor)."
    )
    parser.add_argument(
        "--test-plugin", action="store_true",
        help="When used with --plugin, instantiate its BaseTrigger class "
             "and test should_register_on_this_host()."
    )
    args = parser.parse_args()

    config = read_config(CONF_PATH)
    print(f"Diagnosing daemon using {CONF_PATH}")
    print(f"ShotGrid server: {config.get('shotgun', 'server', fallback='?')}")
    print(f"Run at (UTC): "
          f"{datetime.datetime.now(datetime.timezone.utc).isoformat()}")

    check_os_snapshot(config)
    sg, latest_sg_event_id = check_sg_and_site_config(config)
    check_event_id_file(
        config,
        latest_sg_event_id=latest_sg_event_id,
        target_plugin=args.plugin,
        top=args.top,
    )
    check_database_sections(
        config,
        args.minutes,
        args.hours,
        args.top,
        target_plugin=args.plugin,
    )
    check_log_files(config, top=args.top)

    if args.plugin:
        check_target_plugin(
            config, sg, args.plugin, test_plugin=args.test_plugin
        )

    print()
    print("Done.")


if __name__ == "__main__":
    main()
