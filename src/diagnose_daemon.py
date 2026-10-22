#
# Copyright (c) 2026 Boxel Studio.
#
# :Module: diagnose_daemon.py
# :Description: Standalone, read-only diagnostic for the ShotGrid event
#               daemon. Reads shotgunEventDaemon.conf next to this file,
#               queries the daemon's own MariaDB logging tables (if
#               [database_log] is enabled) for the live per-plugin
#               backlog, recent per-plugin timing stats and recent
#               errors, takes a quick OS-level snapshot of the daemon
#               process, and times a trivial ShotGrid API round trip.
#               Run it directly on whichever server you want to
#               inspect - it has no site-specific values, everything
#               comes from the conf file beside it.
#
#               Usage (same interpreter the daemon itself runs under):
#                 /srv/bxlpy313/bin/python3 \
#                     /srv/shotgunEvents-master/src/diagnose_daemon.py
#               Optional flags: --minutes (stats window, default 60),
#               --hours (error window, default 24), --top (rows shown
#               per table, default 10).
#
# :Author: Fidel Moreno Miranda <fidel.moreno@boxelstudio.com>
#

# Standard modules
import argparse
import configparser
import datetime
import os
import subprocess
import sys
import time
import traceback

# Daemon's own vendored dependencies (shotgun_api3, MySQLdb if present).
# Matches the sys.path setup shotgunEventDaemon.py itself uses - this
# script is meant to live in the same directory on the server.
_PACKAGES_DIR = "/srv/shotgunEvents-master/src/site-packages"
if _PACKAGES_DIR not in sys.path and os.path.isdir(_PACKAGES_DIR):
    sys.path.append(_PACKAGES_DIR)

CONF_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "shotgunEventDaemon.conf")

DAEMON_PROCESS_MARKERS = ("shotgunEventDaemon.py", "supervisor.py")


def _banner(title):
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def _fmt_ms(duration_us):
    if duration_us is None:
        return "n/a"
    # MySQLdb returns DECIMAL columns (and AVG()/division results) as
    # decimal.Decimal, which can't be divided by a plain float.
    return f"{float(duration_us) / 1000.0:.1f} ms"


def _fmt_seconds(seconds):
    if seconds is None:
        return "n/a"
    seconds = int(seconds)
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
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


def check_os_snapshot():
    """Load average, memory, and the daemon's own process(es) via ps.

    Everything here is a plain read of /proc or a call to `ps` - no
    elevated privileges needed, and nothing is written anywhere.
    """
    _banner("OS snapshot")

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

    try:
        result = subprocess.run(
            ["ps", "-eo", "pid,etimes,pcpu,pmem,cmd"],
            capture_output=True, text=True, timeout=10)
        matched_lines = []
        for line in result.stdout.splitlines():
            for marker in DAEMON_PROCESS_MARKERS:
                if marker in line:
                    matched_lines.append(line)
                    break
        if matched_lines:
            print("\nDaemon-related processes (pid, uptime, %cpu, %mem, cmd):")
            for line in matched_lines:
                parts = line.split(None, 4)
                if len(parts) < 5:
                    print(f"  {line.strip()}")
                    continue
                pid, etimes, pcpu, pmem, cmd = parts
                try:
                    uptime = _fmt_seconds(int(etimes))
                except ValueError:
                    uptime = etimes
                print(f"  pid={pid:<8} uptime={uptime:<10} "
                      f"cpu={pcpu:>5}% mem={pmem:>5}%  {cmd}")
        else:
            print("\nNo process matching "
                  f"{DAEMON_PROCESS_MARKERS} found via `ps`. "
                  "Check manually with `ps aux | grep shotgunEvent`.")
    except Exception as exc:
        print(f"Could not run `ps`: {exc}")


def _open_db_connection(config):
    import MySQLdb
    host = config.get("database_log", "host")
    port = (config.getint("database_log", "port")
            if config.has_option("database_log", "port") else 3306)
    database = config.get("database_log", "database")
    user = config.get("database_log", "user")
    password = (config.get("database_log", "password")
                if config.has_option("database_log", "password") else "")
    return MySQLdb.connect(
        host=host, port=port, user=user, passwd=password, db=database,
        charset="utf8mb4", use_unicode=True)


def check_db_backlog(cursor, top):
    """Live per-plugin backlog, straight from plugin_event_queue.

    This table is a snapshot, not history - it shows what's queued and
    what's currently running right now, as of the last write from the
    daemon's own writer thread (every 2s while a plugin has rows).
    """
    _banner("Live backlog (plugin_event_queue)")
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


def check_db_stats(cursor, minutes, top):
    """Per-plugin timing over the last N minutes, from plugin_run_stats."""
    _banner(f"Plugin timing - last {minutes} minute(s) (plugin_run_stats)")
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

    print(f"{'plugin':<35} {'runs':>7} {'errors':>7} "
          f"{'avg':>10} {'max':>10}")
    for plugin_name, run_count, error_count, _dmin, dmax, avg in rows[:top]:
        print(f"{plugin_name:<35} {run_count:>7} {error_count:>7} "
              f"{_fmt_ms(avg):>10} {_fmt_ms(dmax):>10}")


def check_db_errors(cursor, hours, top):
    """Recent error rows from plugin_event_log."""
    _banner(f"Recent errors - last {hours} hour(s) (plugin_event_log)")
    cursor.execute(
        "SELECT event_id, plugin_name, started_at, duration_us, log_reason "
        "FROM plugin_event_log "
        "WHERE had_error = 1 "
        "AND started_at >= UTC_TIMESTAMP() - INTERVAL %s HOUR "
        "ORDER BY started_at DESC "
        "LIMIT %s",
        (hours, top))
    rows = cursor.fetchall()
    if not rows:
        print("No errored plugin runs in this window.")
        return
    for event_id, plugin_name, started_at, duration_us, log_reason in rows:
        print(f"  {started_at}  event={event_id:<10} "
              f"plugin={plugin_name:<30} duration={_fmt_ms(duration_us):<10} "
              f"reason={log_reason}")


def check_database_sections(config, minutes, hours, top):
    if not config.has_section("database_log") or not (
            config.has_option("database_log", "enabled")
            and config.getboolean("database_log", "enabled")):
        _banner("Database-backed diagnostics")
        print("[database_log] is not enabled in this conf file - "
              "skipping backlog/timing/error checks. Only the OS "
              "snapshot and SG latency check below are available here.")
        return

    try:
        conn = _open_db_connection(config)
    except ImportError as exc:
        _banner("Database-backed diagnostics")
        print(f"MySQLdb is not importable in this interpreter: {exc}")
        return
    except Exception as exc:
        _banner("Database-backed diagnostics")
        print(f"Could not connect to {config.get('database_log', 'host')}: {exc}")
        return

    try:
        cursor = conn.cursor()
        try:
            check_db_backlog(cursor, top)
        except Exception:
            print(f"Backlog check failed:\n{traceback.format_exc()}")
        try:
            check_db_stats(cursor, minutes, top)
        except Exception:
            print(f"Stats check failed:\n{traceback.format_exc()}")
        try:
            check_db_errors(cursor, hours, top)
        except Exception:
            print(f"Error check failed:\n{traceback.format_exc()}")
        cursor.close()
    finally:
        conn.close()


def check_sg_latency(config):
    """Time a single trivial ShotGrid API round trip (sg.info()).

    Separates "the plugin code is slow" from "the connection to
    ShotGrid itself is slow right now" - sg.info() does no real query,
    just round-trips to the server.
    """
    _banner("ShotGrid API round-trip")
    try:
        import shotgun_api3
        server = config.get("shotgun", "server")
        name = config.get("shotgun", "name")
        key = config.get("shotgun", "key")
        sg = shotgun_api3.Shotgun(server, name, key)
        start = time.monotonic()
        sg.info()
        elapsed_ms = (time.monotonic() - start) * 1000
        print(f"sg.info() round trip to {server}: {elapsed_ms:.1f} ms")
    except Exception:
        print(f"Could not complete the SG round-trip check:\n"
              f"{traceback.format_exc()}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=int, default=60,
                         help="Lookback window for plugin_run_stats (default 60).")
    parser.add_argument("--hours", type=int, default=24,
                         help="Lookback window for plugin_event_log errors (default 24).")
    parser.add_argument("--top", type=int, default=10,
                         help="Max rows shown per table (default 10).")
    args = parser.parse_args()

    config = read_config(CONF_PATH)
    print(f"Diagnosing daemon using {CONF_PATH}")
    print(f"ShotGrid server: {config.get('shotgun', 'server', fallback='?')}")
    print(f"Run at (UTC): "
          f"{datetime.datetime.now(datetime.timezone.utc).isoformat()}")

    check_os_snapshot()
    check_database_sections(config, args.minutes, args.hours, args.top)
    check_sg_latency(config)

    print()
    print("Done.")


if __name__ == "__main__":
    main()
