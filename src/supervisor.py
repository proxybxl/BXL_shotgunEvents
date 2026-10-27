#!/usr/bin/env python
#
# :Module: supervisor.py
# :Description: Supervisor and Watchdog for shotgunEventDaemon.py.
#               Launches the daemon, restarts it on SIGHUP (sent by
#               bxlApp_triggers_git_autopull.py after a successful pull),
#               and runs a coordinated background watchdog thread that:
#                 1. Detects unexpected daemon stops/crashes, cleans up
#                    stale pidfiles, relaunches the daemon (with crash-loop
#                    backoff), and reports both the crash context and
#                    recovery status to Slack (#log-pipe-triggers).
#                 2. Detects and terminates duplicate shotgunEventDaemon.py
#                    processes not registered in the pidfile.
#
#               Usage mirrors the daemon's own CLI exactly:
#                   sudo python3 supervisor.py start|stop|restart|foreground
#               The mode given here is the mode the child daemon is
#               launched with too, and is preserved across every reload.
#

import configparser
import os
import pickle
import signal
import socket
import subprocess
import sys
import threading
import time

import daemonizer

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Vendored dependencies & bxl_triggers for Slack reporting
_PACKAGES_DIR = "/srv/shotgunEvents-master/src/site-packages"
if _PACKAGES_DIR not in sys.path and os.path.isdir(_PACKAGES_DIR):
    sys.path.append(_PACKAGES_DIR)

_TRIGGERS_DIR = "/srv/shotgunEvents-master/src/bxl_triggers"
if _TRIGGERS_DIR not in sys.path and os.path.isdir(_TRIGGERS_DIR):
    sys.path.append(_TRIGGERS_DIR)

DAEMON_SCRIPT = os.path.join(SCRIPT_DIR, "shotgunEventDaemon.py")
DAEMON_CONF_FILE = os.path.join(SCRIPT_DIR, "shotgunEventDaemon.conf")
DAEMON_PID_FILE = "/var/log/shotgunEventDaemon/shotgunEventDaemon.pid"
DAEMON_LOG_FILE = "/var/log/shotgunEventDaemon/shotgunEventDaemon"
DEFAULT_EVENT_ID_FILE = "/var/log/shotgunEventDaemon/shotgunEventDaemon.id"

SUPERVISOR_PID_FILE = "/var/log/shotgunEventDaemon/shotgunEventSupervisor.pid"
SUPERVISOR_LOG_FILE = "/var/log/shotgunEventDaemon/shotgunEventSupervisor.log"

LOG_PIPE_TRIGGERS_CHANNEL_ID = "C08UT5TSH33"

_SITE_DOMAIN_MAP = {
    "Boxel": ["boxel.lan", "No domain found (Localhost/Workgroup)"],
    "Centergrid": ["centergrid.boxel.lan"],
}

# How often to check whether the daemon has actually stopped, and how
# often to log a "still waiting" status line while doing so. Deliberately
# no upper bound after which the supervisor gives up and launches a
# replacement anyway: launching a new daemon while the old one might
# still be alive is exactly how two processes end up racing over the
# same eventIdFile, producing duplicate event processing. If a stop
# genuinely never finishes, that needs a human to look at it - the
# supervisor keeps logging that it's waiting, it does not paper over the
# situation by starting a replacement.
DAEMON_STOP_POLL_INTERVAL_SECONDS = 1
DAEMON_STOP_LOG_INTERVAL_SECONDS = 10

# How long to wait for a new pidfile to appear after 'start' before giving
# up. Unlike the stop side above, giving up here is safe: no replacement
# process is launched as a result, so there's nothing left racing against
# anything - a human just needs to look at why 'start' didn't come up.
DAEMON_START_CONFIRM_TIMEOUT_SECONDS = 15

# Same reasoning as DAEMON_STOP_* above, for a foreground-mode child
# (tracked via its Popen handle instead of a pidfile) - no give-up point
# that would let a replacement be launched before this one is confirmed
# gone.
FOREGROUND_STOP_POLL_INTERVAL_SECONDS = 1
FOREGROUND_STOP_LOG_INTERVAL_SECONDS = 10

# Watchdog check interval & crash-loop backoff parameters
WATCHDOG_CHECK_INTERVAL_SECONDS = 5
CRASH_WINDOW_SECONDS = 300
CRASH_BACKOFF_SCHEDULE_SECONDS = (0, 10, 30, 60)
DUPLICATE_STOP_TIMEOUT_SECONDS = 30

# Watchdog stall & lag detection thresholds:
# - shotgunEventDaemon.py saves eventIdFile on every _mainLoop pass (~5s).
#   If eventIdFile hasn't been touched for STUCK_EVENT_ID_FILE_SECONDS while
#   the daemon process is alive, the main loop is hung/frozen.
STUCK_EVENT_ID_FILE_SECONDS = 600
HUNG_DAEMON_STOP_TIMEOUT_SECONDS = 60
PLUGIN_LAG_CHECK_INTERVAL_SECONDS = 60
PLUGIN_LAG_WARN_THRESHOLD = 500
PLUGIN_LAG_CLEAR_THRESHOLD = 100


def _log(message: str) -> None:
    """Print a timestamped, immediately-flushed status line.

    Args:
        message (str): The message to log.
    """
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def _resolve_site_and_host():
    """Return (site_name, hostname, domain) for Slack notifications."""
    hostname = socket.gethostname()
    fqdn = socket.getfqdn()
    domain = (
        fqdn.split(".", 1)[1]
        if "." in fqdn
        else "No domain found (Localhost/Workgroup)"
    )
    site_name = os.environ.get("SITE")
    if not site_name:
        site_name = "Boxel"
        for cand_site, domains in _SITE_DOMAIN_MAP.items():
            if domain in domains:
                site_name = cand_site
                break
    return site_name, hostname, domain


def _notify_slack_async(message: str) -> None:
    """Send a Slack notification to #log-pipe-triggers on a daemon thread.

    Never blocks the supervisor's lifecycle or watchdog loops, and never
    raises if Slack is unreachable.
    """
    def _worker():
        try:
            from bxl_triggers.common import slack_msj as bxl_slack
            bxl_slack.send_slack_message_to_channel(
                message, LOG_PIPE_TRIGGERS_CHANNEL_ID
            )
            return
        except Exception:
            pass
        try:
            import slack_msj as local_slack
            local_slack.send_slack_message(message)
        except Exception as exc:
            _log(f"Slack notification failed: {exc}")

    thread = threading.Thread(
        target=_worker, name="SupervisorSlackNotifier", daemon=True
    )
    thread.start()


def _tail_log_lines(path: str, max_lines: int = 8) -> str:
    """Return the last `max_lines` non-empty lines from a log file."""
    if not path or not os.path.isfile(path):
        return "(log file not found)"
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as fh:
            if size > 16384:
                fh.seek(size - 16384)
            text = fh.read().decode("utf-8", errors="replace")
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        if not lines:
            return "(log file empty)"
        return "\n".join(lines[-max_lines:])
    except Exception as exc:
        return f"(could not read log: {exc})"


def _pid_is_alive(pid: int) -> bool:
    """Return whether a process with the given PID is currently running.

    Args:
        pid (int): The process id to check.
    """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _read_pid_file(path: str):
    """Read an integer PID from a pidfile, if present and valid.

    Args:
        path (str): Path to the pidfile.

    Returns:
        int or None: The PID, or None if the file is missing/unreadable.
    """
    try:
        with open(path, "r") as pid_file:
            return int(pid_file.read().strip())
    except (IOError, ValueError):
        return None


def _write_pid_file(path: str, pid: int) -> None:
    """Write an integer PID to `path`."""
    try:
        with open(path, "w") as pid_file:
            pid_file.write(f"{pid}\n")
    except OSError as exc:
        _log(f"Could not write pidfile {path}: {exc}")


def _resolve_event_id_file() -> str:
    """Resolve the `eventIdFile` path from `shotgunEventDaemon.conf`,
    falling back to `DEFAULT_EVENT_ID_FILE`.
    """
    if os.path.isfile(DAEMON_CONF_FILE):
        try:
            cfg = configparser.ConfigParser()
            cfg.read(DAEMON_CONF_FILE)
            path = cfg.get("daemon", "eventIdFile", fallback="").strip()
            if path:
                return path
        except Exception:
            pass
    return DEFAULT_EVENT_ID_FILE


def _list_running_daemon_pids() -> list:
    """Return a list of live `shotgunEventDaemon.py` PIDs (excluding `stop`
    or `sudo` wrapper processes).
    """
    if os.name == "nt":
        return []
    try:
        result = subprocess.run(
            ["ps", "-eo", "pid,cmd"],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except Exception:
        return []

    pids = []
    for line in result.stdout.splitlines()[1:]:
        parts = line.split(None, 1)
        if len(parts) < 2:
            continue
        cmd = parts[1].strip()
        if "shotgunEventDaemon.py" not in cmd:
            continue
        if (
            "diagnose_daemon.py" in cmd
            or cmd.endswith(" stop")
            or cmd.startswith("sudo ")
        ):
            continue
        try:
            pid = int(parts[0])
            if _pid_is_alive(pid):
                pids.append(pid)
        except ValueError:
            continue
    return pids


class SupervisorDaemon(daemonizer.Daemon):
    """Owns the daemon's lifecycle, reloads it on SIGHUP, and runs a
    coordinated watchdog thread that monitors the child daemon process,
    cleans up duplicate instances, auto-restarts on unexpected exits,
    and reports lifecycle/watchdog events to Slack (#log-pipe-triggers).
    """

    def __init__(self, child_mode: str):
        """Initialize the supervisor.

        Args:
            child_mode (str): Either "start" or "foreground" - the mode
                the child daemon is launched with, and re-launched with on
                every reload.
        """
        self.child_mode = child_mode
        self._daemon_proc = None  # Popen handle, only used in foreground mode
        self._tracked_daemon_pid = None
        self._restart_requested = False
        self._stopping = False
        self._child_stop_done = False
        self._lifecycle_lock = threading.Lock()
        self._crash_timestamps = []
        self._last_launch_time = time.time()
        self._last_lag_check_time = 0.0
        self._lagging_plugins_alerted = set()
        self._event_id_file = _resolve_event_id_file()
        super(SupervisorDaemon, self).__init__(
            "shotgunEventSupervisor",
            SUPERVISOR_PID_FILE,
            stdout=SUPERVISOR_LOG_FILE,
            stderr=SUPERVISOR_LOG_FILE,
        )

    def _handle_reload(self, signum, frame) -> None:
        """SIGHUP handler: request a reload on the next loop tick."""
        _log("Received reload signal (SIGHUP); restart requested.")
        self._restart_requested = True

    def _launch_child(self) -> bool:
        """Launch the daemon in self.child_mode and confirm it came up.

        Returns:
            bool: True if the daemon was confirmed running after launch.
        """
        _log(f"Launching daemon in '{self.child_mode}' mode.")
        if self.child_mode == "start":
            # Ensure a stale pidfile from a previous crash/SIGKILL does not
            # block daemonizer.Daemon.start() from starting.
            existing_pid = _read_pid_file(DAEMON_PID_FILE)
            if existing_pid is not None and not _pid_is_alive(existing_pid):
                _log(
                    f"Removing stale daemon pidfile {DAEMON_PID_FILE} "
                    f"(dead pid {existing_pid}) before launch."
                )
                try:
                    os.remove(DAEMON_PID_FILE)
                except OSError:
                    pass

        cmd = ["sudo", sys.executable, DAEMON_SCRIPT, self.child_mode]

        if self.child_mode == "start":
            subprocess.Popen(
                cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            ).wait(timeout=DAEMON_START_CONFIRM_TIMEOUT_SECONDS)
            self._daemon_proc = None
            return self._confirm_daemon_started()
        else:
            # foreground mode never detaches - it stays alive as our own
            # child for as long as the daemon runs.
            self._daemon_proc = subprocess.Popen(cmd)
            time.sleep(2)
            if self._daemon_proc.poll() is not None:
                _log(
                    "Daemon exited immediately after launch in "
                    f"foreground mode (code {self._daemon_proc.returncode})."
                )
                self._tracked_daemon_pid = None
                return False
            self._tracked_daemon_pid = self._daemon_proc.pid
            self._last_launch_time = time.time()
            self._lagging_plugins_alerted.clear()
            return True

    def _confirm_daemon_started(self) -> bool:
        """Poll for a live daemon pidfile after a `start` launch.

        Only meaningful in `start` mode - `foreground` mode is confirmed
        via the Popen handle directly in `_launch_child`.
        """
        deadline = time.time() + DAEMON_START_CONFIRM_TIMEOUT_SECONDS
        while time.time() < deadline:
            pid = _read_pid_file(DAEMON_PID_FILE)
            if pid is not None and _pid_is_alive(pid):
                self._tracked_daemon_pid = pid
                self._last_launch_time = time.time()
                self._lagging_plugins_alerted.clear()
                _log(f"Daemon confirmed running (pid {pid}).")
                return True
            time.sleep(0.5)
        _log(
            "Daemon did not confirm as running within "
            f"{DAEMON_START_CONFIRM_TIMEOUT_SECONDS}s of launch."
        )
        return False

    def _stop_child(self) -> bool:
        """Stop the daemon, using whichever mechanism its mode requires.

        Returns:
            bool: True once the daemon is confirmed fully stopped. False
                only when there was nothing to even attempt waiting on
                (e.g. the 'stop' command itself couldn't be launched) -
                never as a result of "waited long enough, giving up".
        """
        if self.child_mode == "start":
            return self._stop_child_via_cli()
        return self._stop_child_via_process_handle()

    def _stop_child_via_cli(self) -> bool:
        """Stop a `start`-mode daemon via its own pidfile-based `stop`."""
        pid = _read_pid_file(DAEMON_PID_FILE) or self._tracked_daemon_pid
        if pid is None or not _pid_is_alive(pid):
            if os.path.exists(DAEMON_PID_FILE):
                try:
                    os.remove(DAEMON_PID_FILE)
                except OSError:
                    pass
            self._tracked_daemon_pid = None
            _log("No live daemon found; nothing to stop.")
            return True

        _log(f"Stopping daemon (pid {pid}) via its own 'stop' command.")
        try:
            stop_helper = subprocess.Popen(
                ["sudo", sys.executable, DAEMON_SCRIPT, "stop"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except OSError as exc:
            _log(f"Could not launch daemon 'stop' command: {exc}")
            return False

        waited = 0
        while _pid_is_alive(pid):
            time.sleep(DAEMON_STOP_POLL_INTERVAL_SECONDS)
            waited += DAEMON_STOP_POLL_INTERVAL_SECONDS
            if waited % DAEMON_STOP_LOG_INTERVAL_SECONDS == 0:
                _log(
                    f"Still waiting for daemon (pid {pid}) to stop "
                    f"({waited}s elapsed)..."
                )

        try:
            stop_helper.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _log(
                f"Note: the 'stop' helper process (pid {stop_helper.pid}) "
                f"hasn't exited yet even though the daemon itself has - "
                f"harmless, it will be reaped eventually."
            )

        self._tracked_daemon_pid = None
        if os.path.exists(DAEMON_PID_FILE):
            _log(
                f"Warning: daemon pidfile still present even though pid "
                f"{pid} has exited. Removing stale pidfile."
            )
            try:
                os.remove(DAEMON_PID_FILE)
            except OSError:
                pass
        else:
            _log(f"Daemon stop confirmed (pid {pid} exited, pidfile gone).")
        return True

    def _stop_child_via_process_handle(self) -> bool:
        """Stop a `foreground`-mode daemon directly - it has no pidfile."""
        if self._daemon_proc is None or self._daemon_proc.poll() is not None:
            self._daemon_proc = None
            self._tracked_daemon_pid = None
            return True

        _log("Stopping foreground daemon (SIGTERM to tracked process).")
        self._daemon_proc.terminate()

        waited = 0
        while True:
            try:
                self._daemon_proc.wait(
                    timeout=FOREGROUND_STOP_POLL_INTERVAL_SECONDS
                )
                break
            except subprocess.TimeoutExpired:
                waited += FOREGROUND_STOP_POLL_INTERVAL_SECONDS
                if waited % FOREGROUND_STOP_LOG_INTERVAL_SECONDS == 0:
                    _log(
                        "Still waiting for foreground daemon (pid "
                        f"{self._daemon_proc.pid}) to stop ({waited}s "
                        "elapsed)..."
                    )

        _log(f"Foreground daemon (pid {self._daemon_proc.pid}) stop confirmed.")
        self._daemon_proc = None
        self._tracked_daemon_pid = None
        return True

    def _reload_child(self) -> None:
        """Stop and relaunch the daemon in its original mode."""
        site_name, hostname, _ = _resolve_site_and_host()
        with self._lifecycle_lock:
            old_pid = self._tracked_daemon_pid or _read_pid_file(DAEMON_PID_FILE)
            _log(f"Reloading daemon (current pid={old_pid}).")
            if not self._stop_child():
                msg = (
                    "Reload aborted: could not confirm the daemon fully "
                    "stopped. Not launching a replacement - two daemons "
                    "running at once is worse than one running old code a "
                    "bit longer. Investigate manually, then send SIGHUP "
                    "again once the old process is confirmed gone."
                )
                _log(msg)
                _notify_slack_async(
                    f":x: *[Supervisor] Reload aborted on {site_name}* "
                    f"(`{hostname}`)\n"
                    f"     • Could not confirm previous daemon (pid `{old_pid}`) "
                    f"stopped."
                )
                return

            started = self._launch_child()
            new_pid = self._tracked_daemon_pid
            if started:
                _log(f"Reload complete (old pid={old_pid} -> new pid={new_pid}).")
                _notify_slack_async(
                    f":white_check_mark: *[Supervisor] Daemon reloaded on "
                    f"{site_name}* (`{hostname}`)\n"
                    f"     • *Previous PID:* `{old_pid}` -> *New PID:* `{new_pid}`"
                )
            else:
                _log("Reload failed: daemon did not confirm running after start.")
                _notify_slack_async(
                    f":rotating_light: *[Supervisor] Reload failed on "
                    f"{site_name}* (`{hostname}`)\n"
                    f"     • Previous PID `{old_pid}` stopped, but new daemon "
                    f"did not confirm running within "
                    f"{DAEMON_START_CONFIRM_TIMEOUT_SECONDS}s."
                )

    def _watchdog_loop(self) -> None:
        """Background watchdog thread running alongside the supervisor.

        Every WATCHDOG_CHECK_INTERVAL_SECONDS:
          1. Acquires `self._lifecycle_lock` (non-blocking) so it never
             interferes with an in-progress start, stop, or SIGHUP reload.
          2. Verifies whether the child `shotgunEventDaemon.py` is alive.
             If it died unexpectedly, logs and sends a Slack alert with the
             exit context and log tail, cleans up any stale pidfile, and
             relaunches it with crash-loop backoff.
          3. Detects any duplicate `shotgunEventDaemon.py` processes not
             matching the official PID file, terminates them, and notifies
             Slack.
        """
        _log("Watchdog thread started.")
        while not self._stopping:
            time.sleep(WATCHDOG_CHECK_INTERVAL_SECONDS)
            if self._stopping or self._restart_requested:
                continue

            if not self._lifecycle_lock.acquire(blocking=False):
                # A planned start/stop/reload is currently in progress.
                continue

            try:
                if self._stopping or self._restart_requested:
                    continue
                self._watchdog_check_once()
            except Exception as exc:
                _log(f"Watchdog check error: {exc}")
            finally:
                self._lifecycle_lock.release()

    def _watchdog_check_once(self) -> None:
        """Perform a single watchdog check under `self._lifecycle_lock`."""
        site_name, hostname, _ = _resolve_site_and_host()

        if self.child_mode == "foreground":
            alive = (
                self._daemon_proc is not None
                and self._daemon_proc.poll() is None
            )
            if alive:
                self._check_daemon_progress(
                    self._tracked_daemon_pid, site_name, hostname
                )
                return
            dead_pid = self._tracked_daemon_pid
            cause = (
                f"foreground process exited "
                f"(code={getattr(self._daemon_proc, 'returncode', '?')})"
            )
        else:
            pidfile_pid = _read_pid_file(DAEMON_PID_FILE)
            running_pids = _list_running_daemon_pids()

            # Check for duplicate daemon processes first
            if len(running_pids) > 1:
                keep_pid = (
                    pidfile_pid
                    if pidfile_pid in running_pids
                    else (
                        self._tracked_daemon_pid
                        if self._tracked_daemon_pid in running_pids
                        else running_pids[0]
                    )
                )
                extra_pids = [p for p in running_pids if p != keep_pid]
                _log(
                    f"[Watchdog] Multiple daemon processes detected: "
                    f"{running_pids}. Keeping pid={keep_pid}, "
                    f"terminating extra={extra_pids}."
                )
                terminated = []
                for extra_pid in extra_pids:
                    try:
                        os.kill(extra_pid, signal.SIGTERM)
                        deadline = time.time() + DUPLICATE_STOP_TIMEOUT_SECONDS
                        while (
                            time.time() < deadline and _pid_is_alive(extra_pid)
                        ):
                            time.sleep(1)
                        if _pid_is_alive(extra_pid):
                            _log(
                                f"[Watchdog] Extra daemon pid={extra_pid} did "
                                f"not exit after SIGTERM; sending SIGKILL."
                            )
                            os.kill(extra_pid, signal.SIGKILL)
                        terminated.append(extra_pid)
                    except OSError as exc:
                        _log(
                            f"[Watchdog] Error stopping extra daemon "
                            f"{extra_pid}: {exc}"
                        )

                if _read_pid_file(DAEMON_PID_FILE) != keep_pid:
                    _write_pid_file(DAEMON_PID_FILE, keep_pid)
                self._tracked_daemon_pid = keep_pid

                _notify_slack_async(
                    f":warning: *[Supervisor Watchdog] Duplicate daemon "
                    f"terminated on {site_name}* (`{hostname}`)\n"
                    f"     • *Kept PID (in pidfile):* `{keep_pid}`\n"
                    f"     • *Terminated duplicate PID(s):* `{terminated}`"
                )
                return

            # If exactly 1 daemon is running, ensure pidfile & tracked_pid match
            if len(running_pids) == 1:
                live_pid = running_pids[0]
                if pidfile_pid != live_pid:
                    _log(
                        f"[Watchdog] Repairing daemon pidfile {DAEMON_PID_FILE} "
                        f"(was {pidfile_pid}, writing live pid {live_pid})."
                    )
                    _write_pid_file(DAEMON_PID_FILE, live_pid)
                self._tracked_daemon_pid = live_pid
                self._check_daemon_progress(live_pid, site_name, hostname)
                return

            # Check if pidfile_pid or tracked_daemon_pid is alive even if `ps`
            # was unavailable
            if pidfile_pid is not None and _pid_is_alive(pidfile_pid):
                self._tracked_daemon_pid = pidfile_pid
                self._check_daemon_progress(pidfile_pid, site_name, hostname)
                return
            if (
                self._tracked_daemon_pid is not None
                and _pid_is_alive(self._tracked_daemon_pid)
            ):
                if pidfile_pid != self._tracked_daemon_pid:
                    _write_pid_file(DAEMON_PID_FILE, self._tracked_daemon_pid)
                self._check_daemon_progress(
                    self._tracked_daemon_pid, site_name, hostname
                )
                return

            # 0 daemon processes are alive!
            dead_pid = self._tracked_daemon_pid or pidfile_pid
            if pidfile_pid is not None:
                cause = (
                    f"Abrupt exit / crash / SIGKILL "
                    f"(stale pidfile left with pid={pidfile_pid})"
                )
            else:
                cause = (
                    "Process stopped and pidfile removed "
                    "(manual 'stop' or SIGTERM outside supervisor)"
                )

        # Record crash timestamp & compute backoff
        now = time.time()
        self._crash_timestamps = [
            t for t in self._crash_timestamps if (now - t) < CRASH_WINDOW_SECONDS
        ]
        self._crash_timestamps.append(now)
        attempt_num = len(self._crash_timestamps)
        backoff_idx = min(
            attempt_num - 1, len(CRASH_BACKOFF_SCHEDULE_SECONDS) - 1
        )
        backoff_sec = CRASH_BACKOFF_SCHEDULE_SECONDS[backoff_idx]

        log_tail = _tail_log_lines(DAEMON_LOG_FILE, max_lines=6)
        _log(
            f"[Watchdog] Daemon (pid={dead_pid}) is NOT running! "
            f"Cause: {cause}. Relaunch attempt #{attempt_num} "
            f"(backoff={backoff_sec}s)."
        )

        if backoff_sec > 0:
            time.sleep(backoff_sec)
            if self._stopping:
                return

        restarted = self._launch_child()
        new_pid = self._tracked_daemon_pid
        if restarted:
            status_line = (
                f":white_check_mark: *Auto-Restart Succeeded:* "
                f"new daemon running (`pid={new_pid}`, attempt #{attempt_num})"
            )
            icon = ":rotating_light:"
        else:
            status_line = (
                f":x: *Auto-Restart Failed:* daemon did not confirm running "
                f"(attempt #{attempt_num}; will retry)"
            )
            icon = ":sos:"

        _notify_slack_async(
            f"{icon} *[Supervisor Watchdog] Daemon down detected on "
            f"{site_name}* (`{hostname}`)\n"
            f"     • *Previous PID:* `{dead_pid}`\n"
            f"     • *Cause:* {cause}\n"
            f"     • *Recovery:* {status_line}\n"
            f"     • *Last daemon log lines:*\n```{log_tail}```"
        )

    def _check_daemon_progress(
        self, live_pid: int, site_name: str, hostname: str
    ) -> None:
        """Validate that a live daemon process is actually advancing its main
        loop (`eventIdFile` mtime) and check for plugin cursor lag.
        """
        event_id_file = self._event_id_file
        if not event_id_file or not os.path.isfile(event_id_file):
            return

        now = time.time()
        try:
            mtime = os.path.getmtime(event_id_file)
        except OSError:
            return

        age_sec = max(0.0, now - mtime)
        uptime_since_launch = now - self._last_launch_time

        # 1. Hung/frozen main loop check:
        # shotgunEventDaemon.py saves eventIdFile on every _mainLoop pass (~5s),
        # even when 0 events are returned. If the file hasn't been touched for
        # STUCK_EVENT_ID_FILE_SECONDS while the daemon has been up at least that
        # long, the main loop is wedged.
        if (
            age_sec > STUCK_EVENT_ID_FILE_SECONDS
            and uptime_since_launch > STUCK_EVENT_ID_FILE_SECONDS
        ):
            self._recover_hung_daemon(live_pid, age_sec, site_name, hostname)
            return

        # 2. Periodic plugin cursor lag check (every PLUGIN_LAG_CHECK_INTERVAL_SECONDS)
        if (now - self._last_lag_check_time) >= PLUGIN_LAG_CHECK_INTERVAL_SECONDS:
            self._last_lag_check_time = now
            self._check_plugin_cursor_lag(live_pid, site_name, hostname)

    def _recover_hung_daemon(
        self, hung_pid: int, age_sec: float, site_name: str, hostname: str
    ) -> None:
        """Terminate a hung daemon whose `eventIdFile` has not updated in
        `age_sec` seconds, relaunch it, and alert `#log-pipe-triggers`.
        """
        age_min = age_sec / 60.0
        log_tail = _tail_log_lines(DAEMON_LOG_FILE, max_lines=6)
        _log(
            f"[Watchdog] Hung daemon main loop detected (pid={hung_pid}): "
            f"{self._event_id_file} has not been updated for {age_min:.1f}m "
            f"({int(age_sec)}s). Restarting daemon."
        )

        # Attempt graceful SIGTERM first with a bounded timeout; escalate to
        # SIGKILL if the hung daemon does not exit within the timeout.
        killed_with = "SIGTERM"
        if self.child_mode == "foreground" and self._daemon_proc is not None:
            self._daemon_proc.terminate()
            try:
                self._daemon_proc.wait(timeout=HUNG_DAEMON_STOP_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                killed_with = "SIGKILL"
                self._daemon_proc.kill()
                self._daemon_proc.wait(timeout=10)
            self._daemon_proc = None
        else:
            try:
                os.kill(hung_pid, signal.SIGTERM)
                deadline = time.time() + HUNG_DAEMON_STOP_TIMEOUT_SECONDS
                while time.time() < deadline and _pid_is_alive(hung_pid):
                    time.sleep(1)
                if _pid_is_alive(hung_pid):
                    killed_with = "SIGKILL"
                    _log(
                        f"[Watchdog] Hung daemon pid={hung_pid} did not exit "
                        f"after {HUNG_DAEMON_STOP_TIMEOUT_SECONDS}s; sending SIGKILL."
                    )
                    os.kill(hung_pid, signal.SIGKILL)
                    deadline_kill = time.time() + 10
                    while time.time() < deadline_kill and _pid_is_alive(hung_pid):
                        time.sleep(0.5)
            except OSError as exc:
                _log(f"[Watchdog] Error stopping hung daemon {hung_pid}: {exc}")

        if os.path.exists(DAEMON_PID_FILE):
            try:
                os.remove(DAEMON_PID_FILE)
            except OSError:
                pass
        self._tracked_daemon_pid = None

        restarted = self._launch_child()
        new_pid = self._tracked_daemon_pid
        if restarted:
            recovery_str = (
                f":white_check_mark: *Auto-Restart Succeeded:* "
                f"stopped hung process (`{killed_with}`) and launched "
                f"new daemon (`pid={new_pid}`)"
            )
            icon = ":snowflake:"
        else:
            recovery_str = (
                f":x: *Auto-Restart Failed:* stopped hung process "
                f"(`{killed_with}`), but new daemon did not confirm running"
            )
            icon = ":sos:"

        _notify_slack_async(
            f"{icon} *[Supervisor Watchdog] Hung daemon main loop detected on "
            f"{site_name}* (`{hostname}`)\n"
            f"     • *Hung PID:* `{hung_pid}`\n"
            f"     • *Stall Duration:* `eventIdFile` not updated for "
            f"`{age_min:.1f} min` (`{int(age_sec)}s`, threshold "
            f"`{STUCK_EVENT_ID_FILE_SECONDS}s`)\n"
            f"     • *Recovery:* {recovery_str}\n"
            f"     • *Last daemon log lines:*\n```{log_tail}```"
        )

    def _check_plugin_cursor_lag(
        self, live_pid: int, site_name: str, hostname: str
    ) -> None:
        """Inspect the on-disk `eventIdFile` for plugins whose cursor has
        fallen significantly behind the collection's max cursor, alerting
        Slack with hysteresis so notifications do not spam.
        """
        try:
            with open(self._event_id_file, "rb") as fh:
                data = pickle.load(fh)
        except Exception:
            return

        if not isinstance(data, dict):
            return

        newly_lagging = []
        recovered = []

        for col_path, plugin_states in data.items():
            if not isinstance(plugin_states, dict) or not plugin_states:
                continue

            # Only check plugins whose .py file currently exists on disk
            disk_plugins = None
            if os.path.isdir(col_path):
                try:
                    disk_plugins = {
                        os.path.splitext(fn)[0]
                        for fn in os.listdir(col_path)
                        if fn.endswith(".py") and not fn.startswith(".")
                    }
                except OSError:
                    disk_plugins = None

            cursors = {}
            backlogs = {}
            for pname, state in plugin_states.items():
                if disk_plugins is not None and pname not in disk_plugins:
                    continue
                last_id = None
                backlog = {}
                if isinstance(state, tuple) and len(state) == 2:
                    last_id, backlog = state
                elif isinstance(state, int):
                    last_id = state
                if isinstance(last_id, int):
                    cursors[pname] = last_id
                    backlogs[pname] = (
                        len(backlog) if isinstance(backlog, dict) else 0
                    )

            if not cursors:
                continue

            max_id = max(cursors.values())
            for pname, lid in cursors.items():
                lag = max_id - lid
                if (
                    lag >= PLUGIN_LAG_WARN_THRESHOLD
                    and pname not in self._lagging_plugins_alerted
                ):
                    self._lagging_plugins_alerted.add(pname)
                    newly_lagging.append((pname, lid, max_id, lag, backlogs[pname]))
                elif (
                    lag <= PLUGIN_LAG_CLEAR_THRESHOLD
                    and pname in self._lagging_plugins_alerted
                ):
                    self._lagging_plugins_alerted.discard(pname)
                    recovered.append((pname, lid, max_id, lag))

        if newly_lagging:
            newly_lagging.sort(key=lambda r: r[3], reverse=True)
            lines = [
                f"     • `{pname}`: behind by `{lag}` events "
                f"(cursor `{lid}` vs head `{max_id}`, backlog `{bk}`)"
                for pname, lid, max_id, lag, bk in newly_lagging[:10]
            ]
            _log(
                f"[Watchdog] Plugin cursor lag detected on {site_name}: "
                f"{[(r[0], r[3]) for r in newly_lagging]}"
            )
            _notify_slack_async(
                f":hourglass_flowing_sand: *[Supervisor Watchdog] Plugin cursor "
                f"lag detected on {site_name}* (`{hostname}`, daemon pid "
                f"`{live_pid}`)\n"
                + "\n".join(lines)
            )

        if recovered:
            lines = [
                f"     • `{pname}`: caught up (cursor `{lid}`, behind head by `{lag}`)"
                for pname, lid, _max_id, lag in recovered[:10]
            ]
            _log(
                f"[Watchdog] Plugin cursor lag recovered on {site_name}: "
                f"{[r[0] for r in recovered]}"
            )
            _notify_slack_async(
                f":white_check_mark: *[Supervisor Watchdog] Plugin cursor lag "
                f"recovered on {site_name}* (`{hostname}`, daemon pid "
                f"`{live_pid}`)\n"
                + "\n".join(lines)
            )

    def _run(self) -> None:
        """Launch the daemon, start the watchdog thread, wait for reload/stop
        signals, then clean up.
        """
        signal.signal(signal.SIGHUP, self._handle_reload)
        site_name, hostname, _ = _resolve_site_and_host()

        with self._lifecycle_lock:
            started = self._launch_child()

        if started:
            _notify_slack_async(
                f":rocket: *[Supervisor] Started on {site_name}* (`{hostname}`)\n"
                f"     • *Supervisor PID:* `{os.getpid()}`\n"
                f"     • *Daemon PID:* `{self._tracked_daemon_pid}` "
                f"(mode: `{self.child_mode}`)"
            )
        else:
            _notify_slack_async(
                f":warning: *[Supervisor] Started on {site_name}* (`{hostname}`), "
                f"but child daemon did not confirm within "
                f"{DAEMON_START_CONFIRM_TIMEOUT_SECONDS}s (watchdog will retry)."
            )

        watchdog_thread = threading.Thread(
            target=self._watchdog_loop,
            name="SupervisorWatchdog",
            daemon=True,
        )
        watchdog_thread.start()

        while not self._stopping:
            if self._restart_requested:
                self._restart_requested = False
                self._reload_child()
            time.sleep(1)

        with self._lifecycle_lock:
            self._ensure_child_stopped()

        _notify_slack_async(
            f":octagonal_sign: *[Supervisor] Stopped cleanly on {site_name}* "
            f"(`{hostname}`, supervisor pid `{os.getpid()}`)"
        )
        # Brief pause so the daemon thread can flush the stop notification
        time.sleep(1.0)

    def _ensure_child_stopped(self) -> None:
        """Stop the child daemon, but only ever do it once."""
        if self._child_stop_done:
            return
        self._child_stop_done = True
        self._stop_child()

    def _cleanup(self) -> None:
        """Flag for exit. Deliberately does nothing slow."""
        self._stopping = True


def main() -> int:
    """CLI entry point: start|stop|restart|foreground."""
    valid_actions = ("start", "stop", "restart", "foreground")
    action = sys.argv[1] if len(sys.argv) > 1 else None

    if action not in valid_actions:
        print(f"usage: {sys.argv[0]} {'|'.join(valid_actions)}")
        return 2

    child_mode = "foreground" if action == "foreground" else "start"
    supervisor = SupervisorDaemon(child_mode)
    getattr(supervisor, action)()
    return 0


if __name__ == "__main__":
    sys.exit(main())
