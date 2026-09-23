#!/usr/bin/env python
#
# :Module: supervisor.py
# :Description: Supervisor for shotgunEventDaemon.py. Launches the daemon
#               and restarts it on SIGHUP - the signal
#               bxlApp_triggers_git_autopull.py sends after a successful
#               autopull, so newly-pulled trigger code takes effect
#               without a human manually stopping/starting the daemon.
#
#               Usage mirrors the daemon's own CLI exactly:
#                   sudo python3 supervisor.py start|stop|restart|foreground
#               The mode given here is the mode the child daemon is
#               launched with too, and is preserved across every reload.
#

import os
import signal
import subprocess
import sys
import time

import daemonizer

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DAEMON_SCRIPT = os.path.join(SCRIPT_DIR, "shotgunEventDaemon.py")
DAEMON_PID_FILE = "/var/log/shotgunEventDaemon/shotgunEventDaemon.pid"

SUPERVISOR_PID_FILE = "/var/log/shotgunEventDaemon/shotgunEventSupervisor.pid"
SUPERVISOR_LOG_FILE = "/var/log/shotgunEventDaemon/shotgunEventSupervisor.log"

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


def _log(message: str) -> None:
    """Print a timestamped, immediately-flushed status line.

    Args:
        message (str): The message to log.
    """
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


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


class SupervisorDaemon(daemonizer.Daemon):
    """Owns the daemon's lifecycle and reloads it on SIGHUP.

    Reuses shotgunEventDaemon.py's own CLI for control instead of
    reimplementing daemon control logic: after `start`, the daemon is a
    detached, double-forked OS process, so there is no live Python
    reference to hold onto across the fork boundary anyway - shelling out
    to the same commands a human runs manually is the only mechanism that
    actually works, and it guarantees identical behavior either way.

    `foreground` mode is the one exception: `daemonizer.Daemon.start()`
    only writes the pidfile inside `_daemonize()`, which `foreground()`
    skips entirely - a foreground-mode daemon never has a pidfile, so the
    CLI `stop` command is a no-op against it. In that mode this class
    tracks the actual subprocess handle instead and signals it directly.
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
        self._restart_requested = False
        self._stopping = False
        self._child_stop_done = False
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

    def _launch_child(self) -> None:
        """Launch the daemon in self.child_mode and confirm it came up."""
        _log(f"Launching daemon in '{self.child_mode}' mode.")
        cmd = ["sudo", sys.executable, DAEMON_SCRIPT, self.child_mode]

        if self.child_mode == "start":
            # This invocation double-forks and its top-level process
            # exits quickly on its own once detached - reap it so it
            # doesn't linger as a zombie. From here on the real daemon is
            # only reachable via its own pidfile, not this Popen handle.
            #
            # stdout/stderr are discarded rather than inherited: since
            # this launcher process isn't attached to a terminal, print()
            # calls before the double-fork are buffered instead of
            # written immediately, and os.fork() duplicates that
            # unflushed buffer into every resulting process - each one
            # then flushes its own copy independently (on sys.exit(), and
            # again just before the daemon redirects to its own log),
            # which would otherwise show up here 2-3x duplicated. We
            # don't need this output anyway - success is confirmed via
            # the daemon's own pidfile below, not by reading stdout.
            subprocess.Popen(
                cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            ).wait(timeout=DAEMON_START_CONFIRM_TIMEOUT_SECONDS)
            self._daemon_proc = None
            self._confirm_daemon_started()
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

    def _confirm_daemon_started(self) -> None:
        """Poll for a live daemon pidfile after a `start` launch.

        Only meaningful in `start` mode - `foreground` mode is confirmed
        via the Popen handle directly in `_launch_child`.
        """
        deadline = time.time() + DAEMON_START_CONFIRM_TIMEOUT_SECONDS
        while time.time() < deadline:
            pid = _read_pid_file(DAEMON_PID_FILE)
            if pid is not None and _pid_is_alive(pid):
                _log(f"Daemon confirmed running (pid {pid}).")
                return
            time.sleep(0.5)
        _log(
            "Daemon did not confirm as running within "
            f"{DAEMON_START_CONFIRM_TIMEOUT_SECONDS}s of launch."
        )

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
        """Stop a `start`-mode daemon via its own pidfile-based `stop`.

        Blocks until the daemon's pid is actually confirmed gone from the
        OS's perspective - there is no timeout after which this gives up
        and reports success anyway. `daemon.py`'s own `_delpid()` removes
        the pidfile *before* running `_cleanup()` (which drains every
        plugin's queue and can take a while - see Engine.stop()), so the
        pidfile disappearing is not proof the process has actually
        exited; only `_pid_is_alive()` on the pid captured before 'stop'
        was invoked is. Returning early on a slow stop is exactly what let
        the supervisor launch a replacement while the old process was
        still alive, producing two daemons racing over the same
        eventIdFile - see _reload_child(), which refuses to launch
        unless this returns True.
        """
        pid = _read_pid_file(DAEMON_PID_FILE)
        if pid is None:
            _log("No daemon pidfile found; nothing to stop.")
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

        # The daemon pid is confirmed gone, so the helper's own SIGTERM
        # loop (daemonizer.Daemon.stop(), unmodified) has already seen
        # "No such process" and returned - this just reaps it instead of
        # leaving it a zombie until the long-running supervisor process
        # happens to exit. Best-effort only: a slow-to-exit sudo wrapper
        # here is a zombie-cleanup nicety, not a reason to fail a stop
        # that's already confirmed by the pid check above.
        try:
            stop_helper.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _log(
                f"Note: the 'stop' helper process (pid {stop_helper.pid}) "
                f"hasn't exited yet even though the daemon itself has - "
                f"harmless, it will be reaped eventually."
            )

        if os.path.exists(DAEMON_PID_FILE):
            _log(
                f"Warning: daemon pidfile still present even though pid "
                f"{pid} has exited. Leaving it - check manually before "
                f"the next start."
            )
        else:
            _log(f"Daemon stop confirmed (pid {pid} exited, pidfile gone).")
        return True

    def _stop_child_via_process_handle(self) -> bool:
        """Stop a `foreground`-mode daemon directly - it has no pidfile.

        Blocks until the tracked process is actually confirmed exited -
        no timeout after which this gives up and reports success anyway
        (see _stop_child_via_cli's docstring for why that matters). No
        SIGKILL escalation either: a process that ignores SIGTERM this
        long is worth a human looking at, not the supervisor forcing it
        closed.
        """
        if self._daemon_proc is None or self._daemon_proc.poll() is not None:
            self._daemon_proc = None
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
        return True

    def _reload_child(self) -> None:
        """Stop and relaunch the daemon in its original mode.

        Never launches a replacement unless the previous process is
        confirmed fully stopped first: two daemon processes racing over
        the same eventIdFile is exactly the duplicate-event-processing
        bug a rapid burst of autopull-triggered SIGHUPs exposed (each
        successful reload re-signals on the next successful pull, so a
        stop that silently "gave up" while the old process was still
        draining its queues meant the new one started fetching from a
        stale checkpoint while the old one was still finishing).
        """
        _log("Reloading daemon.")
        if not self._stop_child():
            _log(
                "Reload aborted: could not confirm the daemon fully "
                "stopped. Not launching a replacement - two daemons "
                "running at once is worse than one running old code a "
                "bit longer. Investigate manually, then send SIGHUP "
                "again once the old process is confirmed gone."
            )
            return
        self._launch_child()
        _log("Reload complete.")

    def _run(self) -> None:
        """Launch the daemon, wait for reload/stop signals, then clean up.

        The actual child shutdown happens here, after the loop exits, in
        plain control flow - never inside `_cleanup`/the SIGTERM handler
        itself. See `_cleanup` for why that distinction matters.
        """
        signal.signal(signal.SIGHUP, self._handle_reload)
        self._launch_child()

        while not self._stopping:
            if self._restart_requested:
                self._restart_requested = False
                self._reload_child()
            time.sleep(1)

        self._ensure_child_stopped()

    def _ensure_child_stopped(self) -> None:
        """Stop the child daemon, but only ever do it once."""
        if self._child_stop_done:
            return
        self._child_stop_done = True
        self._stop_child()

    def _cleanup(self) -> None:
        """Flag for exit. Deliberately does nothing slow.

        Called synchronously from daemonizer.Daemon's SIGTERM handler (via
        `_delpid`), which runs *inside* signal delivery - and again from
        `atexit` on normal process exit. This must never block: `stop()`
        (also inherited, unmodified) sends SIGTERM in a loop every 0.1s
        until the process is confirmed dead. If this handler did the
        actual (slow) child shutdown directly - as an earlier version of
        this file did - each subsequent SIGTERM arriving while that
        subprocess call was still in flight would interrupt it and
        re-invoke this same handler again (Python retries signal-safe
        syscalls per PEP 475, re-checking for pending signals first),
        stacking up overlapping shutdown attempts recursively for as long
        as signals kept arriving - which is exactly what produced 100+
        duplicate 'stop' Slack notifications in testing, because the
        process never actually got a chance to exit. Setting a flag is
        atomic and safe to repeat any number of times; the real shutdown
        work happens once, in `_run`'s normal control flow, via
        `_ensure_child_stopped`.
        """
        self._stopping = True


def main() -> int:
    """CLI entry point: start|stop|restart|foreground."""
    valid_actions = ("start", "stop", "restart", "foreground")
    action = sys.argv[1] if len(sys.argv) > 1 else None

    if action not in valid_actions:
        print(f"usage: {sys.argv[0]} {'|'.join(valid_actions)}")
        return 2

    # `stop` targets an already-running supervisor via its pidfile, so the
    # child mode here is unused - `start`/`restart` default to daemonized,
    # matching daemonizer.Daemon's own default.
    child_mode = "foreground" if action == "foreground" else "start"
    supervisor = SupervisorDaemon(child_mode)
    getattr(supervisor, action)()
    return 0


if __name__ == "__main__":
    sys.exit(main())
