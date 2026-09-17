#!/usr/bin/env python
#
# Init file for Flow Production Tracking event daemon
#
# chkconfig: 345 99 00
# description: Flow Production Tracking event daemon
#
### BEGIN INIT INFO
# Provides: shotgunEvent
# Required-Start: $network
# Should-Start: $remote_fs
# Required-Stop: $network
# Should-Stop: $remote_fs
# Default-Start: 2 3 4 5
# Short-Description: Flow Production Tracking event daemon
# Description: Flow Production Tracking event daemon
### END INIT INFO

"""
For an overview of shotgunEvents, please see raw documentation in the docs
folder or an html compiled version at:

http://shotgunsoftware.github.com/shotgunEvents
"""

__version__ = "1.0"
__version_info__ = (1, 0)

import datetime
import json
import threading
import logging
import logging.handlers
import os
import pprint
import queue
import socket
import sys
import time
import traceback
import configparser
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed

#
# BOXEL Customizations
#
print("Boxel adding third party modules")
packages = r"/srv/shotgunEvents-master/src/site-packages"
if packages not in sys.path:
    sys.path.append(packages)

print("Boxel adding triggers modules")
sys.path.append(r"/srv/shotgunEvents-master/src/bxl_triggers")

# Boxel modules
from bxl_triggers.common import slack_msj

from distutils.version import StrictVersion


if sys.platform == "win32":
    import win32serviceutil
    import win32service
    import win32event
    import servicemanager

import daemonizer
import db_logger
import shotgun_api3 as sg
from shotgun_api3.lib.sgtimezone import SgTimezone

import importlib_wrapper

SG_TIMEZONE = SgTimezone()

EMAIL_FORMAT_STRING = """Time: %(asctime)s
Logger: %(name)s
Path: %(pathname)s
Function: %(funcName)s
Line: %(lineno)d

%(message)s"""


def _setFilePathOnLogger(logger, path):
    # Remove any previous handler.
    _removeHandlersFromLogger(logger, logging.handlers.TimedRotatingFileHandler)

    # Add the file handler
    handler = logging.handlers.TimedRotatingFileHandler(
        path, "midnight", backupCount=10
    )
    handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(handler)


def _removeHandlersFromLogger(logger, handlerTypes=None):
    """
    Remove all handlers or handlers of a specified type from a logger.

    @param logger: The logger who's handlers should be processed.
    @type logger: A logging.Logger object
    @param handlerTypes: A type of handler or list/tuple of types of handlers
        that should be removed from the logger. If I{None}, all handlers are
        removed.
    @type handlerTypes: L{None}, a logging.Handler subclass or
        I{list}/I{tuple} of logging.Handler subclasses.
    """
    for handler in logger.handlers:
        if handlerTypes is None or isinstance(handler, handlerTypes):
            logger.removeHandler(handler)


def _addMailHandlerToLogger(
    logger,
    smtpServer,
    fromAddr,
    toAddrs,
    emailSubject,
    username=None,
    password=None,
    secure=None,
):
    """
    Configure a logger with a handler that sends emails to specified
    addresses.

    The format of the email is defined by L{LogFactory.EMAIL_FORMAT_STRING}.

    @note: Any SMTPHandler already connected to the logger will be removed.

    @param logger: The logger to configure
    @type logger: A logging.Logger instance
    @param toAddrs: The addresses to send the email to.
    @type toAddrs: A list of email addresses that will be passed on to the
        SMTPHandler.
    """
    if smtpServer and fromAddr and toAddrs and emailSubject:
        mailHandler = CustomSMTPHandler(
            smtpServer, fromAddr, toAddrs, emailSubject, (username, password), secure
        )
        mailHandler.setLevel(logging.ERROR)
        mailFormatter = logging.Formatter(EMAIL_FORMAT_STRING)
        mailHandler.setFormatter(mailFormatter)

        logger.addHandler(mailHandler)


class Config(configparser.ConfigParser):
    def __init__(self, path):
        configparser.ConfigParser.__init__(self, os.environ)
        self.read(path)

    def getShotgunURL(self):
        return self.get("shotgun", "server")

    def getEngineScriptName(self):
        return self.get("shotgun", "name")

    def getEngineScriptKey(self):
        return self.get("shotgun", "key")

    def getEngineProxyServer(self):
        try:
            proxy_server = self.get("shotgun", "proxy_server").strip()
            if not proxy_server:
                return None
            return proxy_server
        except configparser.NoOptionError:
            return None

    def getEventIdFile(self):
        return self.get("daemon", "eventIdFile")

    def getEnginePIDFile(self):
        return self.get("daemon", "pidFile")

    def getPluginPaths(self):
        return [s.strip() for s in self.get("plugins", "paths").split(",")]

    def getSMTPEnabled(self):
        return self.getboolean("emails", "enabled")

    def getSMTPServer(self):
        return self.get("emails", "server")

    def getSMTPPort(self):
        if self.has_option("emails", "port"):
            return self.getint("emails", "port")
        return 25

    def getFromAddr(self):
        return self.get("emails", "from")

    def getToAddrs(self):
        return [s.strip() for s in self.get("emails", "to").split(",")]

    def getEmailSubject(self):
        return self.get("emails", "subject")

    def getEmailUsername(self):
        if self.has_option("emails", "username"):
            return self.get("emails", "username")
        return None

    def getEmailPassword(self):
        if self.has_option("emails", "password"):
            return self.get("emails", "password")
        return None

    def getSecureSMTP(self):
        if self.has_option("emails", "useTLS"):
            return self.getboolean("emails", "useTLS") or False
        return False

    def getLogMode(self):
        return self.getint("daemon", "logMode")

    def getLogLevel(self):
        return self.getint("daemon", "logging")

    def getMaxEventBatchSize(self):
        if self.has_option("daemon", "max_event_batch_size"):
            return self.getint("daemon", "max_event_batch_size")
        return 500

    def getPluginBacklogAlertThreshold(self):
        """Pending-event count (dispatched but not yet processed) at
        which a plugin is considered critically behind. Defaults to
        twice the batch size - i.e. already two full fetch cycles
        behind, not just briefly lagging within one."""
        if self.has_option("daemon", "plugin_backlog_alert_threshold"):
            return self.getint("daemon", "plugin_backlog_alert_threshold")
        return self.getMaxEventBatchSize() * 2

    def getPluginBacklogClearThreshold(self):
        """Pending-event count below which a plugin's backlog alert is
        cleared, allowing it to fire again later. Kept lower than the
        alert threshold (hysteresis) so a count oscillating right at
        one boundary doesn't spam repeated alerts."""
        if self.has_option("daemon", "plugin_backlog_clear_threshold"):
            return self.getint("daemon", "plugin_backlog_clear_threshold")
        return int(self.getPluginBacklogAlertThreshold() * 0.7)

    def getLogFile(self, filename=None):
        if filename is None:
            if self.has_option("daemon", "logFile"):
                filename = self.get("daemon", "logFile")
            else:
                raise ConfigError("The config file has no logFile option.")

        if self.has_option("daemon", "logPath"):
            path = self.get("daemon", "logPath")

            if not os.path.exists(path):
                os.makedirs(path)
            elif not os.path.isdir(path):
                raise ConfigError(
                    "The logPath value in the config should point to a directory."
                )

            path = os.path.join(path, filename)

        else:
            path = filename

        return path

    def getTimingLogFile(self):
        if (
            not self.has_option("daemon", "timing_log")
            or self.get("daemon", "timing_log") != "on"
        ):
            return None

        return self.getLogFile() + ".timing"

    def getDatabaseLogEnabled(self):
        if not self.has_section("database_log"):
            return False
        if not self.has_option("database_log", "enabled"):
            return False
        return self.getboolean("database_log", "enabled")


class Engine(object):
    """
    The engine holds the main loop of event processing.
    """

    def __init__(self, configPath):
        """ """
        self._continue = True
        self._eventIdData = {}
        self.db_logger = None

        # Populated once at startup by _loadDisabledEventLogScriptIds()
        # (called from run(), after self._sg below exists) - empty here
        # only as a safe default for anything that might run before
        # that (e.g. tests constructing an Engine directly).
        self._disabledEventLogScriptIds = set()

        # Read/parse the config
        self.config = Config(configPath)

        # Get config values
        self._pluginCollections = [
            PluginCollection(self, s) for s in self.config.getPluginPaths()
        ]
        self._sg = sg.Shotgun(
            self.config.getShotgunURL(),
            self.config.getEngineScriptName(),
            self.config.getEngineScriptKey(),
            http_proxy=self.config.getEngineProxyServer(),
        )
        self._max_conn_retries = self.config.getint("daemon", "max_conn_retries")
        self._conn_retry_sleep = self.config.getint("daemon", "conn_retry_sleep")
        self._fetch_interval = self.config.getint("daemon", "fetch_interval")
        self._use_session_uuid = self.config.getboolean("shotgun", "use_session_uuid")

        # Setup the loggers for the main engine
        if self.config.getLogMode() == 0:
            # Set the root logger for file output.
            rootLogger = logging.getLogger()
            rootLogger.config = self.config
            _setFilePathOnLogger(rootLogger, self.config.getLogFile())
            print(self.config.getLogFile())

            # Set the engine logger for email output.
            self.log = logging.getLogger("engine")
            self.setEmailsOnLogger(self.log, True)
        else:
            # Set the engine logger for file and email output.
            self.log = logging.getLogger("engine")
            self.log.config = self.config
            _setFilePathOnLogger(self.log, self.config.getLogFile())
            self.setEmailsOnLogger(self.log, True)

        self.log.setLevel(self.config.getLogLevel())

        # Setup the timing log file
        timing_log_filename = self.config.getTimingLogFile()
        if timing_log_filename:
            self.timing_logger = logging.getLogger("timing")
            self.timing_logger.setLevel(self.config.getLogLevel())
            _setFilePathOnLogger(self.timing_logger, timing_log_filename)
        else:
            self.timing_logger = None

        # Constructed here so plugins can attach capture handlers; started in
        # Engine.start() after daemonize() so the writer thread survives fork.
        # __init__ runs in the pre-fork parent process (LinuxDaemon.__init__
        # constructs the Engine before daemonizer.Daemon.start() forks) -
        # threads started here would not exist in the post-fork daemon
        # process at all (fork only carries over the calling thread), so
        # db_logger.start()'s writer/flush threads have to wait until
        # start() below, which runs after the fork, inside the real daemon
        # process.
        self.db_logger = None
        if self.config.getDatabaseLogEnabled():
            try:
                self.db_logger = db_logger.DatabaseLogger(self.config, self.log)
            except Exception:
                self.log.error(
                    "Failed to configure database logging; continuing with file logging only.\n\n%s",
                    traceback.format_exc(),
                )
                self.db_logger = None

        super().__init__()

    def setEmailsOnLogger(self, logger, emails):
        # Configure the logger for email output
        _removeHandlersFromLogger(logger, logging.handlers.SMTPHandler)

        if emails is False:
            return

        if not self.config.getSMTPEnabled():
            return

        smtpServer = self.config.getSMTPServer()
        smtpPort = self.config.getSMTPPort()
        fromAddr = self.config.getFromAddr()
        emailSubject = self.config.getEmailSubject()
        username = self.config.getEmailUsername()
        password = self.config.getEmailPassword()
        if self.config.getSecureSMTP():
            secure = (None, None)
        else:
            secure = None

        if emails is True:
            toAddrs = self.config.getToAddrs()
        elif isinstance(emails, (list, tuple)):
            toAddrs = emails
        else:
            msg = "Argument emails should be True to use the default addresses, False to not send any emails or a list of recipient addresses. Got %s."
            raise ValueError(msg % type(emails))

        _addMailHandlerToLogger(
            logger,
            (smtpServer, smtpPort),
            fromAddr,
            toAddrs,
            emailSubject,
            username,
            password,
            secure,
        )

    def start(self):
        """
        Start the processing of events.

        The last processed id is loaded up from persistent storage on disk and
        the main loop is started.
        """
        # TODO: Take value from config
        socket.setdefaulttimeout(60)

        # Notify which version of shotgun api we are using
        self.log.info("Using SG Python API version %s" % sg.__version__)

        # Runs here (not Engine.__init__) so the writer/flush threads are
        # created in the actual daemon process, after daemonize()'s fork -
        # see the comment in __init__ for why starting them any earlier
        # would silently lose them.
        if self.db_logger is not None:
            try:
                self.db_logger.start()
            except Exception:
                self.log.error(
                    "Failed to start database logging; continuing with file logging only.\n\n%s",
                    traceback.format_exc(),
                )
                self.db_logger = None

        try:
            for collection in self._pluginCollections:
                collection.load()

            self._loadDisabledEventLogScriptIds()
            self._loadEventIdData()

            self._mainLoop()
        except KeyboardInterrupt:
            self.log.warning("Keyboard interrupt. Cleaning up...")
            # Fire-and-forget: a blocking send_slack_message() call
            # here would hold up shutdown itself if Slack (or the
            # network) is slow to respond, forcing a second Ctrl+C to
            # actually kill the process instead of exiting cleanly -
            # observed directly once already (the traceback showed
            # this call still stuck in sock.connect() when the second
            # KeyboardInterrupt landed).
            threading.Thread(
                target=slack_msj.send_slack_message,
                args=("Boxel site: Daemon stopped by user.",),
                daemon=True,
            ).start()
        except Exception as err:
            msg = "Crash!!!!! Unexpected error (%s) in main loop.\n\n%s"
            self.log.critical(msg, type(err), traceback.format_exc())

            threading.Thread(
                target=slack_msj.send_slack_message,
                args=(msg % (type(err), traceback.format_exc()),),
                daemon=True,
            ).start()

    def _loadEventIdData(self):
        """
        Load the last processed event id from the disk

        If no event has ever been processed or if the eventIdFile has been
        deleted from disk, no id will be recoverable. In this case, we will try
        contacting Shotgun to get the latest event's id and we'll start
        processing from there.
        """
        eventIdFile = self.config.getEventIdFile()

        if eventIdFile and os.path.exists(eventIdFile):
            try:
                fh = open(eventIdFile, "rb")
                try:
                    self._eventIdData = pickle.load(fh)

                    # Provide event id info to the plugin collections. Once
                    # they've figured out what to do with it, ask them for their
                    # last processed id.
                    noStateCollections = []
                    for collection in self._pluginCollections:
                        state = self._eventIdData.get(collection.path)
                        if state:
                            collection.setState(state)
                        else:
                            noStateCollections.append(collection)

                    # If we don't have a state it means there's no match
                    # in the id file. First we'll search to see the latest id a
                    # matching plugin name has elsewhere in the id file. We do
                    # this as a fallback in case the plugins directory has been
                    # moved. If there's no match, use the latest event id
                    # in Shotgun.
                    if noStateCollections:
                        maxPluginStates = {}
                        for collection in self._eventIdData.values():
                            for pluginName, pluginState in collection.items():
                                if pluginName in maxPluginStates.keys():
                                    if pluginState[0] > maxPluginStates[pluginName][0]:
                                        maxPluginStates[pluginName] = pluginState
                                else:
                                    maxPluginStates[pluginName] = pluginState

                        lastEventId = self._getLastEventIdFromDatabase()
                        for collection in noStateCollections:
                            state = collection.getState()
                            for pluginName in state.keys():
                                if pluginName in maxPluginStates.keys():
                                    state[pluginName] = maxPluginStates[pluginName]
                                else:
                                    state[pluginName] = lastEventId
                            collection.setState(state)

                except pickle.UnpicklingError:
                    fh.close()

                    # Backwards compatibility:
                    # Reopen the file to try to read an old-style int
                    fh = open(eventIdFile, "rb")
                    line = fh.readline().strip()
                    if line.isdigit():
                        # The _loadEventIdData got an old-style id file containing a single
                        # int which is the last id properly processed.
                        lastEventId = int(line)
                        self.log.debug(
                            "Read last event id (%d) from file.", lastEventId
                        )
                        for collection in self._pluginCollections:
                            collection.setState(lastEventId)
                except EOFError:
                    # The file exists but is completely empty - e.g. a
                    # previous run was killed (Ctrl+C, SIGTERM, crash)
                    # between _saveEventIdData() truncating the file
                    # and pickle.dump() actually writing to it. There's
                    # no legacy int to recover here (nothing at all to
                    # read), so treat this exactly like a missing file:
                    # fall back to the latest event id in Shotgun
                    # instead of silently leaving every collection
                    # with no starting point, which would make
                    # _getNewEvents() never fetch anything at all.
                    fh.close()
                    self.log.warning(
                        "Event id file %s exists but is empty (likely "
                        "left behind by an interrupted write) - "
                        "falling back to the latest event id in "
                        "Shotgun.",
                        eventIdFile,
                    )
                    lastEventId = self._getLastEventIdFromDatabase()
                    if lastEventId:
                        for collection in self._pluginCollections:
                            collection.setState(lastEventId)
                fh.close()
            except OSError as err:
                raise EventDaemonError(
                    "Could not load event id from file.\n\n%s" % traceback.format_exc()
                )
        else:
            # No id file?
            # Get the event data from the database.
            lastEventId = self._getLastEventIdFromDatabase()
            if lastEventId:
                for collection in self._pluginCollections:
                    collection.setState(lastEventId)

            self._saveEventIdData()

    def _loadDisabledEventLogScriptIds(self):
        """
        One-time query, at daemon startup, for every ApiUser (Script)
        with generate_event_log_entries disabled - the single source
        of truth already maintained in Shotgun itself (each Script's
        own checkbox), instead of a separately maintained list this
        daemon's config would otherwise have to duplicate and keep in
        sync by hand.

        Used two ways:
        - _getNewEvents() excludes these scripts' own (non-sudo)
          writes directly in the EventLogEntry query, the same way
          _getRegisteredEventTypes() excludes whole event types.
        - isSuppressedSudoEvent() recognizes a sudo_as_login write
          authored by one of them after the fact, via
          event["meta"]["sudo_actual_user"] - generate_event_log_entries
          is bypassed entirely for a sudo_as_login write (confirmed
          live for one Script), so the query filter above can't catch
          those; they can only be caught post-fetch.

        Queried once at startup, not periodically: in practice a
        script's generate_event_log_entries setting changes at the
        same time as a trigger release, which already restarts this
        daemon via the autopull/reload cycle - there's no realistic
        window where the two drift apart.
        """
        conn_attempts = 0
        while self._continue:
            try:
                scripts = self._sg.find(
                    "ApiUser",
                    [["generate_event_log_entries", "is", False]],
                    ["id", "firstname"],
                )
            except (sg.ProtocolError, sg.ResponseError, socket.error) as err:
                conn_attempts = self._checkConnectionAttempts(conn_attempts, str(err))
            except Exception as err:
                msg = "Unknown error: %s" % str(err)
                conn_attempts = self._checkConnectionAttempts(conn_attempts, msg)
            else:
                self._disabledEventLogScriptIds = {s["id"] for s in scripts}
                self.log.info(
                    "%d script(s) with generate_event_log_entries "
                    "disabled: %s",
                    len(scripts),
                    sorted(s.get("firstname") for s in scripts),
                )
                return

    def _getLastEventIdFromDatabase(self):
        conn_attempts = 0
        lastEventId = None
        while lastEventId is None:
            order = [{"column": "id", "direction": "desc"}]
            try:
                result = self._sg.find_one(
                    "EventLogEntry", filters=[], fields=["id"], order=order
                )
            except (sg.ProtocolError, sg.ResponseError, socket.error) as err:
                conn_attempts = self._checkConnectionAttempts(conn_attempts, str(err))
            except Exception as err:
                msg = "Unknown error: %s" % str(err)
                conn_attempts = self._checkConnectionAttempts(conn_attempts, msg)
            else:
                lastEventId = result["id"]
                self.log.info("Last event id (%d) from the SG database.", lastEventId)

        return lastEventId

    def _getRegisteredEventTypes(self):
        """
        Union of event_type strings some active plugin, in some
        collection, currently registered a callback for - or None if
        any callback anywhere matches every event type, in which case
        _getNewEvents() must not filter by event_type at all.

        Recomputed fresh on every call (cheap - purely in-memory,
        no Shotgun round trip) rather than cached, since it must
        reflect whatever collection.load() just picked up this same
        _mainLoop() pass (a newly added or removed plugin file changes
        this set immediately, not on the next restart).
        """
        event_types = set()
        for collection in self._pluginCollections:
            matched = collection.getMatchedEventTypes()
            if matched is None:
                return None
            event_types.update(matched)
        return event_types

    def isSuppressedSudoEvent(self, event):
        """
        True if this event was authored via sudo_as_login by a Script
        in _disabledEventLogScriptIds (see
        _loadDisabledEventLogScriptIds).

        generate_event_log_entries=False on a Script only suppresses
        the events it logs acting as itself - _getNewEvents()'s
        "user" not_in filter already handles that case in the query
        itself. A sudo_as_login write bypasses that flag entirely
        though: it still gets an EventLogEntry, attributed to the
        sudo'd human as event["user"], with the actual Script recorded
        separately in event["meta"]["sudo_actual_user"] (confirmed
        live for one Script that has generate_event_log_entries
        disabled). Since EventLogEntry.meta is a "serializable" (blob)
        field - not queryable - this can't be excluded in the SG query;
        it has to be checked here, after the event is already fetched.

        Called from Plugin._process() so every plugin's own cursor/
        backlog bookkeeping still advances normally for a suppressed
        event (see _updateLastEventId) - only the callbacks' own work
        is skipped, the same as if canProcess() had returned False for
        all of them.
        """
        if not self._disabledEventLogScriptIds:
            return False

        meta = event.get("meta")
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except (TypeError, ValueError):
                return False
        if not isinstance(meta, dict):
            return False

        sudo_actual_user = meta.get("sudo_actual_user")
        if not isinstance(sudo_actual_user, dict):
            return False

        return sudo_actual_user.get("id") in self._disabledEventLogScriptIds

    def _mainLoop(self):
        """
        Run the event processing loop.

        General behavior:
        - Load plugins from disk - see L{load} method.
        - Get new events from Shotgun
        - Loop through events
        - Hand each event off to every active plugin's own worker thread -
          see L{PluginCollection.process}. This does not wait for the
          plugins to actually finish with the event: each plugin has a
          persistent worker thread that consumes its own queue of events at
          its own pace, independently of every other plugin. A plugin that
          crashes or takes a long time on one event never blocks or delays
          any other plugin, nor does it block the engine from moving on to
          the next event.
        - Once all events in the current batch have been handed off, save
          whatever event id state has actually been processed so far (this
          is a checkpoint for restart recovery - it may lag behind what's
          been dispatched if some plugins are still working through their
          queue, which is fine: on restart those events will simply be
          redelivered).
        - Once all events are processed, wait for the defined fetch interval time and start over.

        Caveats:
        - If a plugin is deemed "inactive" (an error occured during
          registration), skip it.
        - If a callback is deemed "inactive" (an error occured during callback
          execution), skip it.
        - Each time through the loop, if the pidFile is gone, stop.
        """
        self.log.debug("Starting the event processing loop.")
        while self._continue:
            # Process events
            events = self._getNewEvents()
            for event in events:
                for collection in self._pluginCollections:
                    collection.process(event)
            self._saveEventIdData()
            self._checkPluginBacklogs()

            # if we're lagging behind Shotgun, we received a full batch of events
            # skip the sleep() call in this case
            if len(events) < self.config.getMaxEventBatchSize():
                time.sleep(self._fetch_interval)

            # Reload plugins
            for collection in self._pluginCollections:
                collection.load()

            # Make sure that newly loaded events have proper state.
            self._loadEventIdData()

        self.log.debug("Shuting down event processing loop.")

    def stop(self):
        if not self._continue:
            # Already stopped, or a stop is already underway. daemonizer.Daemon
            # calls _cleanup() (which calls this) from two independent places:
            # the SIGTERM/SIGINT handler (termHandler, guarded against a
            # second signal re-entering it) and an unconditional
            # atexit.register(self._delpid) that always fires again once the
            # process is actually exiting - including after a signal-triggered
            # stop already ran this method to completion. That second call
            # used to be a harmless no-op (Plugin.shutdown() already checks
            # its worker thread is alive before doing anything), but
            # PluginCollection.shutdown() unconditionally building a fresh
            # ThreadPoolExecutor is not: by the time atexit callbacks run,
            # concurrent.futures.thread's own shutdown machinery may already
            # be tearing down, and submitting to a brand new executor at that
            # point raises "cannot schedule new futures after interpreter
            # shutdown" instead of quietly doing nothing. Bail out here
            # before doing any of that a second time.
            return

        self._continue = False
        for collection in self._pluginCollections:
            collection.shutdown()

        if self.db_logger is not None:
            self.db_logger.shutdown()
            self.db_logger = None

        # collection.shutdown() blocks until every plugin's queue is fully
        # drained (see PluginCollection.shutdown()/Plugin.shutdown()), so
        # by this point every plugin's _lastEventId reflects everything it
        # actually finished processing - including whatever ran after the
        # last periodic save inside _mainLoop() (that one only runs once
        # per fetch iteration, not on every event a worker thread
        # completes). Without this final save, any event processed in that
        # gap between the last periodic save and shutdown was never
        # persisted: on restart the checkpoint on disk still points before
        # it, so it gets re-fetched and reprocessed - this is exactly how
        # a restart mid-flight (e.g. the autopull/SIGHUP reload cycle)
        # produced duplicate processing of the same event across two
        # process instances.
        self._saveEventIdData()

    def _getNewEvents(self):
        """
        Fetch new events from Shotgun.

        @return: Recent events that need to be processed by the engine.
        @rtype: I{list} of Flow Production Tracking event dictionaries.
        """
        nextEventId = None
        for newId in [
            coll.getNextUnprocessedEventId() for coll in self._pluginCollections
        ]:
            if newId is not None and (nextEventId is None or newId < nextEventId):
                nextEventId = newId

        if nextEventId is not None:
            filters = [["id", "greater_than", nextEventId - 1]]

            # Narrow the query to only the event types some callback
            # actually registered for. PluginCollection.process() still
            # hands every fetched event to every active plugin regardless
            # of its own callbacks' event types (unchanged below) - this
            # only stops fetching+dispatching types no plugin anywhere
            # cares about in the first place (e.g. Shotgun_PublishedFile_Change,
            # Shotgun_Attachment_View - ~36% of one measured day's total
            # volume, with zero registered listeners). registeredEventTypes
            # is None if any callback matches everything, in which case
            # this can't safely exclude anything.
            registeredEventTypes = self._getRegisteredEventTypes()
            if registeredEventTypes is not None:
                filters.append(
                    ["event_type", "in", sorted(registeredEventTypes)])

            # Also exclude direct (non-sudo) writes from any Script with
            # generate_event_log_entries disabled (see
            # _loadDisabledEventLogScriptIds) - confirmed live that
            # "not_in" is a valid operator against EventLogEntry.user.
            # This does NOT catch a sudo_as_login write from one of
            # these scripts (event["user"] there is the sudo'd human,
            # not the script) - that case is only catchable after the
            # fetch, via isSuppressedSudoEvent().
            if self._disabledEventLogScriptIds:
                filters.append([
                    "user", "not_in",
                    [{"type": "ApiUser", "id": scriptId}
                     for scriptId in sorted(self._disabledEventLogScriptIds)],
                ])

            fields = [
                "id",
                "event_type",
                "attribute_name",
                "meta",
                "entity",
                "user",
                "project",
                "session_uuid",
                "created_at",
            ]
            order = [{"column": "id", "direction": "asc"}]

            conn_attempts = 0
            # Was `while True:`, which never looked at self._continue - a
            # SIGTERM/SIGINT during a connection outage sets that flag, but
            # this loop kept retrying forever regardless, so stop()/the
            # supervisor's own stop attempt would hang until the outage
            # resolved on its own (measured over 2 hours in practice).
            # supervisor.py now waits indefinitely (not just a fixed
            # timeout) for a clean stop before launching a replacement,
            # so a stuck outage here would otherwise block every future
            # reload, not just this one.
            while self._continue:
                try:
                    events = self._sg.find(
                        "EventLogEntry",
                        filters,
                        fields,
                        order,
                        limit=self.config.getMaxEventBatchSize(),
                    )
                    if events:
                        self.log.debug(
                            "Got %d events: %d to %d.",
                            len(events),
                            events[0]["id"],
                            events[-1]["id"],
                        )
                    return events
                except (sg.ProtocolError, sg.ResponseError, socket.error) as err:
                    conn_attempts = self._checkConnectionAttempts(
                        conn_attempts, str(err)
                    )
                except Exception as err:
                    msg = "Unknown error: %s" % str(err)
                    conn_attempts = self._checkConnectionAttempts(conn_attempts, msg)

        return []

    def _saveEventIdData(self):
        """
        Save an event Id to persistant storage.

        Next time the engine is started it will try to read the event id from
        this location to know at which event it should start processing.
        """
        eventIdFile = self.config.getEventIdFile()

        if eventIdFile is not None:
            for collection in self._pluginCollections:
                self._eventIdData[collection.path] = collection.getState()

            for colPath, state in self._eventIdData.items():
                if state:
                    try:
                        # Write to a temp file first, then atomically
                        # replace the real one. A direct
                        # open(eventIdFile, "wb") truncates it the
                        # instant it's opened, before pickle.dump()
                        # writes anything - a signal (Ctrl+C, SIGTERM)
                        # or crash landing in that window leaves a
                        # 0-byte file that raises EOFError on the next
                        # startup. os.replace() is atomic: the real
                        # file is only ever touched by the rename
                        # itself, which either fully happens or
                        # doesn't - an interrupted write only corrupts
                        # the (discarded) temp file, never this one.
                        tmpEventIdFile = eventIdFile + ".tmp"
                        with open(tmpEventIdFile, "wb") as fh:
                            pickle.dump(
                                self._eventIdData, fh, protocol=pickle.HIGHEST_PROTOCOL
                            )
                        os.replace(tmpEventIdFile, eventIdFile)
                    except OSError as err:
                        self.log.error(
                            "Can not write event id data to %s.\n\n%s",
                            eventIdFile,
                            traceback.format_exc(),
                        )
                    break
            else:
                self.log.warning("No state was found. Not saving to disk.")

    def _checkPluginBacklogs(self):
        """
        Alert on Slack when a plugin's own queue falls critically behind
        the engine's dispatch cursor (see Plugin.getPendingCount()).

        Uses two thresholds (hysteresis) instead of one: a count that
        settles anywhere between them changes nothing, so a value
        oscillating right at a single cutoff can't spam repeated
        alerts. Only the alert-threshold crossing (not-alerting ->
        alerting) and the clear-threshold crossing (alerting -> not)
        actually send anything - being critical for many consecutive
        passes only sends the one alert from the first crossing.

        Cheap to run every pass: getPendingCount() is in-memory cursor
        arithmetic, no queue or SG access.
        """
        alertThreshold = self.config.getPluginBacklogAlertThreshold()
        clearThreshold = self.config.getPluginBacklogClearThreshold()

        for collection in self._pluginCollections:
            for plugin in collection:
                pending = plugin.getPendingCount()

                if not plugin._backlogAlertActive and pending >= alertThreshold:
                    plugin._backlogAlertActive = True
                    message = (
                        "Boxel: plugin backlog critical\n"
                        "plugin={0}\n"
                        "pending_events={1}\n"
                        "alert_threshold={2}"
                    ).format(plugin.getName(), pending, alertThreshold)
                    threading.Thread(
                        target=slack_msj.send_slack_message,
                        args=(message,),
                        daemon=True,
                    ).start()
                elif plugin._backlogAlertActive and pending <= clearThreshold:
                    plugin._backlogAlertActive = False
                    message = (
                        "Boxel: plugin backlog recovered\n"
                        "plugin={0}\n"
                        "pending_events={1}\n"
                        "clear_threshold={2}"
                    ).format(plugin.getName(), pending, clearThreshold)
                    threading.Thread(
                        target=slack_msj.send_slack_message,
                        args=(message,),
                        daemon=True,
                    ).start()

    def _checkConnectionAttempts(self, conn_attempts, msg):
        conn_attempts += 1
        if conn_attempts == self._max_conn_retries:
            self.log.error(
                "Unable to connect to SG (attempt %s of %s): %s",
                conn_attempts,
                self._max_conn_retries,
                msg,
            )
            conn_attempts = 0
            self._interruptibleSleep(self._conn_retry_sleep)
        else:
            self.log.warning(
                "Unable to connect to SG (attempt %s of %s): %s",
                conn_attempts,
                self._max_conn_retries,
                msg,
            )
        return conn_attempts

    def _interruptibleSleep(self, seconds):
        """
        Sleep for up to `seconds`, but check self._continue at least once a
        second instead of blocking for the whole duration - a stop()
        request during the connection-retry backoff (self._conn_retry_sleep,
        default 60s) should take effect within about a second, not have to
        wait out the rest of that sleep.
        """
        deadline = time.time() + seconds
        while self._continue and time.time() < deadline:
            time.sleep(min(1, deadline - time.time()))


class PluginCollection(object):
    """
    A group of plugin files in a location on the disk.
    """

    def __init__(self, engine, path):
        if not os.path.isdir(path):
            raise ValueError("Invalid path: %s" % path)

        self._engine = engine
        self.path = path
        self._plugins = {}
        self._stateData = {}

    def setState(self, state):
        if isinstance(state, int):
            for plugin in self:
                plugin.setState(state)
                self._stateData[plugin.getName()] = plugin.getState()
        else:
            self._stateData = state
            for plugin in self:
                pluginState = self._stateData.get(plugin.getName())
                if pluginState:
                    plugin.setState(pluginState)

    def getState(self):
        for plugin in self:
            self._stateData[plugin.getName()] = plugin.getState()
        return self._stateData

    def getNextUnprocessedEventId(self):
        eId = None
        for plugin in self:
            if not plugin.isActive():
                continue

            newId = plugin.getNextUnprocessedEventId()
            if newId is not None and (eId is None or newId < eId):
                eId = newId
        return eId

    def getMatchedEventTypes(self):
        """
        Union of event_type strings every active plugin in this
        collection cares about, or None if any of them matches every
        event type. See Engine._getRegisteredEventTypes().
        """
        event_types = set()
        for plugin in self:
            if not plugin.isActive():
                continue
            matched = plugin.getMatchedEventTypes()
            if matched is None:
                return None
            event_types.update(matched)
        return event_types

    def process(self, event):
        """
        Hand C{event} off to every active plugin in this collection.

        This does not wait for the plugins to actually process the event.
        Each plugin has its own persistent worker thread and its own queue
        (see L{Plugin.enqueue}); this call only pushes the event onto that
        queue and moves on. A plugin's worker thread processes its queue in
        order, at its own pace, independently of every other plugin - so a
        plugin that crashes or takes a long time on one event never blocks
        or delays any other plugin, and never blocks this call or the main
        thread. As soon as a plugin's worker thread finishes an event, and
        there's nothing else already waiting in that plugin's queue, it is
        immediately free to pick up the next event without waiting on any
        of its siblings.
        """
        for plugin in self:
            if plugin.isActive():
                plugin.enqueue(event)
            else:
                plugin.logger.debug("Skipping: inactive.")

    def shutdown(self):
        """
        Stop every plugin's worker thread, waiting for any event currently
        being processed to finish. Should be called when the engine is
        shutting down.

        Each plugin's shutdown() only touches its own queue/worker thread,
        so they're all run concurrently here instead of one at a time -
        draining them one after another would make total shutdown time the
        sum of every plugin's drain time instead of just the slowest one,
        which matters directly for supervisor.py's stop timeout.
        """
        plugins = list(self)
        if not plugins:
            return
        with ThreadPoolExecutor(
            max_workers=len(plugins), thread_name_prefix="PluginShutdown"
        ) as executor:
            list(executor.map(lambda plugin: plugin.shutdown(), plugins))

    def load(self):
        """
        Load plugins from disk.

        General behavior:
        - Loop on all paths.
        - Find all valid .py plugin files.
        - Loop on all plugin files.
        - For any new plugins, load them, otherwise, refresh them.

        Each plugin's actual load() (file read + running its own
        registerCallbacks(), which sets up its own SG connection) is
        independent of every other plugin's - none of them share
        mutable state - so they're all run concurrently here instead of
        one at a time. This is what was making every single startup and
        every reload pass (whenever a modified file is picked up) take
        roughly (plugin count * per-plugin load time) instead of about
        as long as the single slowest plugin.
        """
        newPlugins = {}

        for basename in os.listdir(self.path):
            if not basename.endswith(".py") or basename.startswith("."):
                continue

            if basename in self._plugins:
                newPlugins[basename] = self._plugins[basename]
            else:
                newPlugins[basename] = Plugin(
                    self._engine, os.path.join(self.path, basename)
                )

        if newPlugins:
            with ThreadPoolExecutor(
                max_workers=len(newPlugins), thread_name_prefix="PluginLoad"
            ) as executor:
                futures = {
                    executor.submit(plugin.load): basename
                    for basename, plugin in newPlugins.items()
                }
                for future in as_completed(futures):
                    basename = futures[future]
                    try:
                        future.result()
                    except Exception:
                        # Plugin.load() already catches and logs its own
                        # errors (deactivating itself on failure). This
                        # is only a last-resort net so one plugin's
                        # truly unexpected exception can't take down
                        # this loading pass or any other plugin's load.
                        self._engine.log.critical(
                            "Unhandled exception loading plugin %s.\n\n%s",
                            basename,
                            traceback.format_exc(),
                        )

        # Any plugin that disappeared from disk is no longer referenced and
        # won't get new events, but its worker thread is still sitting there
        # waiting on an empty queue. Shut those down explicitly so they don't
        # leak.
        for basename, plugin in self._plugins.items():
            if basename not in newPlugins:
                plugin.shutdown()

        self._plugins = newPlugins

    def __iter__(self):
        for basename in sorted(self._plugins.keys()):
            yield self._plugins[basename]


class Plugin(object):
    """
    The plugin class represents a file on disk which contains one or more
    callbacks.
    """

    def __init__(self, engine, path):
        """
        @param engine: The engine that instanciated this plugin.
        @type engine: L{Engine}
        @param path: The path of the plugin file to load.
        @type path: I{str}

        @raise ValueError: If the path to the plugin is not a valid file.
        """
        self._engine = engine
        self._path = path

        if not os.path.isfile(path):
            raise ValueError("The path to the plugin is not a valid file - %s." % path)

        self._pluginName = os.path.splitext(os.path.split(self._path)[1])[0]
        self._active = True
        self._callbacks = []
        self._mtime = None
        self._lastEventId = None
        self._backlog = {}

        # Guards _callbacks/_active/_mtime, which are written by load() (on
        # the main thread, whenever the plugin file changes on disk) and read
        # by process()/_process() (on this plugin's worker thread, for every
        # event). Without this a reload could swap _callbacks out from under
        # a callback loop that's mid-iteration.
        self._lock = threading.RLock()

        # Each plugin gets its own queue and a single persistent worker
        # thread that consumes it in order for as long as the plugin exists.
        # This is what lets every plugin proceed at its own pace: a plugin
        # stuck on a slow or crashing event only ever blocks itself, never
        # the engine's main thread nor any other plugin's queue.
        self._queue = queue.Queue()
        self._stopSentinel = object()
        self._workerThread = threading.Thread(
            target=self._workerLoop,
            name="Plugin-%s" % self._pluginName,
            daemon=True,
        )
        self._workerThread.start()

        # Highest event id handed to this plugin's queue so far (whether or
        # not the worker thread has actually gotten to it yet). This drives
        # what the engine fetches next from Shotgun - see
        # getNextUnprocessedEventId() below - and is intentionally kept
        # separate from _lastEventId, which only advances once an event has
        # actually been processed. Using _lastEventId for that purpose would
        # make the engine keep re-fetching and re-queuing events a lagging
        # plugin hasn't gotten to yet, processing them twice.
        self._lastDispatchedEventId = None

        # Whether this plugin currently has an active "critically behind"
        # Slack alert outstanding - see Engine._checkPluginBacklogs(). Only
        # read/written from the main thread, so it needs no lock of its own.
        self._backlogAlertActive = False

        # Setup the plugin's logger
        self.logger = logging.getLogger("plugin." + self.getName())
        self.logger.config = self._engine.config
        self._engine.setEmailsOnLogger(self.logger, True)
        self.logger.setLevel(self._engine.config.getLogLevel())
        if self._engine.config.getLogMode() == 1:
            _setFilePathOnLogger(
                self.logger, self._engine.config.getLogFile("plugin." + self.getName())
            )

        self._db_event_log = False
        self._last_run_invoked = False
        self._last_run_had_error = False

        # Capture this plugin's log output (and callback child loggers) for
        # the database event log. File handlers already attached above are unchanged.
        self._db_output_handler = None
        if self._engine.db_logger is not None:
            self._db_output_handler = self._engine.db_logger.attach_capture(self.logger)

    def getName(self):
        return self._pluginName

    def setState(self, state):
        with self._lock:
            if isinstance(state, int):
                newLastEventId, newBacklog = state, self._backlog
            elif isinstance(state, tuple):
                newLastEventId, newBacklog = state
            else:
                raise ValueError("Unknown state type: %s." % type(state))

            # Only apply the loaded state the first time (i.e. at startup,
            # before anything has been dispatched this run). Engine._mainLoop
            # calls Engine._loadEventIdData() again on every pass, re-applying
            # whatever was last written by _saveEventIdData() - which is a
            # snapshot from before this plugin's worker thread may have kept
            # processing its queue in the meantime (it runs independently, on
            # its own thread). Applying that stale snapshot unconditionally
            # would roll _lastEventId/_backlog backwards, resurrecting a
            # backlog id the worker thread already resolved and removed - the
            # engine would then re-fetch and re-queue it, and this plugin
            # would process it a second time. Once dispatching has started
            # this run, this plugin's own in-memory state is already the
            # source of truth and must not be overwritten by disk.
            if self._lastDispatchedEventId is None:
                self._lastEventId = newLastEventId
                self._backlog = newBacklog
                self._lastDispatchedEventId = self._lastEventId

    def getState(self):
        with self._lock:
            # Return a snapshot (not a live reference) of _backlog: this is
            # handed off to Engine._saveEventIdData(), which pickles it after
            # we've released the lock, while this plugin's worker thread may
            # still be concurrently mutating the real dict.
            return (self._lastEventId, dict(self._backlog))

    def getPendingCount(self):
        """
        Number of events dispatched to this plugin's queue but not yet
        actually processed - how far its worker thread is behind the
        engine's own dispatch cursor. Free to compute (in-memory
        arithmetic on cursors already tracked, no queue/SG access), so
        it's safe to check every pass through the main loop.
        """
        with self._lock:
            if self._lastDispatchedEventId is None or self._lastEventId is None:
                return 0
            return max(0, self._lastDispatchedEventId - self._lastEventId)

    def getMatchedEventTypes(self):
        """
        Union of event_type strings every active callback registered by
        this plugin cares about, or None if any of them (or the plugin
        having no callbacks at all, which historically meant "run on
        everything until it registers some") matches every event type.

        See Callback.getMatchedEventTypes() and
        Engine._getRegisteredEventTypes().
        """
        with self._lock:
            callbacks = list(self._callbacks)

        if not callbacks:
            return None

        event_types = set()
        for callback in callbacks:
            if not callback.isActive():
                continue
            matched = callback.getMatchedEventTypes()
            if matched is None:
                return None
            event_types.update(matched)
        return event_types

    def getNextUnprocessedEventId(self):
        """
        Next event id the engine should fetch from Shotgun for this plugin.

        This is a dispatch cursor, not a "fully processed" cursor: it
        reflects what's already been queued for this plugin, which may be
        ahead of what its worker thread has actually gotten around to. That
        distinction is what allows the worker thread to lag behind without
        the engine re-fetching and re-queuing the same events on every pass
        through the main loop.
        """
        if self._lastDispatchedEventId:
            nextId = self._lastDispatchedEventId + 1
        else:
            nextId = None

        now = datetime.datetime.now()
        for k in list(self._backlog):
            v = self._backlog[k]
            if v < now:
                self.logger.warning("Timeout elapsed on backlog event id %d.", k)
                del self._backlog[k]
            elif nextId is None or k < nextId:
                nextId = k

        return nextId

    def isActive(self):
        """
        Is the current plugin active. Should it's callbacks be run?

        @return: True if this plugin's callbacks should be run, False otherwise.
        @rtype: I{bool}
        """
        return self._active

    def deactivate(self):
        """
        Mark this plugin as inactive so its callbacks are skipped on
        subsequent events.
        """
        self._active = False

    def enqueue(self, event):
        """
        Queue C{event} up for this plugin's worker thread to process.

        Returns immediately - this never waits for the event to actually be
        processed.
        """
        self._lastDispatchedEventId = event["id"]
        self._queue.put(event)

    def shutdown(self):
        """
        Ask this plugin's worker thread to stop once its queue is drained,
        and wait for it to do so.
        """
        if self._workerThread.is_alive():
            self._queue.put(self._stopSentinel)
            self._workerThread.join()

    def _workerLoop(self):
        """
        Entry point for this plugin's dedicated worker thread.

        Pulls events off this plugin's queue one at a time, in order, for as
        long as the plugin exists. As soon as one event is done, if there's
        another already waiting in the queue, it's picked up immediately -
        this thread never waits around for any other plugin.
        """
        while True:
            item = self._queue.get()
            try:
                if item is self._stopSentinel:
                    return

                event = item
                try:
                    self.process(event)
                except Exception:
                    # Plugin.process()/_process() already isolate and log
                    # callback-level errors on their own. This is a last
                    # resort safety net so that a truly unexpected crash can
                    # never escape this thread and take down anything else.
                    self.logger.critical(
                        "Unhandled exception while processing event %d. "
                        "The plugin has been deactivated.\n\n%s",
                        event["id"],
                        traceback.format_exc(),
                    )
                    self.deactivate()
            finally:
                self._queue.task_done()

    def setEmails(self, *emails):
        """
        Set the email addresses to whom this plugin should send errors.

        @param emails: See L{LogFactory.getLogger}'s emails argument for info.
        @type emails: A I{list}/I{tuple} of email addresses or I{bool}.
        """
        self._engine.setEmailsOnLogger(self.logger, emails)

    def enableDatabaseEventLog(self, enabled=True):
        """
        Record this plugin's per-event output in the database event log.

        Errors are always stored, for every plugin. Call this from
        C{registerCallbacks} to also store successful runs and their logger
        output. Default is off.

        @param enabled: True to persist per-event output, False to keep only
            errors (and per-minute stats).
        @type enabled: I{bool}
        """
        self._db_event_log = bool(enabled)

    def load(self):
        """
        Load/Reload the plugin and all its callbacks.

        If a plugin has never been loaded it will be loaded normally. If the
        plugin has been loaded before it will be reloaded only if the file has
        been modified on disk. In this event callbacks will all be cleared and
        reloaded.

        General behavior:
        - Try to load the source of the plugin.
        - Try to find a function called registerCallbacks in the file.
        - Try to run the registration function.

        At every step along the way, if any error occurs the whole plugin will
        be deactivated and the function will return.

        This can run concurrently with this plugin's worker thread handling
        an event (see L{_workerLoop}/L{process}), so every mutation here is
        guarded by C{self._lock}, which C{_process} also holds while it reads
        C{_callbacks}/C{_active}.
        """
        # Check file mtime
        mtime = os.path.getmtime(self._path)
        if self._mtime is None:
            self._engine.log.info("Loading plugin at %s" % self._path)
        elif self._mtime < mtime:
            self._engine.log.info("Reloading plugin at %s" % self._path)
        else:
            # The mtime of file is equal or older. We don't need to do anything.
            return

        with self._lock:
            # Reset values
            self._mtime = mtime
            self._callbacks = []
            self._active = True
            self._db_event_log = False

            try:
                plugin = importlib_wrapper.load_source(self._pluginName, self._path)
            except:
                self._active = False
                self.logger.error(
                    "Could not load the plugin at %s.\n\n%s",
                    self._path,
                    traceback.format_exc(),
                )
                return

            regFunc = getattr(plugin, "registerCallbacks", None)
            if callable(regFunc):
                try:
                    regFunc(Registrar(self))
                except:
                    self._engine.log.critical(
                        "Error running register callback function from plugin at %s.\n\n%s",
                        self._path,
                        traceback.format_exc(),
                    )
                    self._active = False
            else:
                self._engine.log.critical(
                    "Did not find a registerCallbacks function in plugin at %s.", self._path
                )
                self._active = False

    def registerCallback(
        self,
        sgScriptName,
        sgScriptKey,
        callback,
        matchEvents=None,
        args=None,
        stopOnError=True,
    ):
        """
        Register a callback in the plugin.
        """
        global sg
        sgConnection = sg.Shotgun(
            self._engine.config.getShotgunURL(),
            sgScriptName,
            sgScriptKey,
            http_proxy=self._engine.config.getEngineProxyServer(),
        )
        self._callbacks.append(
            Callback(
                callback,
                self,
                self._engine,
                sgConnection,
                matchEvents,
                args,
                stopOnError,
            )
        )

    def process(self, event):
        db_logger_obj = self._engine.db_logger
        started_at = None
        self._last_run_invoked = False
        self._last_run_had_error = False
        if db_logger_obj is not None:
            started_at = datetime.datetime.now(datetime.timezone.utc).replace(
                tzinfo=None
            )
            if self._db_output_handler is not None:
                if self._db_event_log:
                    self._db_output_handler.setLevel(logging.NOTSET)
                else:
                    self._db_output_handler.setLevel(logging.ERROR)
                self._db_output_handler.begin()

        try:
            with self._lock:
                if event["id"] in self._backlog:
                    if self._process(event):
                        self.logger.info("Processed id %d from backlog." % event["id"])
                        del self._backlog[event["id"]]
                        self._updateLastEventId(event)
                elif self._lastEventId is not None and event["id"] <= self._lastEventId:
                    msg = "Event %d is too old. Last event processed was (%d)."
                    self.logger.debug(msg, event["id"], self._lastEventId)
                else:
                    if self._process(event):
                        self._updateLastEventId(event)

                return self._active
        finally:
            output = None
            if self._db_output_handler is not None:
                output = self._db_output_handler.finish()
            if db_logger_obj is not None and (
                self._last_run_invoked or self._last_run_had_error
            ):
                completed_at = datetime.datetime.now(datetime.timezone.utc).replace(
                    tzinfo=None
                )
                duration_us = int(
                    (completed_at - started_at).total_seconds() * 1_000_000
                )
                had_error = self._last_run_had_error
                db_logger_obj.log_plugin_run(
                    event["id"],
                    self.getName(),
                    started_at,
                    duration_us,
                    completed_at,
                    output,
                    had_error=had_error,
                    event_log=self._db_event_log or had_error,
                )

    def _process(self, event):
        with self._lock:
            if self._engine.isSuppressedSudoEvent(event):
                # A sudo_as_login write from a Script with
                # generate_event_log_entries disabled (see
                # Engine.isSuppressedSudoEvent) - skip every callback's
                # own work entirely, but still return self._active so
                # process() calls _updateLastEventId(event) exactly as
                # it would for any other event none of this plugin's
                # callbacks matched. That keeps this plugin's cursor/
                # backlog bookkeeping advancing normally instead of
                # treating a whole burst of these as a gap to retry.
                # self._last_run_invoked/_last_run_had_error stay at the
                # False the caller (process(), above) just reset them
                # to, so the database logger correctly records nothing
                # for a suppressed event either.
                msg = "Skipping event %d - suppressed sudo_as_login event."
                self.logger.debug(msg, event["id"])
                return self._active

            invoked = False
            had_error = False
            for callback in self:
                if callback.isActive():
                    if callback.canProcess(event):
                        invoked = True
                        msg = "Dispatching event %d to callback %s."
                        self.logger.debug(msg, event["id"], str(callback))
                        if not callback.process(event):
                            # A callback in the plugin failed. Deactivate the whole
                            # plugin.
                            had_error = had_error or callback._had_error
                            self._active = False
                            break
                        had_error = had_error or callback._had_error
                else:
                    msg = "Skipping inactive callback %s in plugin."
                    self.logger.debug(msg, str(callback))

            self._last_run_invoked = invoked
            self._last_run_had_error = had_error
            return self._active

    def _updateLastEventId(self, event):
        BACKLOG_TIMEOUT = (
            5  # minutes to keep retrying a gap before giving up on it
        )

        # Engine._getRegisteredEventTypes() now excludes event types no
        # plugin anywhere registered for directly in the SG query, so a
        # gap this size or larger is overwhelmingly one of those excluded
        # types (tens of thousands/day on a busy site) rather than the
        # handful-of-ids visibility-ordering hiccup this backlog exists
        # for - those ids will never come back no matter how long we
        # wait, since every future fetch excludes them the same way.
        # Backlogging them anyway would just grow _backlog without bound
        # for zero benefit. Only gaps at or under this size still get the
        # retry treatment below.
        MAX_BACKLOG_GAP = 50

        if self._lastEventId is not None and event["id"] > self._lastEventId + 1:
            gap_size = event["id"] - self._lastEventId - 1
            if gap_size <= MAX_BACKLOG_GAP:
                # Always give a newly-discovered gap a real chance to resolve,
                # timed from right now - not from how old `event` (created_at)
                # already is. The previous logic gave up immediately whenever
                # the event that revealed the gap was itself already more than
                # BACKLOG_TIMEOUT minutes old, which conflates two different
                # things: an id SG genuinely never produced (a rolled-back
                # transaction, say) versus this daemon simply having been
                # offline or badly backlogged before it got around to noticing
                # the gap. In the second case every event already looks old
                # by the time the daemon catches up, so real, still-pending
                # events were being discarded outright instead of retried -
                # exactly the scenario we've now measured happening for over
                # two hours at a stretch. Adding the gap to the backlog
                # unconditionally instead lets getNextUnprocessedEventId()'s
                # own expiry (also timed from here, unchanged) make that call
                # after BACKLOG_TIMEOUT real minutes of actually trying,
                # regardless of how long the daemon was down beforehand.
                expiration = datetime.datetime.now() + datetime.timedelta(
                    minutes=BACKLOG_TIMEOUT
                )
                for skippedId in range(self._lastEventId + 1, event["id"]):
                    self.logger.info("Adding event id %d to backlog.", skippedId)
                    self._backlog[skippedId] = expiration
            else:
                self.logger.debug(
                    "Skipping %d-id gap before event %d - too large to be "
                    "a visibility-ordering hiccup, almost certainly "
                    "excluded event types (see "
                    "Engine._getRegisteredEventTypes()).",
                    gap_size, event["id"],
                )
        self._lastEventId = event["id"]

    def __iter__(self):
        """
        A plugin is iterable and will iterate over all its L{Callback} objects.
        """
        return self._callbacks.__iter__()

    def __str__(self):
        """
        Provide the name of the plugin when it is cast as string.

        @return: The name of the plugin.
        @rtype: I{str}
        """
        return self.getName()


class Registrar(object):
    """
    See public API docs in docs folder.
    """

    def __init__(self, plugin):
        """
        Wrap a plugin so it can be passed to a user.
        """
        self._plugin = plugin
        self._allowed = [
            "logger",
            "setEmails",
            "registerCallback",
            "enableDatabaseEventLog",
        ]

    def getLogger(self):
        """
        Get the logger for this plugin.

        @return: The logger configured for this plugin.
        @rtype: L{logging.Logger}
        """
        # TODO: Fix this ugly protected member access
        return self.logger

    def __getattr__(self, name):
        if name in self._allowed:
            return getattr(self._plugin, name)
        raise AttributeError(
            "type object '%s' has no attribute '%s'" % (type(self).__name__, name)
        )


class Callback(object):
    """
    A part of a plugin that can be called to process a Flow Production Tracking event.
    """

    def __init__(
        self,
        callback,
        plugin,
        engine,
        shotgun,
        matchEvents=None,
        args=None,
        stopOnError=True,
    ):
        """
        @param callback: The function to run when a Flow Production Tracking event occurs.
        @type callback: A function object.
        @param engine: The engine that will dispatch to this callback.
        @type engine: L{Engine}.
        @param shotgun: The Shotgun instance that will be used to communicate
            with your Shotgun server.
        @type shotgun: L{sg.Shotgun}
        @param matchEvents: The event filter to match events against before invoking callback.
        @type matchEvents: dict
        @param args: Any datastructure you would like to be passed to your
            callback function. Defaults to None.
        @type args: Any object.

        @raise TypeError: If the callback is not a callable object.
        """
        if not callable(callback):
            raise TypeError(
                "The callback must be a callable object (function, method or callable class instance)."
            )

        self._name = None
        self._shotgun = shotgun
        self._callback = callback
        self._engine = engine
        self._logger = None
        self._matchEvents = matchEvents
        self._args = args
        self._stopOnError = stopOnError
        self._active = True
        self._had_error = False

        # Find a name for this object
        if hasattr(callback, "__name__"):
            self._name = callback.__name__
        elif hasattr(callback, "__class__") and hasattr(callback, "__call__"):
            self._name = "%s_%s" % (callback.__class__.__name__, hex(id(callback)))
        else:
            raise ValueError(
                "registerCallback should be called with a function or a callable object instance as callback argument."
            )

        # TODO: Get rid of this protected member access
        self._logger = logging.getLogger(plugin.logger.name + "." + self._name)
        self._logger.config = self._engine.config

    def canProcess(self, event):
        if not self._matchEvents:
            return True

        if "*" in self._matchEvents:
            eventType = "*"
        else:
            eventType = event["event_type"]
            if eventType not in self._matchEvents:
                return False

        attributes = self._matchEvents[eventType]

        if attributes is None or "*" in attributes:
            return True

        if event["attribute_name"] and event["attribute_name"] in attributes:
            return True

        return False

    def getMatchedEventTypes(self):
        """
        The set of event_type strings this callback matches, or None if
        it matches every event type (a falsy matchEvents, or one
        containing the "*" wildcard key - mirrors the two "match
        everything" branches at the top of canProcess()).

        Used by Engine._getRegisteredEventTypes() to narrow the
        EventLogEntry query in _getNewEvents() to only the event types
        some callback actually cares about.
        """
        if not self._matchEvents or "*" in self._matchEvents:
            return None
        return set(self._matchEvents.keys())

    def process(self, event):
        """
        Process an event with the callback object supplied on initialization.

        If an error occurs, it will be logged appropriately and the callback
        will be deactivated.

        @param event: The Flow Production Tracking event to process.
        @type event: I{dict}
        """
        # set session_uuid for UI updates
        if self._engine._use_session_uuid:
            self._shotgun.set_session_uuid(event["session_uuid"])

        self._had_error = False

        if self._engine.timing_logger:
            start_time = datetime.datetime.now(SG_TIMEZONE.local)

        try:
            self._callback(self._shotgun, self._logger, event, self._args)
            error = False
        except:
            error = True

            # Get the local variables of the frame of our plugin
            tb = sys.exc_info()[2]
            stack = []
            while tb:
                stack.append(tb.tb_frame)
                tb = tb.tb_next

            msg = "An error occured processing an event.\n\n%s\n\nLocal variables at outer most frame in plugin:\n\n%s"
            self._logger.critical(
                msg, traceback.format_exc(), pprint.pformat(stack[1].f_locals)
            )
            if self._stopOnError:
                self._active = False

        if self._engine.timing_logger:
            callback_name = self._logger.name.replace("plugin.", "")
            end_time = datetime.datetime.now(SG_TIMEZONE.local)
            duration = self._prettyTimeDeltaFormat(end_time - start_time)
            delay = self._prettyTimeDeltaFormat(start_time - event["created_at"])
            msg_format = "event_id=%d created_at=%s callback=%s start=%s end=%s duration=%s error=%s delay=%s"
            data = [
                event["id"],
                event["created_at"].isoformat(),
                callback_name,
                start_time.isoformat(),
                end_time.isoformat(),
                duration,
                str(error),
                delay,
            ]
            self._engine.timing_logger.info(msg_format, *data)

        self._had_error = error
        return self._active

    def _prettyTimeDeltaFormat(self, time_delta):
        days, remainder = divmod(time_delta.total_seconds(), 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)
        return "%02d:%02d:%02d:%02d.%06d" % (
            days,
            hours,
            minutes,
            seconds,
            time_delta.microseconds,
        )

    def isActive(self):
        """
        Check if this callback is active, i.e. if events should be passed to it
        for processing.

        @return: True if this callback should process events, False otherwise.
        @rtype: I{bool}
        """
        return self._active

    def __str__(self):
        """
        The name of the callback.

        @return: The name of the callback
        @rtype: I{str}
        """
        return self._name


class CustomSMTPHandler(logging.handlers.SMTPHandler):
    """
    A custom SMTPHandler subclass that will adapt it's subject depending on the
    error severity.
    """

    LEVEL_SUBJECTS = {
        logging.ERROR: "ERROR - SG event daemon.",
        logging.CRITICAL: "CRITICAL - SG event daemon.",
    }

    def __init__(
        self, smtpServer, fromAddr, toAddrs, emailSubject, credentials=None, secure=None
    ):
        args = [smtpServer, fromAddr, toAddrs, emailSubject, credentials]
        if credentials:
            args.append(secure)

        logging.handlers.SMTPHandler.__init__(self, *args)

    def getSubject(self, record):
        subject = logging.handlers.SMTPHandler.getSubject(self, record)
        if record.levelno in self.LEVEL_SUBJECTS:
            return subject + " " + self.LEVEL_SUBJECTS[record.levelno]
        return subject

    def emit(self, record):
        """
        Emit a record.

        Format the record and send it to the specified addressees.
        """

        # Mostly copied from Python 2.7 implementation.
        try:
            import smtplib
            from email.utils import formatdate

            port = self.mailport
            if not port:
                port = smtplib.SMTP_PORT
            smtp = smtplib.SMTP(self.mailhost, port, timeout=5)
            msg = self.format(record)
            msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\nDate: %s\r\n\r\n%s" % (
                self.fromaddr,
                ",".join(self.toaddrs),
                self.getSubject(record),
                formatdate(),
                msg,
            )
            if self.username:
                if self.secure is not None:
                    smtp.ehlo()
                    smtp.starttls(*self.secure)
                    smtp.ehlo()
                smtp.login(self.username, self.password)
            smtp.sendmail(self.fromaddr, self.toaddrs, msg)
            smtp.close()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


class EventDaemonError(Exception):
    """
    Base error for the Flow Production Tracking event system.
    """

    pass


class ConfigError(EventDaemonError):
    """
    Used when an error is detected in the config file.
    """

    pass


if sys.platform == "win32":

    class WindowsService(win32serviceutil.ServiceFramework):
        """
        Windows service wrapper
        """

        _svc_name_ = "ShotgunEventDaemon"
        _svc_display_name_ = "Flow Production Tracking Event Handler"

        def __init__(self, args):
            win32serviceutil.ServiceFramework.__init__(self, args)
            self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
            self._engine = Engine(_getConfigPath())

        def SvcStop(self):
            """
            Stop the Windows service.
            """
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
            win32event.SetEvent(self.hWaitStop)
            self._engine.stop()

        def SvcDoRun(self):
            """
            Start the Windows service.
            """
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_INFORMATION_TYPE,
                servicemanager.PYS_SERVICE_STARTED,
                (self._svc_name_, ""),
            )
            self.main()

        def main(self):
            """
            Primary Windows entry point
            """
            self._engine.start()


class LinuxDaemon(daemonizer.Daemon):
    """
    Linux Daemon wrapper or wrapper used for foreground operation on Windows
    """

    def __init__(self):
        self._engine = Engine(_getConfigPath())
        super().__init__("shotgunEvent", self._engine.config.getEnginePIDFile())

    def start(self, daemonize=True):
        if not daemonize:
            # Setup the stdout logger
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter("%(levelname)s:%(name)s:%(message)s")
            )
            logging.getLogger().addHandler(handler)

        super().start(daemonize)

    def _run(self):
        """
        Start the engine's main loop
        """
        self._engine.start()

    def _cleanup(self):
        self._engine.stop()


def main():
    """ """
    action = None
    if len(sys.argv) > 1:
        action = sys.argv[1]

    if sys.platform == "win32" and action != "foreground":
        win32serviceutil.HandleCommandLine(WindowsService)
        return 0

    if action:
        daemon = LinuxDaemon()

    if action:
        try:
            slack_msj.send_slack_message(
                "Boxel Server: Executing shotgunEventDaemon action: {}".format(action))
        except Exception as e:
            pass  # Ignore Slack errors


        # Find the function to call on the daemon and call it
        func = getattr(daemon, action, None)
        if action[:1] != "_" and func is not None:
            func()
            return 0

        print("Unknown command: %s" % action)

    print("usage: %s start|stop|restart|foreground" % sys.argv[0])
    return 2


def _getConfigPath():
    """
    Get the path of the shotgunEventDaemon configuration file.
    """
    paths = ["/etc", os.path.dirname(__file__)]

    # Get the current path of the daemon script
    scriptPath = sys.argv[0]
    if scriptPath != "" and scriptPath != "-c":
        # Make absolute path and eliminate any symlinks if any.
        scriptPath = os.path.abspath(scriptPath)
        scriptPath = os.path.realpath(scriptPath)

        # Add the script's directory to the paths we'll search for the config.
        paths[:0] = [os.path.dirname(scriptPath)]

    # Search for a config file.
    for path in paths:
        path = os.path.join(path, "shotgunEventDaemon.conf")
        if os.path.exists(path):
            return path

    # No config file was found
    raise EventDaemonError("Config path not found, searched %s" % ", ".join(paths))


if __name__ == "__main__":
    sys.exit(main())
