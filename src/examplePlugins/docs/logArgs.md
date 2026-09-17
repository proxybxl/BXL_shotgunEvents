# logArgs.py

This is an EXAMPLE PLUGIN.

Use this plugin to determine if the Flow Production Tracking Event Daemon has been successfully
installed. It will log all events in Shotgun by spitting out the event
dictionary.

## Args

No settings.

To also store this plugin's per-event logger output in MariaDB (not only
errors and per-minute stats), uncomment in `registerCallbacks`:

```
reg.enableDatabaseEventLog()
```
