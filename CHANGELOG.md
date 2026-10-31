## 0.1.5

ENHANCEMENTS:

* `TEAKFLAKE_ADVERTISE_ADDR` (a bare host or IP) sets the advertised address. Without it, falls back to the hostname's canonname. (C-2042)
* Log the claimed worker id, advertised URL and every peer after registering. (C-2045)

BUG FIXES:

* Declare `logger` and `benchmark`, which zookeeper requires without declaring and Ruby 4.0 dropped from the default gems. Raise the rack floor to 2.2.24. (C-2024)

## 0.1.4

BUG FIXES:

* Use File.exist? instead of File.exists?, which was removed in Ruby 3.2

## 0.1.3

ENHANCEMENTS:

* Log on server error

## 0.1.2

BUG FIXES:

* Fixed missing require in teakflake/worker_id_sources/static_worker_id

## 0.1.1

BUG FIXES:

* Fixed missing/spurious requires in teakflake/server
* Fixed sanity checking peers for StaticWorkerId

## 0.1.0

Initial Release.
