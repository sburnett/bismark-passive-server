Bismark Passive Server-side Data Processing Documentation
=========================================================

This is a repository of scripts for processing data collected by routers running
Bismark-passive. (See https://github.com/sburnett/bismark-passive)

We've collected nearly 3 GB of compressed log files from ten Bismark routers;
the amount of data and number of routers continues to grow every week.
Therefore, it's impractical to store and process all the data in a relational
database like Postgres. (I tried this for a week and got frustrated by sluggish
performance and awkward SQL queries.)

Instead, I've written a collection of Python modules that efficiently process
the data. This system has two important properties:

1. **Incremental.** Each processor module saves state about the data it's
   already seen, so providing a computation with additional data doesn't
   require re-processing all prior data.
2. **Parallel.** We use Python's multiprocessing module to spread processing
   across cores. This significantly decreases computation time on machines
   with many cores.

Terminology
-----------

* Each Bismark router is identified by its **node id**, which is typically its
  LAN-facing MAC address prefixed by "OW". Example: OW0123456789AB
* Every bismark-passive router has a unique **anonymization context** (also
  **anonymization id**) under which it anonymizes (i.e., hashes) sensitive
  information like IP addresses, MAC addresses and domain names. When a router
  is reflashed, it receives a fresh anonymization context; otherwise, a router's
  context typically doesn't change. Data anonymized under different
  anonymization contexts cannot be compared for equality.
* A **session** is a running bismark-passive process. Each time bismark-passive
  restarts (e.g., because of a softare update, a crash, a router reboot) is
  considered the start of a new session. Sessions are identified by their
  **session id**, which is typically the Unix timestamp when the session
  started.
* An **update** is a single log file sent by a bismark-passive router. Every
  update has an associated node id, anonymization context, session id, and
  sequence number. Updates typically contain 30 seconds of passive measurement
  data; they are always gzipped.
* On disk, updates are stored in tarballs. Typically, there are 20 gzipped
  updates per tarball.
* The **updates index** is a sqlite database that enables scripts to locate
  update files inside the tarballs in constant time.

* A **session processor** runs some computation on all of a session's updates in
  order of their sequence number. Session processors write their results to a
  **session context**, which is shared among all session processors operating on
  that session. There are typically multiple session processors running on the
  same session data.
* The system writes all session contexts to disk as Python pickle files. This
  allows the system to data augment a session context with new data without
  rerunning the session processors on all the data for that session.
* A **processor coordinator** does three things:
    1. Produces session processor instances for operating on the sessions in the
       raw data.
    2. Aggregates the session contexts into a **global context**.
    3. Presents the global context to the user (e.g., by writing it to a
       database.)
* An **experiment harness** runs an ordered set of processor coordinators on the
  raw data to produce results.

Overview of Files
-----------------

* `collect_uploads.sh`

* `update_parser.py`
* `update_parser_test.py`

* `updates_index.py`
* `index_traces.py`

* `session_processor.py`
* `session_context.py`
* `process_sessions.py`
* `utils.py`

* `correlation_processor.py`
* `byte_count_processor.py`
* `domain_processor.py`
* `domains_per_flow_processor.py`
* `flow_properties_processor.py`
* `flow_statistics_processor.py`
* `ip_counts_processor.py`
* `packet_size_processor.py`
* `update_statistics_processor.py`

* `postgres_session_processor.py` and `sqlite_session_processor.py`

* `harnesses.py`

* `database_postgres.py` and `database_sqlite.py`

* `schema.sql` and `drop.sql`
* `materialized_views.sql` and `refresh_matviews.sql`
* `memoization_test.py`
* `generate_merge_functions.py`

* `anonymize_data.py`


