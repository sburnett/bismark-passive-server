Bismark Passive Server-side Data Processing Documentation
=========================================================

This is a repository of scripts for processing data collected by routers running
Bismark-passive. (See https://github.com/sburnett/bismark-passive)

We've collected nearly 3 GB of compressed log files from ten Bismark routers,
and the amount of data and number of routers continues to grow every week.  It's
impractical to store and process all the data in a relational database like
Postgres. (I tried this for a week and got frustrated by sluggish performance
and awkward SQL queries.)

Instead, I've written a collection of Python modules that efficiently process
the data. This system has two important properties:

1. **Incremental.** Each processor module saves state about the data it's
   already seen, so providing a computation with additional data doesn't require
   re-processing all prior data.
2. **Parallel.** We use Python's multiprocessing module to spread processing
   across cores. This significantly decreases computation time on machines with
   many cores.

Terminology
-----------

* Each Bismark router has a **node id**, which is typically its LAN-facing MAC
  address prefixed by "OW". Example: OW0123456789AB
* Every bismark-passive router has a unique **anonymization context** (or
  **anonymization id**) under which it anonymizes (i.e., hashes) sensitive
  information like IP addresses, MAC addresses and domain names. When a router
  is reflashed, it receives a fresh anonymization context; otherwise, a router's
  context typically doesn't change. Data anonymized under different
  anonymization contexts cannot be compared for equality, even if those contexts
  were used on the same router.
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
  updates per tarball. Note that this is a tarball of gzips, *not* a gzipped
  tarball.
* The **updates index** is a sqlite database which enables scripts to locate
  update files inside the tarballs in constant time.

* A **session processor** is a computation to be run on all of a session's
  updates in order of their sequence number.
* Session processors write their results to a **session context**, which is
  shared among all session processors operating on that session. Because the
  context is shared among all processors for a session, processors can also read
  data from the context that was previously written by other processors. There
  are typically multiple session processors running on the same session data.
* The **session context manager** loads and stores session contexts to disk as
  Python pickle files. This allows the system to augment a session context with
  new data without rerunning the session processors on all the data for that
  session.
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

* `collect_uploads.sh` fetches raw log files uploaded by bismark-data-transmit
  (e.g., `/data/users/bismark/data/http_uploads/passive`), tars them, and puts
  the tarball in the data directory with the rest of the data (e.g.,
  `/data/users/bismark/data/passive`).

* `update_parser.py` parses an update file into a Python data structure.

* `updates_index.py` is an interface to the sqlite updates index database and
  `index_traces.py` uses that interface to build and query the index.

* `process_sessions.py` has the main processing logic. It loads updates from
  disk using the updates index, loads and stores contexts, and distributes
  session processing over multiple cores using the multiprocessing module.
* `session_processor.py` contains the base classes for session processors and
  processor coordinators.
* `session_context.py` contains the session context manager.
* `utils.py` contains various short functions and datatypes, mainly used by
  the processor coordinators.

* `correlation_processor.py` constructs dictionaries that transform between
  opaque identifiers in the update files and real Python data structures.
  Examples include (1) mapping flow ID numbers to Python objects containing
  information about each flow, (2) mapping IP addresses to address table
  identifiers, and (3) mapping address table identifiers and IP addresses to
  domain names using DNS response packets. Most experiment harnesses should
  include the CorrelationProcessorCoordinator.
* `byte_count_processor.py` computes various statistics about traffic from each
  router on a per-byte basis over time. Examples include bytes per minute, bytes
  per port per minute, and bytes per domain per minute. This stores the results
  in a postgres database.
* `flow_properties_processor.py` computes flow-level information about each
  packet, such as its external-facing port, local device MAC addresses, and
  associated DNS mappings. If you're writing a processor that's interested in
  these statistics then you probably want to inherit from this processor.
* `domains_per_flow_processor.py` is an auxiliear processor that should always be used in
  conjunction with the FlowPropertiesSessionProcessor or its subclasses.
* `flow_statistics_processor.py` counts flow-level statistics like bytes per
  flow, packets per flow and flow duration.
* `ip_counts_processor.py` counts statistics like the number of bytes and
  packets sent to and from each IP address.
* `packet_size_processor.py` computes packet size distributions, such as
  packet sizes per port.
* `domain_processor.py` 
* `update_statistics_processor.py` records information about the update files
  themselves. It can useful for debugging problems with update files.

* `postgres_session_processor.py` and `sqlite_session_processor.py` are
  abstract processor coordinators that talk to databases. If you want
  your processor coordinator to write its results to Sqlite or Postgres,
  then it should inherit from one of these classes.
* `database_postgres.py` and `database_sqlite.py` are used by the previous
  classes to talk to the databases.

* `harnesses.py` is the entry point for all processing. It runs a set of
  processors together on the raw updates.

* `schema.sql` and `drop.sql` contain Postgres code for creating and destroying
  database tables for the ByteCountProcessor.
* `materialized_views.sql` and `refresh_matviews.sql` are also used by
  ByteCountProcessor.
* `generate_merge_functions.py` autogenerates extra Postgres functions for
  ByteCountProcessor.

* `anonymize_data.py` syncs an anonymized copy of all the update files to
  another directory on the same machine.

