Bismark Passive Server-side Data Processing Documentation
=========================================================

This repository contains infrastructure for processing data collected by
routers running [bismark-passive](https://github.com/sburnett/bismark-passive).

We've collected nearly 7 GB of compressed log files from 14 Bismark routers, and
the amount of data and number of routers continues to grow every week. I've
written a collection of Python modules that efficiently process the data. This
system has two important properties:

1. **Incremental.** Each processor module saves state about the data it's
   already seen, so providing a computation with additional data doesn't require
   re-processing all prior data.
2. **Parallel.** We use Python's multiprocessing module, which significantly
   decreases computation time on machines with many cores.

Installation
------------

Install the code before using it:

    python build
    python install --user

Whenever you `git pull` you need to reinstall using the same procedure.

Verify successful installation:

    (cd /tmp && python -m bismarkpassive.harness)

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
  **session id**, which is the Unix timestamp when the session started.
* An **update** is a single log file sent by a bismark-passive router. Every
  update has an associated node id, anonymization context, session id, and
  sequence number. Updates typically contain 30 seconds of passive measurement
  data; they are always gzipped.
* On disk, updates are stored in tarballs. Typically, there are 20 gzipped
  updates per tarball. Note that this is a tarball of gzips, *not* a gzipped
  tarball.
* The **updates index** is a giant sqlite database which contains efficient
  representations of all the update files. You interact with the data via the
  updates index and should never read the data files directly.

* A **session processor** is a computation run on all of a session's updates in
  order of their sequence number. For example, a simple session processor could
  sum the sizes of all the packets in a session.
* Session processors write their results to a **session context**, which is
  shared among all session processors operating on that session. Because the
  context is shared among all processors for a session, processors can also read
  data from the context that was previously written by other processors. (You
  typically run multiple session processors on the same session data.)
* After the session processors process all updates for all sessions, they merge
  the session contexts into one **global context**, which is then passed to
  another class for storage, plotting, etc. For example, if a session processor
  computes the sizes of all packets in each session, then those results could be
  merged into the global context to compute the sizes of all packets across all
  sessions.
* A **harness** runs a set of session processors then does something with their
  resulting global context.

Example
-------

See `example.py` for a small yet complete session processor and harness.

Overview of Files
-----------------

* `__init__.py`` contains the public symbols exported by the package. These are
  typically all you'll need to write your own processors and harnesses.
* `harness.py` contains the base class for the experiment harness.
* `node_plot.py` is an abstract base class for producing per-node graphs with matplotlib.
* `update_statistics_plot.py` plots data availability for each router.

* `session_processor.py` contains the abstract base classes for session processors.
* `process_sessions.py` has the main processing logic. It loads updates from
  the updates index, loads and stores contexts, and distributes session
  processing over multiple cores using the multiprocessing module.

* `processors/correlation_processor.py` constructs dictionaries that transform
  between opaque identifiers in the update files and real Python data
  structures.  Examples include (1) mapping flow ID numbers to Python objects
  containing information about each flow, (2) mapping IP addresses to address
  table identifiers, and (3) mapping address table identifiers and IP addresses
  to domain names using DNS response packets. Most experiment harnesses will
  probably include the processors from this file.
* `processors/meta_statistics_processor.py` keeps track of what's been processed
  during *this processing run*, as opposed to previous processing runs loaded
  from disk. It's useful for figuring out what's been updated since the last
  time processing ran.
* `processors/update_statistics_processor.py` records information about the
  update files themselves. It can useful for debugging problems with update
  files.

* `update_parser.py` parses an update file into a Python data structure.
* `updates_index.py` is an interface to the sqlite updates index database and
  `index_traces.py` uses that interface to build the index.
* `anonymize_data.py` syncs an anonymized copy of all the update files to
  another directory on the same machine.
* `collect_uploads.sh` fetches raw log files uploaded by bismark-data-transmit
  (e.g., `/data/users/bismark/data/http_uploads/passive`), tars them, and puts
  the tarball in the data directory with the rest of the data (e.g.,
  `/data/users/bismark/data/passive`).
