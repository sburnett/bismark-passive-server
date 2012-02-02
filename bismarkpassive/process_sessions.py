import atexit
from itertools import imap
from multiprocessing import Pool
from os import getpid, makedirs
from os.path import join
try:
    import cPickle as pickle
except ImportError:
    import pickle
try:
    import progressbar
    if progressbar.__version__ < '2.3':
        print "Upgrade to version 2.3 of the 'progressbar' package if " \
                "you're curious how long this will take"
        progressbar = None
except ImportError:
    print "Install the 'progressbar' package if " \
            "you're curious how long this will take"
    progressbar = None
from shutil import rmtree
import sys
import traceback

from updates_index import UpdatesReader

class SessionContext(object):
    def __init__(self, session):
        self._node_id = session.node_id
        self._anonymization_context = session.anonymization_context
        self._session_id = session.id

    @property
    def node_id(self):
        return self._node_id
    @property
    def anonymization_context(self):
        return self._anonymization_id
    @property
    def session_id(self):
        return self._session_id

class PersistentContext(SessionContext):
    def __init__(self, session):
        super(PersistentContext, self).__init__(session)
        self.last_sequence_number_processed = -1

class EphemeralContext(SessionContext):
    def __init__(self, session):
        super(EphemeralContext, self).__init__(session)

class GlobalContext(object):
    pass

def process_session((session,
                     index_filename,
                     disk_pickle_root,
                     ram_pickle_root,
                     processors,
                     multiprocessed,
                     ignore_pickles)):
    """
        Args:

        session: namedtuple Session
            - node_id
            - anonymization_context
            - session_id 
        index_filename: string
            the filename of the updates index, which is where all
            updates information comes from.
        disk_pickle_root: String
            pickle directory given as a commandline argument
        ram_pickle_root: String
            directory for storing the intermediate result pickle files. Defaults
            to /dev/shm but can be modified with a commandline parameter
        processors: list
            list of children of SessionProcessor class, provide the
            process_update function as needed by the processor
        multiprocessed: boolean
            whether we're running under multiprocessing. If not,
            then we don't need to pickle our results; instead we
            can just result them.
        ignore_pickles: boolean
            If true, then disregard existing pickle files and
            generate them from scratch.
    """

    pickle_filename = '%s_%s_%s.pickle' \
            % (session.node_id, session.anonymization_context, str(session.id))
    disk_pickle_path = join(disk_pickle_root, pickle_filename)
    if not ignore_pickles:
        try:
            persistent_context = pickle.load(open(disk_pickle_path, 'rb'))
        except:
            persistent_context = None
    if ignore_pickles or persistent_context is None:
        persistent_context = PersistentContext(session)
        for processor in processors:
            processor.initialize_persistent_context(persistent_context)
        pickle.dump(persistent_context,
                    open(disk_pickle_path, 'wb'),
                    pickle.HIGHEST_PROTOCOL)
    ephemeral_context = EphemeralContext(session)
    for processor in processors:
        processor.initialize_ephemeral_context(ephemeral_context)

    processed_new_update = False
    current_tarname = None
    index = UpdatesReader(index_filename)
    session_data = index.session_data(
            session, persistent_context.last_sequence_number_processed + 1)
    for sequence_number, update in session_data:
        last_processed = persistent_context.last_sequence_number_processed
        if sequence_number > last_processed + 1:
            print 'Invalid sequence number: %d' % sequence_number
            break
        elif sequence_number == last_processed:
            continue
        for processor in processors:
            processor.process_update(
                    persistent_context, ephemeral_context, update)
        persistent_context.last_sequence_number_processed = \
                update.sequence_number
        processed_new_update = True
    for processor in processors:
        processor.complete_session(persistent_context, ephemeral_context)
    if processed_new_update:
        pickle.dump(persistent_context,
                    open(disk_pickle_path, 'wb'),
                    pickle.HIGHEST_PROTOCOL)
    if multiprocessed:
        ram_pickle_path = join(ram_pickle_root, pickle_filename)
        if processed_new_update:
            pickle.dump((persistent_context, ephemeral_context),
                        open(ram_pickle_path, 'wb'),
                        pickle.HIGHEST_PROTOCOL)
            return (ram_pickle_path, ram_pickle_path)
        else:
            pickle.dump(ephemeral_context,
                        open(ram_pickle_path, 'wb'),
                        pickle.HIGHEST_PROTOCOL)
            return (disk_pickle_path, ram_pickle_path)
    else:
        return (persistent_context, ephemeral_context)

def process_sessions(harness,
                     index_filename,
                     disk_pickle_root,
                     ram_pickles_dir='/dev/shm',
                     num_workers=None,
                     ignore_pickles=False,
                     cached_global_context=None):
    """
        Args:
        harness: Harness (or subclass)
        index_filename: String
            the file which is the basis of index sqlite database. This is given
            as a command line argument
        disk_pickle_root: String
            pickle directory given as a commandline argument
        ram_pickles_dir:
            directory for storing the intermediate result pickle files. Defaults
            to /dev/shm but can be modified with a commandline parameter
        num_workers: Integer
            the number of threads that the process can use when multiprocessing
            the updates. Can be provided as a commandline argument
        ignore_pickles: boolean
            when this is True, then ignore all existing pickle files and recompute
            everything from scratch. You need to do this whenever you change
            a processor. Equivalently, you can manually delete the pickles.
    """

    ram_pickle_root = join(ram_pickles_dir, str(getpid()))
    makedirs(ram_pickle_root)
    atexit.register(rmtree, ram_pickle_root)

    if cached_global_context is not None:
        try:
            global_context = pickle.load(open(cached_global_context, 'r'))
        except:
            print 'Failed to load cached global context:'
            traceback.print_exc(file=sys.stdout)
            global_context = None
        if global_context is not None:
            print 'Post-processing from cached global context'
            harness.process_results(global_context)
            return

    if num_workers != 0:
        pool = Pool(processes=num_workers)

    process_args = []
    index = UpdatesReader(index_filename)
    processors = harness.instantiate_processors()
    for session in index.sessions:
        if harness.exclude_nodes and session.node_id in harness.exclude_nodes:
            continue
        if harness.include_nodes \
                and session.node_id not in harness.include_nodes:
            continue
        process_args.append((session,
                             index_filename,
                             disk_pickle_root,
                             ram_pickle_root,
                             processors,
                             num_workers != 0,
                             ignore_pickles))
    number_of_sessions = len(process_args)

    if progressbar is not None:
        progress = progressbar.ProgressBar(
                maxval=number_of_sessions,
                widgets=[progressbar.SimpleProgress(),
                         progressbar.Bar(),
                         progressbar.Timer()])
    else:
        progress = lambda x: x
    global_context = GlobalContext()
    for processor in processors:
        processor.initialize_global_context(global_context)
    if num_workers == 0:
        results = imap(process_session, process_args)
        for persistent_context, ephemeral_context in progress(results):
            for processor in processors:
                processor.merge_contexts(
                        persistent_context, ephemeral_context, global_context)
    else:
        results = pool.imap_unordered(process_session, process_args)
        for persistent_pickle_path, ephemeral_pickle_path in progress(results):
            if persistent_pickle_path == ephemeral_pickle_path:
                persistent_context, ephemeral_context = \
                        pickle.load(open(persistent_pickle_path, 'rb'))
            else:
                persistent_context = pickle.load(
                        open(persistent_pickle_path, 'rb'))
                ephemeral_context = pickle.load(
                        open(ephemeral_pickle_path, 'rb'))
            for processor in processors:
                processor.merge_contexts(
                        persistent_context, ephemeral_context, global_context)
        pool.close()
        pool.join()
    processor.complete_global_context(global_context)
    if cached_global_context is not None:
        try:
            pickle.dump(global_context,
                        open(cached_global_context, 'wb'),
                        pickle.HIGHEST_PROTOCOL)
            print 'Cached global context'
        except:
            print 'Failed to save cached global context'
            traceback.print_exc(file=sys.stdout)

    print 'Post-processing'
    harness.process_results(global_context)
