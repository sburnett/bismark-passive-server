from gzip import GzipFile
from multiprocessing import Pool
from os import getpid, makedirs
from os.path import join
from shutil import rmtree
import tarfile

from index_traces import index_traces
from session_context import GlobalContext, SessionContextManager
from update_parser import PassiveUpdate
from updates_index import UpdatesIndex
from utils import return_negative_one

def process_session(session,
                    session_context_manager,
                    pickle_root,
                    result_pickle_root,
                    processors,
                    update_files,
                    updates_directory,
                    multiprocessed,
                    ignore_pickles):
    """
        Args:

        session: namedtuple Session
            - node_id
            - anonymization_context
            - session_id 
        session_context_manager: SessionContextManager
            provides the functions for creating context, loading context and
            finally mergin contexts
        pickle_root: String
            pickle directory given as a commandline argument
        result_pickle_root: String
            directory for storing the intermediate result pickle files. Defaults
            to /dev/shm but can be modified with a commandline parameter
        processors: list
            list of children of SessionProcessor class, provide the
            process_update function as needed by the processor
        update_files: list
            [(tarname, filename), ..] list of the tuples which belong to this
            node and session
        updates_directory: String
            the directory of updates that has all the tar files, is given as a
            command line argument
    """
    pickle_filename = '%s_%s_%s.pickle' \
            % (session.node_id, session.anonymization_context, str(session.id))
    pickle_path = join(pickle_root, pickle_filename)
    if not ignore_pickles:
        context = session_context_manager.load_context(pickle_path)
    if ignore_pickles or context is None:
        context = session_context_manager.create_context(
                session.node_id, session.anonymization_context, session.id)
        session_context_manager.save_persistent_context(context, pickle_path)
    processed_new_update = False
    current_tarname = None
    while (context.last_sequence_number_processed + 1) in update_files:
        tarname, filename = \
                update_files[context.last_sequence_number_processed + 1]
        processed_new_update = True
        if current_tarname != tarname:
            current_tarname = tarname
            full_tarname = join(updates_directory, current_tarname)
            tarball = tarfile.open(full_tarname, 'r')
        tarhandle = tarball.extractfile(filename)
        update_content = GzipFile(fileobj=tarhandle).read()
        try:
            update = PassiveUpdate(update_content)
        except:
            print '%s:%s filename skipped: parser error' % (tarname, filename)
            break
        if update.sequence_number != context.last_sequence_number_processed + 1:
            print '%s:%s has invalid sequence number' % (tarname, filename)
            break
        for processor in processors:
            processor.process_update(context, update)
        context.last_sequence_number_processed = update.sequence_number
    del update_files
    if processed_new_update:
        session_context_manager.save_persistent_context(context, pickle_path)
        if not multiprocessed:
            return context
        results_pickle_path = join(result_pickle_root, pickle_filename)
        session_context_manager.save_all_context(context, results_pickle_path)
    else:
        if not multiprocessed:
            return context
        results_pickle_path = pickle_path
    return results_pickle_path

def process_session_wrapper(args):
    result = process_session(*args)
    del args
    return result

def process_sessions_real(coordinators,
                          updates_directory,
                          index_filename,
                          pickle_root,
                          result_pickle_root,
                          num_workers,
                          ignore_pickles):
    """ The main function for processing the updates """
    if num_workers != 0:
        pool = Pool(processes=num_workers)

    session_context_manager = SessionContextManager()
    session_context_manager.declare_persistent_state(
            'last_sequence_number_processed', return_negative_one, None)
    for coordinator in coordinators:
        for name, (init_func, merge_func) \
                in coordinator.persistent_state.iteritems():
            session_context_manager.declare_persistent_state(
                    name, init_func, merge_func)
        for name, (init_func, merge_func) \
                in coordinator.ephemeral_state.iteritems():
            session_context_manager.declare_ephemeral_state(
                    name, init_func, merge_func)

    print 'Preparing processors'
    process_args = []
    index = UpdatesIndex(index_filename)
    for session in index.sessions:
        processors = []
        for coordinator in coordinators:
            processors.append(coordinator.create_processor(session))
        update_files = index.session_data(session)
        process_args.append((session,
                             session_context_manager,
                             pickle_root,
                             result_pickle_root,
                             processors,
                             update_files,
                             updates_directory,
                             num_workers != 0,
                             ignore_pickles))

    print 'Processing sessions'
    global_context = GlobalContext()
    if num_workers == 0:
        for args in process_args:
            session_context = process_session_wrapper(args)
            session_context_manager.merge_contexts(session_context, global_context)
            del session_context
    else:
        results = pool.imap_unordered(process_session_wrapper, process_args)
        for pickle_path in results:
            session_context = session_context_manager.load_context(pickle_path)
            session_context_manager.merge_contexts(session_context, global_context)
            del session_context
        pool.close()
        pool.join()

    print 'Post-processing'
    for coordinator in coordinators:
        coordinator.finished_processing(global_context)

def process_sessions(coordinators,
                     updates_directory,
                     index_filename,
                     pickle_root,
                     temp_pickles_dir='/dev/shm',
                     num_workers=None,
                     refresh_index=True,
                     ignore_pickles=False):
    """
        Args:
        coordinators: list
            list of the coordinator objects which were provided in the main
            function of process_session_updates
        updates_directory: String
            the directory of updates that has all the tar files, is given as a
            command line argument
        index_filename: String
            the file which is the basis of index sqlite database. This is given
            as a command line argument
        pickle_root: String
            pickle directory given as a commandline argument
        temp_pickles_dir:
            directory for storing the intermediate result pickle files. Defaults
            to /dev/shm but can be modified with a commandline parameter
        num_workers: Integer
            the number of threads that the process can use when multiprocessing
            the updates. Can be provided as a commandline argument
        refresh_index: boolean
            when this is set to True then sqlite database which contains the
            indexing of tar file names to update file names, is refreshed.
            Recommended setting is True.
        ignore_pickles: boolean
            when this is True, then ignore all existing pickle files and recompute
            everything from scratch. You need to do this whenever you change
            a processor. Equivalently, you can manually delete the pickles.
    """
    if refresh_index:
        print 'Indexing new updates'
        index_traces(updates_directory, index_filename)
    result_pickle_root = join(temp_pickles_dir, str(getpid()))
    makedirs(result_pickle_root)
    try:
        process_sessions_real(coordinators,
                              updates_directory,
                              index_filename,
                              pickle_root,
                              result_pickle_root,
                              num_workers,
                              ignore_pickles)
    finally:
        rmtree(result_pickle_root)
