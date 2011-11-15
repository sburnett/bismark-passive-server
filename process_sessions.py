#!/usr/bin/env python

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
                    updates_directory):
    pickle_filename = '%s_%s_%s.pickle' \
            % (session.node_id, session.anonymization_context, str(session.id))
    pickle_path = join(pickle_root, pickle_filename)
    context = session_context_manager.load_context(pickle_path)
    if context is None:
        context = session_context_manager.create_context(
                session.node_id, session.anonymization_context, session.id)
    processed_new_update = False
    current_tarname = None
    for tarname, filename in update_files:
        if filename in context.filenames_processed:
            continue
        else:
            processed_new_update = True
        if current_tarname != tarname:
            current_tarname = tarname
            full_tarname = join(updates_directory, current_tarname)
            tarball = tarfile.open(full_tarname, 'r')
        tarhandle = tarball.extractfile(filename)
        update_content = GzipFile(fileobj=tarhandle).read()
        update = PassiveUpdate(update_content)
        if update.sequence_number == context.last_sequence_number_processed + 1:
            for processor in processors:
                processor.process_update(context, update)
            context.last_sequence_number_processed = update.sequence_number
            context.filenames_processed.add(filename)
        else:
            print '%s:%s filename skipped: bad sequence number' \
                    % (tarname, filename)
    del update_files
    if processed_new_update:
        session_context_manager.save_persistent_context(context, pickle_path)
        results_pickle_path = join(result_pickle_root, pickle_filename)
        session_context_manager.save_all_context(context, results_pickle_path)
    else:
        results_pickle_path = pickle_path
    return results_pickle_path

def process_session_wrapper(args):
    results_pickle_path = process_session(*args)
    del args
    return results_pickle_path

def process_sessions_real(coordinators,
                          updates_directory,
                          index_filename,
                          pickle_root,
                          result_pickle_root,
                          num_workers=None):
    session_context_manager = SessionContextManager()
    session_context_manager.declare_persistent_state(
            'filenames_processed', set, None)
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
                             updates_directory))

    print 'Processing sessions'
    global_context = GlobalContext()
    pool = Pool(processes=num_workers)
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
                     refresh_index=True):
    if refresh_index:
        print 'Indexing new updates'
        index_traces(updates_directory, index_filename)
    result_pickle_root = join(temp_pickles_dir, str(getpid()))
    makedirs(result_pickle_root)
    process_sessions_real(coordinators,
                          updates_directory,
                          index_filename,
                          pickle_root,
                          result_pickle_root,
                          num_workers)
    rmtree(result_pickle_root)
