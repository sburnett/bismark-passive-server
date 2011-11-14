#!/usr/bin/env python

import gzip
import multiprocessing
from optparse import OptionParser
from os.path import join
import tarfile

from anonymize_data import anonymize_update
from byte_count_processor import ByteCountProcessorCoordinator
from correlation_processor import CorrelationProcessorCoordinator
from index_traces import index_traces
from packet_size_processor import PacketSizeProcessorCoordinator
from session_context import GlobalContext, SessionContextManager
from update_parser import PassiveUpdate
from update_statistics_processor import UpdateStatisticsProcessorCoordinator
from updates_index import UpdatesIndex
from utils import return_negative_one

def process_session(args):
    (session,
            session_context_manager,
            pickle_root,
            processors,
            update_files,
            updates_directory) = args
    proc_id = multiprocessing.current_process().pid
    print 'Start %d: %s-%s-%d' % (proc_id,
                                  session.node_id,
                                  session.anonymization_context,
                                  session.id)
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
        update_content = gzip.GzipFile(fileobj=tarhandle).read()
        update = PassiveUpdate(update_content)
        if update.sequence_number == context.last_sequence_number_processed + 1:
            for processor in processors:
                processor.process_update(context, update)
            context.last_sequence_number_processed = update.sequence_number
            context.filenames_processed.add(filename)
        else:
            print '%s:%s filename skipped: bad sequence number' \
                    % (tarname, filename)
    if processed_new_update:
        session_context_manager.save_context(context, pickle_path)
    results = {}
    for processor in processors:
        processor.finished_session()
    print 'Stop %d: %s-%s-%d' % (proc_id,
                                 session.node_id,
                                 session.anonymization_context,
                                 session.id)
    return context

def process_sessions(coordinators,
                     updates_directory,
                     index_filename,
                     pickle_root,
                     num_workers):
    session_context_manager = SessionContextManager()
    session_context_manager.declare_state('filenames_processed', set, None)
    session_context_manager.declare_state(
            'last_sequence_number_processed', return_negative_one, None)
    for coordinator in coordinators:
        for name, (init_func, merge_func) in coordinator.states.iteritems():
            session_context_manager.declare_state(name, init_func, merge_func)

    pool = multiprocessing.Pool(processes=num_workers)
    results = []
    index = UpdatesIndex(index_filename)
    print 'Dispatching jobs'
    args = []
    for session in index.sessions:
        processors = []
        for coordinator in coordinators:
            processor = coordinator.create_processor(session)
            processors.append(processor)
        update_files = index.session_data(session)
        args.append((session,
                     session_context_manager,
                     pickle_root,
                     processors,
                     update_files,
                     updates_directory))
    
    print 'Waiting for results'
    global_context = GlobalContext()
    for session_context in pool.imap_unordered(process_session, args):
        print 'Got result'
        session_context_manager.merge_contexts(session_context, global_context)
        print 'Done processing result'
    print 'Finishing'
    for coordinator in coordinators:
        coordinator.finished_processing(global_context)

def parse_args():
    usage = 'usage: %prog [options]' \
            ' updates_directory index_filename pickle_directory'
    parser = OptionParser(usage=usage)
    parser.add_option('-u', '--username', action='store', dest='db_user',
                      default='sburnett', help='Database username')
    parser.add_option('-d', '--database', action='store', dest='db_name',
                      default='bismark_openwrt_live_v0_1',
                      help='Database name')
    parser.add_option('-w', '--workers', type='int', action='store',
                      dest='workers', default=8,
                      help='Maximum number of worker threads to use')
    parser.add_option('-n', '--disable-refresh', action='store_true',
                      dest='disable_refresh', default=False,
                      help='Disable refresh of index before processing')
    parser.add_option('-a', '--anonymize-traces-dir', action='store',
                      dest='anonymize_traces_dir', default=None,
                      help='Anonymize newly-indexed traces to into directory')
    parser.add_option('-r', '--rebuild', action='store_true',
                      dest='rebuild', default=False,
                      help='Rebuild database from scratch (advanced)')
    options, args = parser.parse_args()
    if len(args) != 3:
        parser.error('Missing required option')
    mandatory = { 'updates_directory': args[0],
                  'index_filename': args[1],
                  'pickle_directory': args[2] }
    return options, mandatory

def main():
    (options, args) = parse_args()
    coordinators = [
            CorrelationProcessorCoordinator(),
            ByteCountProcessorCoordinator(options.db_user,
                                          options.db_name,
                                          options.rebuild),
            UpdateStatisticsProcessorCoordinator(options.db_user,
                                                 options.db_name,
                                                 options.rebuild),
            PacketSizeProcessorCoordinator(options.db_user,
                                           options.db_name)
            ]
    if not options.disable_refresh:
        print 'Indexing new updates'
        new_traces = index_traces(args['updates_directory'],
                                  args['index_filename'])
        if options.anonymize_traces_dir is not None:
            print 'Anonymizing traces into', options.anonymize_traces_dir
            for new_trace in new_traces:
                anonymize_update(new_trace, options.anonymize_traces_dir)
    print 'Processing sessions'
    process_sessions(coordinators,
                     args['updates_directory'],
                     args['index_filename'],
                     args['pickle_directory'],
                     options.workers)

if __name__ == '__main__':
    main()
