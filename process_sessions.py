#!/usr/bin/env python

import cPickle
import errno
import multiprocessing
from optparse import OptionParser
from os import makedirs
from os.path import join

from bytes_computation import BytesSessionProcessor, BytesSessionAggregator
from index_traces import index_traces
from update_statistics_computation import \
        UpdateStatisticsSessionProcessor, UpdateStatisticsSessionAggregator
from updates_index import UpdatesIndex

def process_session(pickle_subdir, processor_class, update_files, updates_directory, session):
    pickle_filename = '%s.pickle' % processor_class.__name__
    pickle_path = join(pickle_subdir, pickle_filename)
    try:
        processor = cPickle.load(open(pickle_path, 'rb'))
    except:
        processor = processor_class()
    processed_updates, process_result \
            = processor.process_session(update_files, updates_directory)
    if processed_updates:
        cPickle.dump(processor, open(pickle_path, 'wb'), 2)
    return processed_updates, process_result, processor.results

def process_sessions(computations,
                     updates_directory,
                     index_filename,
                     pickle_root_directory,
                     workers):
    pool = multiprocessing.Pool(processes=workers)
    results = []
    index = UpdatesIndex(index_filename)
    for session in index.sessions:
        update_files = index.session_data(session)
        pickle_subdir = join(pickle_root_directory, session.node_id, str(session.id))
        try:
            makedirs(pickle_subdir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        for processor_class, aggregator in computations:
            args = (pickle_subdir, processor_class, update_files, updates_directory, session)
            result = pool.apply_async(process_session, args)
            results.append((result, aggregator, session))

    for result, aggregator, session in results:
        processed_updates, process_result, processor_results = result.get()
        aggregator.augment_results(session.node_id,
                                   session.anonymization_context,
                                   session.id,
                                   processor_results,
                                   processed_updates,
                                   process_result)

    for _, aggregator in computations:
        aggregator.store_results()

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
    computations = [
            (BytesSessionProcessor,
                BytesSessionAggregator(options.db_user,
                                       options.db_name,
                                       options.rebuild)),
            (UpdateStatisticsSessionProcessor,
                UpdateStatisticsSessionAggregator(options.db_user,
                                                  options.db_name,
                                                  options.rebuild))
            ]
    if not options.disable_refresh:
        print 'Indexing new updates'
        index_traces(args['updates_directory'], args['index_filename'])
    print 'Processing sessions'
    process_sessions(computations,
                     args['updates_directory'],
                     args['index_filename'],
                     args['pickle_directory'],
                     options.workers)

if __name__ == '__main__':
    main()
