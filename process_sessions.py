#!/usr/bin/env python

from optparse import OptionParser

from bytes_computation import BytesSessionProcessor, BytesSessionAggregator
from session_computations import process_sessions
from update_statistics_computation import \
        UpdateStatisticsSessionProcessor, UpdateStatisticsSessionAggregator

def parse_args():
    usage = 'usage: %prog [options] index_directory pickle_directory'
    parser = OptionParser(usage=usage)
    parser.add_option('-u', '--username', action='store', dest='db_user',
                      default='sburnett', help='Database username')
    parser.add_option('-d', '--database', action='store', dest='db_name',
                      default='bismark_openwrt_live_v0_1',
                      help='Database name')
    options, args = parser.parse_args()
    if len(args) != 2:
        parser.error('Missing required option')
    mandatory = { 'index_directory': args[0],
                  'pickle_directory': args[1] }
    return options, mandatory

def main():
    (options, args) = parse_args()
    computations = [
            (BytesSessionProcessor,
                BytesSessionAggregator(options.db_user, options.db_name)),
            (UpdateStatisticsSessionProcessor,
                UpdateStatisticsSessionAggregator(options.db_user,
                                                  options.db_name))
            ]

    process_sessions(computations,
                     args['index_directory'],
                     args['pickle_directory'])

if __name__ == '__main__':
    main()
