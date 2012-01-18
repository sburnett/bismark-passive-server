#!/usr/bin/env python

import errno
from optparse import OptionParser
from os import makedirs
from os.path import join

from process_sessions import process_sessions

from byte_count_processor import ByteCountProcessorCoordinator
from byte_statistics_processor import ByteStatisticsProcessorCoordinator
from correlation_processor import CorrelationProcessorCoordinator
from domains_per_flow_processor import DomainsPerFlowProcessorCoordinator
from flow_statistics_processor import FlowStatisticsProcessorCoordinator
from ip_counts_processor import IpCountsProcessorCoordinator
from packet_size_processor import PacketSizeProcessorCoordinator
from update_statistics_processor import UpdateStatisticsProcessorCoordinator
from domain_processor import DomainProcessorCoordinator

# Add new processing harnesses here. Keep the names in alphabetical order.
# Coordinators are called in the given order once per update file.
harnesses = {
        'byte_statistics': [CorrelationProcessorCoordinator,
                            DomainsPerFlowProcessorCoordinator,
                            ByteStatisticsProcessorCoordinator],
        'dashboard': [CorrelationProcessorCoordinator,
                      DomainsPerFlowProcessorCoordinator,
                      ByteCountProcessorCoordinator],
        'flow_statistics': [CorrelationProcessorCoordinator,
                            DomainsPerFlowProcessorCoordinator,
                            FlowStatisticsProcessorCoordinator],
        'ip_counts': [CorrelationProcessorCoordinator,
                      IpCountsProcessorCoordinator],
        'packet_size': [CorrelationProcessorCoordinator,
                        PacketSizeProcessorCoordinator],
        'updates': [UpdateStatisticsProcessorCoordinator],
        'domains_accessed': [CorrelationProcessorCoordinator,
                        DomainProcessorCoordinator],
        }

def parse_coordinator_args(parser):
    """Add arguments for your custom coordinator here. Keep arguments in
    alphabetical order. Don't use short options in this function."""
    parser.add_option('--db_filename', action='store', dest='db_filename',
                      help='Sqlite database filename')
    parser.add_option('--db_name', action='store', dest='db_name',
                      default='bismark_openwrt_live_v0_1',
                      help='Database name')
    parser.add_option('--db_user', action='store', dest='db_user',
                      default='sburnett', help='Database username')
    parser.add_option('--db_host', action='store', dest='db_host',
                      default='localhost', help='Database hostname')
    parser.add_option('--db_password', action='store', dest='db_password',
                      default='', help='Database password')
    parser.add_option('--db_port', action='store', dest='db_port',
                      default=5432, help = 'Database port')
    parser.add_option('--db_rebuild', action='store_true',
                      dest='db_rebuild', default=False,
                      help='Rebuild database from scratch (advanced)')

def parse_args():
    """Don't add coordinator-specific options to this funciton."""
    usage = 'usage: %prog [options]' \
            ' harness updates_directory index_filename pickles_directory'
    parser = OptionParser(usage=usage)
    parser.add_option('-t', '--temp-pickles-dir', action='store',
                      dest='temp_pickles_dir', default='/dev/shm',
                      help='Directory for temporary runtime pickle storage')
    parser.add_option('-w', '--workers', type='int', action='store',
                      dest='workers', default=None,
                      help='Maximum number of worker threads to use')
    parser.add_option('-p', '--ignore-pickles', action='store_true',
                      dest='ignore_pickles', default=False,
                      help='Compute from scratch (use when processors change)')
    parser.add_option('-n', '--disable-refresh', action='store_true',
                      dest='disable_refresh', default=False,
                      help='Disable refresh of index before processing')
    parse_coordinator_args(parser)
    options, args = parser.parse_args()
    if len(args) != 4:
        parser.error('Missing required option')
    mandatory = { 'harness': args[0],
                  'updates_directory': args[1],
                  'index_filename': args[2],
                  'pickles_directory': args[3] }
    return options, mandatory

def main():
    (options, args) = parse_args()
    pickles_path = join(args['pickles_directory'], args['harness'])
    try:
        makedirs(pickles_path)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise
    coordinators = map(lambda cl: cl(options), harnesses[args['harness']])
    process_sessions(coordinators,
                     args['updates_directory'],
                     args['index_filename'],
                     pickles_path,
                     options.temp_pickles_dir,
                     options.workers,
                     not options.disable_refresh,
                     options.ignore_pickles)

if __name__ == '__main__':
    main()
