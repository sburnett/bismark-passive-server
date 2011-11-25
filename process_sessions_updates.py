from optparse import OptionParser

from update_statistics_processor import UpdateStatisticsProcessorCoordinator
from process_sessions import process_sessions

def parse_args():
    usage = 'usage: %prog [options]' \
            ' updates_directory index_filename pickle_directory'
    parser = OptionParser(usage=usage)
    parser.add_option('-u', '--username', action='store', dest='db_user',
                      default='sburnett', help='Database username')
    parser.add_option('-d', '--database', action='store', dest='db_name',
                      default='bismark_openwrt_live_v0_1',
                      help='Database name')
    parser.add_option('-t', '--temp-pickles-dir', action='store',
                      dest='temp_pickles_dir', default='/dev/shm',
                      help='Directory for temporary runtime pickle storage')
    parser.add_option('-w', '--workers', type='int', action='store',
                      dest='workers', default=None,
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
    coordinators = [
            UpdateStatisticsProcessorCoordinator(options.db_user,
                                                 options.db_name,
                                                 options.rebuild),
            ]
    process_sessions(coordinators,
                     args['updates_directory'],
                     args['index_filename'],
                     args['pickle_directory'],
                     options.temp_pickles_dir,
                     options.workers,
                     not options.disable_refresh)

if __name__ == '__main__':
    main()
