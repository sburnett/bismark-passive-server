from abc import abstractmethod, ABCMeta
from errno import EEXIST
from optparse import OptionParser
from os import makedirs
from os.path import join

from process_sessions import process_sessions

class Harness(object):
    __metaclass__ = ABCMeta

    processors = []

    # Exclude this set of nodes from processing.
    # Exclusion takes priority over inclusion.
    exclude_nodes = set([])
    # Include only this set of nodes while processing
    # Exclusion takes priority over inclusion.
    include_nodes = set([])

    def __init__(self):
        pass

    @staticmethod
    def setup_options(parser):
        """Add arguments for your custom coordinator here. Keep arguments in
        alphabetical order. Don't use short options in this function.
        You can also add options in a subclass, but make sure to
        call this parent version first."""
        parser.add_option('--db_filename', action='store', dest='db_filename',
                          help='Sqlite database filename')
        parser.add_option('--db_host', action='store', dest='db_host',
                          default='localhost', help='Database hostname')
        parser.add_option('--db_name', action='store', dest='db_name',
                          default='bismark_openwrt_live_v0_1',
                          help='Database name')
        parser.add_option('--db_password', action='store', dest='db_password',
                          default='', help='Database password')
        parser.add_option('--db_port', action='store', dest='db_port',
                          default=5432, help = 'Database port')
        parser.add_option('--db_rebuild', action='store_true',
                          dest='db_rebuild', default=False,
                          help='Rebuild database from scratch (advanced)')
        parser.add_option('--db_user', action='store', dest='db_user',
                          default='sburnett', help='Database username')
        parser.add_option('--plots_directory', action='store',
                          dest='plots_directory', default='/tmp',
                          help='Store plots in this directory')

    def instantiate_processors(self):
        """Override this method to pass custom arguments to processors."""
        return map(lambda P: P(), self.processors)

    @abstractmethod
    def process_results(self, global_context):
        pass

def parse_args(HarnessClass):
    usage = 'usage: %prog [options] index_filename pickles_directory'
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
    parser.add_option('-g', '--cached-global-context', action='store',
                      dest='cached_global_context', default=None,
                      help='Attempt to cache global context to the given filename')
    parser.add_option('-r', '--run-name', action='store',
                      dest='run_name', default='default',
                      help='Assign a name to this processing run')
    HarnessClass.setup_options(parser)
    options, args = parser.parse_args()
    if len(args) != 2:
        parser.error('Missing required option')
    mandatory = { 'index_filename': args[0],
                  'pickles_directory': args[1] }
    return options, mandatory

def main(HarnessClass):
    (opts, args) = parse_args(HarnessClass)
    global options
    options = opts
    pickles_path = join(args['pickles_directory'],
                        options.run_name,
                        HarnessClass.__name__)
    try:
        makedirs(pickles_path)
    except OSError, e:
        if e.errno != EEXIST:
            raise
    harness = HarnessClass()
    process_sessions(harness,
                     args['index_filename'],
                     pickles_path,
                     options.temp_pickles_dir,
                     options.workers,
                     options.ignore_pickles,
                     options.cached_global_context)

# You can't run this harness, since it's an abstract class.  But to run your own
# harness, put the following lines in the module with your harness, replacing
# "Harness" with the name of your Harness subclass.
#
#if __name__ == '__main__':
#    main(Harness)
