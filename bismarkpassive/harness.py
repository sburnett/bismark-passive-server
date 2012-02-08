from abc import abstractmethod, ABCMeta
from errno import EEXIST
from optparse import OptionParser
from os import makedirs
from os.path import join

from process_sessions import process_sessions

class Harness(object):

    """
    An abstract base class for consuming the results of computations on
    bismark-passive data.

    A harness does two things: First, it specifies a sequence of session
    processors to run on the bismark-passive data. Second, it does something
    useful with the results of those session processors.
    
    """

    __metaclass__ = ABCMeta

    def __init__(self, options):
        self._options = options

    @property
    def options(self):
        """Return the command line options.
        
        Please don't modify this object."""
        return self._options

    @property
    def processors(self):
        """These are the session processor classes to instantiate and run for
        each session.

        Session processors are always run the the order given."""
        return []

    @property
    def exclude_nodes(self):
        """Exclude this set of nodes from processing.
        
        Exclusion takes priority over inclusion."""
        return set([])

    @property
    def include_nodes(self):
        """Include only this set of nodes while processing.

        Exclusion takes priority over inclusion."""
        return set([])

    @staticmethod
    def setup_options(parser):
        """Add command line arguments for the harness.
        
        Keep arguments in alphabetical order. Don't use short options in this
        function. You can also add options in a subclass, but make sure to call
        this parent version first."""
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
        return map(lambda P: P(self.options), self.processors)

    @abstractmethod
    def process_results(self, global_context):
        """Override this method to do something useful with the global context.

        The global context contains the results of all the processors we ran."""

def parse_args(HarnessClass):
    usage = 'usage: %prog [options] ' \
            'database_backend database_name pickles_directory'
    parser = OptionParser(usage=usage)
    parser.add_option('-t', '--temp-pickles-dir', action='store',
                      dest='temp_pickles_dir', default='/dev/shm',
                      help='Directory for temporary runtime pickle storage')
    parser.add_option('-w', '--workers', type='int', action='store',
                      dest='workers',
                      help='Maximum number of worker threads to use')
    parser.add_option('-p', '--ignore-pickles', action='store_true',
                      dest='ignore_pickles', default=False,
                      help='Compute from scratch (use when processors change)')
    parser.add_option('-g', '--cached-global-context', action='store',
                      dest='cached_global_context',
                      help='Attempt to cache global context to the given filename')
    parser.add_option('-r', '--run-name', action='store',
                      dest='run_name', default='default',
                      help='Assign a name to this processing run')
    parser.add_option('--indexer-postgres-user', action='store',
                      dest='indexer_postgres_user',
                      help='Log into Postgres as this user')
    parser.add_option('--indexer-postgres-host', action='store',
                      dest='indexer_postgres_host',
                      help='Log into Postgres on this host')
    HarnessClass.setup_options(parser)
    options, args = parser.parse_args()
    if len(args) != 3:
        parser.error('Missing required option')
    mandatory = { 'database_backend': args[0],
                  'database_name': args[1],
                  'pickles_directory': args[2],
                }
    database_options = {}
    if options.indexer_postgres_host is not None:
        database_options['postgres_host'] = options.indexer_postgres_host
    if options.indexer_postgres_user is not None:
        database_options['postgres_user'] = options.indexer_postgres_user
    return options, mandatory, database_options

def main(HarnessClass):
    (options, args, database_options) = parse_args(HarnessClass)
    pickles_path = join(args['pickles_directory'],
                        HarnessClass.__name__,
                        options.run_name)
    try:
        makedirs(pickles_path)
    except OSError, e:
        if e.errno != EEXIST:
            raise
    harness = HarnessClass(options)
    process_sessions(harness,
                     args['database_backend'],
                     args['database_name'],
                     database_options,
                     pickles_path,
                     options.temp_pickles_dir,
                     options.workers,
                     options.ignore_pickles,
                     options.cached_global_context)

# You can't run this harness, since it's an abstract class. To run your own
# harness, put the following lines in the module with your harness, replacing
# "Harness" with the name of your Harness subclass.
#
#   from bismarkpassive import main
#   ...
#   if __name__ == '__main__':
#       main(Harness)
