from abc import abstractmethod, ABCMeta

from session_processor import ProcessorCoordinator
from database_sqlite import BismarkPassiveSqliteDatabase

class SqliteProcessorCoordinator(ProcessorCoordinator):
    __metaclass__ = ABCMeta

    def __init__(self, options):
        super(SqliteProcessorCoordinator, self).__init__(options)
        self._filename = options.db_filename

    def finished_processing(self, global_context):
        database = BismarkPassiveSqliteDatabase(self._filename)
        self.write_to_database(database, global_context)

    @abstractmethod
    def write_to_database(self, database, global_context):
        """Implement this method instead of overriding finished_processing."""
