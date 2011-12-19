from abc import abstractmethod, ABCMeta

from session_processor import ProcessorCoordinator
from database_postgres import BismarkPassivePostgresDatabase

class PostgresProcessorCoordinator(ProcessorCoordinator):
    __metaclass__ = ABCMeta

    def __init__(self, options):
        super(PostgresProcessorCoordinator, self).__init__(options)
        self._username = options.db_user
        self._password = options.db_password
        self._database = options.db_name
        self._host = options.db_host
        self._port = options.db_port

    def finished_processing(self, global_context):
        database = BismarkPassivePostgresDatabase(self._username,
                                                  self._password,
                                                  self._host,
                                                  self._port,
                                                  self._database,
                                                  )
        self.write_to_database(database, global_context)

    @abstractmethod
    def write_to_database(self, database, global_context):
        """Implement this method instead of overriding finished_processing."""
