from abc import abstractmethod, ABCMeta

from session_processor import SessionProcessor
from database import BismarkPassiveDatabase

class DatabaseSessionProcessor(SessionProcessor):
    __metaclass__ = ABCMeta

    def __init__(self, username, database):
        super(DatabaseSessionProcessor, self).__init__()
        self._username = username
        self._database = database

    def finished_processing(self, global_context):
        database = BismarkPassiveDatabase(self._username, self._database)
        self.write_to_database(database, global_context)

    @abstractmethod
    def write_to_database(self, database, global_context):
        """Implement this method instead of override finished_processing."""
