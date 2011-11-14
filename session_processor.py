from abc import abstractmethod, ABCMeta

class SessionProcessor(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    def process_update(self, session_context, update):
        """Process a single update. This method is guaranteed to be called with
        updates with sequence numbers incrementing from 0."""

    def finished_session(self):
        pass

class ProcessorCoordinator(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    def create_processor(self, session):
        pass

    def finished_processing(self, global_context):
        """This method is called when all computations are complete."""