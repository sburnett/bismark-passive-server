from abc import abstractmethod, ABCMeta

class SessionProcessor(object):
    __metaclass__ = ABCMeta

    def initialize_persistent_context(self, persistent_context):
        pass

    def initialize_ephemeral_context(self, ephemeral_context):
        pass

    @abstractmethod
    def process_update(self, persistent_context, ephemeral_context, update):
        """Process a single update. This method is guaranteed to be called with
        updates with sequence numbers incrementing from 0."""

    def complete_session(self, persistent_context, ephemeral_context):
        pass

    def initialize_global_context(self, global_context):
        pass

    @abstractmethod
    def merge_contexts(self, persistent_context, ephemeral_context, global_context):
        pass

class PersistentSessionProcessor(SessionProcessor):
    def process_update(self, persistent_context, ephemeral_context, update):
        self.process_update_persistent(persistent_context, update)

    @abstractmethod
    def process_update_persistent(self, persistent_context, update):
        pass

    def merge_contexts(self, persistent_context, ephemeral_context, global_context):
        self.merge_contexts_persistent(persistent_context, global_context)

    @abstractmethod
    def merge_contexts_persistent(self, persistent_context, global_context):
        pass

class EphemeralSessionProcessor(SessionProcessor):
    def process_update(self, persistent_context, ephemeral_context, update):
        self.process_update_ephemeral(ephemeral_context, update)

    @abstractmethod
    def process_update_ephemeral(self, ephemeral_context, update):
        pass

    def merge_contexts(self, persistent_context, ephemeral_context, global_context):
        self.merge_contexts_ephemeral(ephemeral_context, global_context)

    @abstractmethod
    def merge_contexts_ephemeral(self, ephemeral_context, global_context):
        pass
