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

    def merge_contexts(self, persistent_context, ephemeral_context, global_context):
        pass

    def complete_global_context(self, global_context):
        pass

class PersistentSessionProcessor(SessionProcessor):
    def initialize_persistent_context(self, persistent_context):
        self.initialize_context(persistent_context)

    def initialize_context(self, context):
        pass

    def process_update(self, persistent_context, ephemeral_context, update):
        self.process_update_persistent(persistent_context, update)

    @abstractmethod
    def process_update_persistent(self, persistent_context, update):
        pass

    def complete_session(self, persistent_context, ephemeral_context):
        self.complete_session_persistent(persistent_context)

    def complete_session_persistent(self, persistent_context):
        pass

    def merge_contexts(self, persistent_context, ephemeral_context, global_context):
        self.merge_contexts_persistent(persistent_context, global_context)

    def merge_contexts_persistent(self, persistent_context, global_context):
        pass

class EphemeralSessionProcessor(SessionProcessor):
    def initialize_ephemeral_context(self, ephemeral_context):
        self.initialize_context(ephemeral_context)

    def initialize_context(self, context):
        pass

    def process_update(self, persistent_context, ephemeral_context, update):
        self.process_update_ephemeral(ephemeral_context, update)

    @abstractmethod
    def process_update_ephemeral(self, ephemeral_context, update):
        pass

    def complete_session(self, persistent_context, ephemeral_context):
        self.complete_session_ephemeral(ephemeral_context)

    def complete_session_ephemeral(self, ephemeral_context):
        pass

    def merge_contexts(self, persistent_context, ephemeral_context, global_context):
        self.merge_contexts_ephemeral(ephemeral_context, global_context)

    def merge_contexts_ephemeral(self, ephemeral_context, global_context):
        pass
