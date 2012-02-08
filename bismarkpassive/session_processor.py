from abc import abstractmethod, ABCMeta

class SessionProcessor(object):

    """
    An abstract base class for processing bismark-passive session data.

    We instantiate a set of session processors to run computations on the data.
    There is one such set of session processors for every session in the data,
    which means there will usually be hundreds of session processor objects.
    
    Instead of using member variables, the processors should read and write
    results into two shared *contexts*: the *persistent* and *ephemeral*
    contexts. The set of session processors for a given session all share the
    same persistent and ephemeral contexts. Contexts are instances of the
    ``PersistentContext`` and ``EphemeralContext`` classes in the
    ``process_sessions`` module. You should create the additional member
    variables you need in the contexts using the initialize_persistent_context
    and initialize_ephemeral_context methods.
    
    We serialize persistent contexts to disk, which lets us quickly augment them
    with new data by rerunning the processors under the existing persistent
    contexts. Ephemeral contexts are *not* serialized to disk, so fresh
    ephemeral contexts are created every time we augment the sessions.  In most
    cases you should only use the persistent context.
    PersistentSessionProcessor enforces this constraint.

    After the session processors consume all available data, we merge the
    session contexts into one *global context*. It is the final result of the
    computations we run on the data. You typically use the contents of the
    global context to produce a graph or write information to a database. The
    global context is not serialized to disk.

    A recap of the relationships between sessions, session processors,
    persistent and ephemeral contexts, and the global context: Each session has
    many session processors (each for doing a separate kind of computation on
    that session's data) but only one persistent and one ephemeral context. We
    merge the persistent and ephemeral contexts into a single global context,
    which represents the final results of all computation. When new data arrives
    for a session, we can augment the persistent context and produce a new
    global context by remerging all the persistent contexts.
    """

    __metaclass__ = ABCMeta

    def __init__(self, options):
        self._options = options

    @property
    def options(self):
        return self._options

    def initialize_persistent_context(self, persistent_context):
        """Initialize members of the persistent context.

        persistent_context is an object, so initialize it by creating new member
        variables using dot notation. (e.g., persistent_context.my_counter = 0.)
        We initialize a session's persistent context once, before it processes
        any data for that session."""

    def initialize_ephemeral_context(self, ephemeral_context):
        """Initialize members of the ephemeral context.

        ephemeral_context is an object, so initialize it by creating new member
        variables using dot notation. (e.g., ephemeral_context.my_counter = 0.)
        We initialize a session's ephemeral context every time we augment a
        session with additional data."""

    @abstractmethod
    def process_update(self, persistent_context, ephemeral_context, update):
        """Process a single update.
        
        See the update_parser module for documentation on "update". We will
        always call this method with updates with sequence numbers incrementing
        from 0. We will initialize the persistent and ephemeral contexts before
        calling this method."""

    def complete_session(self, persistent_context, ephemeral_context):
        """We call this method after processing all of a session's data.

        We may call this method many times during the lifetime of a persistent
        context, whenever we augment the session with new data."""

    def initialize_global_context(self, global_context):
        """Initialize members of the global context.
        
        global_context is an object, so initialize it by creating new member
        variables using dot notation. (e.g., global_context.my_counter = 0.)"""

    def merge_contexts(self, persistent_context, ephemeral_context, global_context):
        """Merge the session's contexts into the global context."""

    def complete_global_context(self, global_context):
        """We call this method after merging peristent and ephemeral contexts
        for all sessions into the global context."""

class PersistentSessionProcessor(SessionProcessor):

    """
    A base class for session processors with no ephemeral context.

    You should use subclasses of this processor unless you explicitely need an
    ephemeral context, which is very rare.
    """

    def initialize_persistent_context(self, persistent_context):
        """Do not override this method."""
        self.initialize_context(persistent_context)

    def initialize_context(self, context):
        """You should override this method, not
        initialize_persistent_context."""

    def process_update(self, persistent_context, ephemeral_context, update):
        """Do not override this method."""
        self.process_update_persistent(persistent_context, update)

    @abstractmethod
    def process_update_persistent(self, persistent_context, update):
        """You should override this method, not process_update."""

    def complete_session(self, persistent_context, ephemeral_context):
        """Do not override this method."""
        self.complete_session_persistent(persistent_context)

    def complete_session_persistent(self, persistent_context):
        """You should override this method, not complete_session."""

    def merge_contexts(self, persistent_context, ephemeral_context, global_context):
        """Do not override this method."""
        self.merge_contexts_persistent(persistent_context, global_context)

    def merge_contexts_persistent(self, persistent_context, global_context):
        """You should override this method, not merge_contexts."""
