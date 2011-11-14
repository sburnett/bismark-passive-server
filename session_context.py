import cPickle
import gzip
from os.path import join
import tarfile
from update_parser import PassiveUpdate

class SessionContext(object):
    def __init__(self, node_id, anonymization_id, session_id):
        self._node_id = node_id
        self._anonymization_id = anonymization_id
        self._session_id = session_id

    @property
    def node_id(self):
        return self._node_id
    @property
    def anonymization_id(self):
        return self._anonymization_id
    @property
    def session_id(self):
        return self._session_id

class GlobalContext(object):
    pass

class SessionContextManager(object):
    def __init__(self):
        self._names = set()
        self._initializers = dict()
        self._mergers = dict()

    def declare_state(self, name, init_func, merge_func):
        if name in self._names:
            raise ValueError('Context state has already been declared')
        self._names.add(name)
        self._initializers[name] = init_func
        self._mergers[name] = merge_func

    def create_context(self, node_id, anonymization_id, session_id):
        context = SessionContext(node_id, anonymization_id, session_id)
        for name, initialize in self._initializers.iteritems():
            setattr(context, name, initialize())
        return context

    def load_context(self, filename):
        try:
            context = cPickle.load(open(filename, 'rb'))
        except:
            return None
        for name, initialize in self._initializers.iteritems():
            if not hasattr(context, name):
                setattr(context, name, initialize())
        return context

    def save_context(self, context, filename):
        cPickle.dump(context, open(filename, 'wb'), 2)

    def merge_contexts(self, session_context, global_context):
        for name, merger in self._mergers.iteritems():
            if merger is not None:
                if not hasattr(global_context, name):
                    setattr(global_context, name, self._initializers[name]())
                setattr(global_context,
                        name,
                        merger(getattr(session_context, name),
                               getattr(global_context, name)))
