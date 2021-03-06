from collections import namedtuple
from itertools import imap
from os.path import basename
try:
    from cPickle import dumps, loads, HIGHEST_PROTOCOL
except ImportError:
    from pickle import dumps, loads, HIGHEST_PROTOCOL
import sqlite3
from zlib import compress, decompress

Session = namedtuple('Session', ['node_id', 'anonymization_context', 'id'])

DATABASE_LOCK_TIMEOUT=600  # 10 minutes

class UpdatesIndex(object):
    def __init__(self, filename):
        self._conn = sqlite3.connect(filename, timeout=DATABASE_LOCK_TIMEOUT)
        self._conn.row_factory = sqlite3.Row

class UpdatesIndexer(UpdatesIndex):
    def __init__(self, filename):
        super(UpdatesIndexer, self).__init__(filename)
        self._conn.execute('''PRAGMA synchronous = OFF''')
        self._conn.execute('''PRAGMA journal_mode = MEMORY''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS tarnames
                              (tarname text PRIMARY KEY)''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS sessions
                              (node_id text,
                               anonymization_context text,
                               session_id integer,
                               pickle_size integer,
                               UNIQUE (node_id,
                                       anonymization_context,
                                       session_id)
                              )''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS updates
                              (node_id text,
                               anonymization_context text,
                               session_id integer,
                               sequence_number integer,
                               pickle blob,
                               size integer)''')
        self._conn.commit()

    @property
    def tarnames(self):
        return map(lambda row: row['tarname'],
                   self._conn.execute('''SELECT tarname FROM tarnames'''))

    @staticmethod
    def map_update(update):
        data = buffer(compress(dumps(update, HIGHEST_PROTOCOL)))
        return (update.bismark_id,
                update.anonymization_signature,
                update.creation_time,
                update.sequence_number,
                data,
                len(data))

    @staticmethod
    def map_session(update):
        return (update.bismark_id,
                update.anonymization_signature,
                update.creation_time)

    def index(self, tarnames, updates, reindex=False):
        if reindex:
            self._conn.execute('DROP INDEX IF EXISTS updates_index')
        self._conn.executemany(
                'INSERT OR IGNORE INTO tarnames (tarname) VALUES (?)',
                imap(lambda n: (basename(n),), tarnames))
        print 'Inserting new updates'
        self._conn.executemany(
                '''INSERT INTO updates
                   (node_id,
                    anonymization_context,
                    session_id,
                    sequence_number,
                    pickle,
                    size)
                   VALUES (?, ?, ?, ?, ?, ?)''',
                imap(UpdatesIndexer.map_update, updates))
        print 'Building index'
        self._conn.execute('''CREATE INDEX IF NOT EXISTS
                              updates_index ON updates
                              (node_id,
                               anonymization_context,
                               session_id,
                               sequence_number)''')
        print 'Computing sessions'
        self._conn.execute('DELETE FROM sessions')
        self._conn.execute(
                '''INSERT INTO sessions
                   (node_id, anonymization_context, session_id, pickle_size)
                   SELECT node_id,
                          anonymization_context,
                          session_id,
                          sum(size)
                   FROM updates
                   GROUP BY node_id, anonymization_context, session_id''')
        self._conn.commit()

class UpdatesReader(UpdatesIndex):
    def __init__(self, filename):
        super(UpdatesReader, self).__init__(filename)

    @property
    def sessions(self):
        for row in self._conn.execute(
                '''SELECT node_id, anonymization_context, session_id
                   FROM sessions ORDER BY pickle_size DESC'''):
            yield Session(row['node_id'],
                          row['anonymization_context'],
                          row['session_id'])

    def session_data(self, session, first_sequence_number=0):
        for row in self._conn.execute(
                '''SELECT sequence_number, pickle FROM updates
                   WHERE node_id = ?
                   AND anonymization_context = ?
                   AND session_id = ?
                   AND sequence_number >= ?
                   ORDER BY sequence_number''',
                (session.node_id,
                 session.anonymization_context,
                 session.id,
                 first_sequence_number)):
            yield (row['sequence_number'],
                   loads(decompress(row['pickle'])))
