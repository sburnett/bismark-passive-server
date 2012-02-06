from collections import namedtuple
from itertools import imap
from os.path import basename
try:
    from cPickle import dumps, loads, HIGHEST_PROTOCOL
except ImportError:
    from pickle import dumps, loads, HIGHEST_PROTOCOL
import psycopg2
from zlib import compress, decompress

Session = namedtuple('Session', ['node_id', 'anonymization_context', 'id'])

class UpdatesIndex(object):
    def __init__(self, database, postgres_host=None, postgres_user=None):
        self._conn = psycopg2.connect(database=database)

class UpdatesIndexer(UpdatesIndex):
    def __init__(self, database, **options):
        super(UpdatesIndexer, self).__init__(database, **options)

    @property
    def tarnames(self):
        cur = self._conn.cursor()
        cur.execute('SELECT tarname FROM tarnames')
        return map(lambda row: row[0], cur)

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
        cur = self._conn.cursor()
        if reindex:
            cur.execute('DROP INDEX IF EXISTS updates_index')
        cur.executemany('INSERT INTO tarnames (tarname) VALUES (%s)',
                        imap(lambda n: (basename(n),), tarnames))
        print 'Inserting new updates'
        cur.executemany(
                '''INSERT INTO updates
                   (node_id,
                    anonymization_context,
                    session_id,
                    sequence_number,
                    pickle,
                    size)
                   VALUES (%s, %s, %s, %s, %s, %s)''',
                imap(UpdatesIndexer.map_update, updates))
        print 'Building index'
        if reindex:
            cur.execute('''CREATE INDEX
                           updates_index ON updates
                           (node_id,
                            anonymization_context,
                            session_id,
                            sequence_number)''')
        print 'Computing sessions'
        cur.execute('DELETE FROM sessions')
        cur.execute(
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
    def __init__(self, database, **options):
        super(UpdatesReader, self).__init__(database, **options)

    @property
    def sessions(self):
        cur = self._conn.cursor()
        cur.execute('''SELECT node_id, anonymization_context, session_id
                       FROM sessions ORDER BY pickle_size DESC''')
        for row in cur:
            yield Session(row[0], row[1], row[2])

    def session_data(self, session, first_sequence_number=0):
        cur = self._conn.cursor()
        cur.execute('''SELECT sequence_number, pickle FROM updates
                       WHERE node_id = %s
                       AND anonymization_context = %s
                       AND session_id = %s
                       AND sequence_number >= %s
                       ORDER BY sequence_number''',
                    (session.node_id,
                     session.anonymization_context,
                     session.id,
                     first_sequence_number))
        for row in cur:
            yield (row[0], loads(decompress(row[1])))
