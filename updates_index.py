from collections import namedtuple
import sqlite3

Session = namedtuple('Session', ['node_id', 'anonymization_context', 'id'])

class UpdatesIndex(object):
    def __init__(self, filename):
        self._conn = sqlite3.connect(filename)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute('''CREATE TABLE IF NOT EXISTS updates
                              (tarname text,
                               filename text,
                               node_id text,
                               anonymization_context text,
                               session_id integer,
                               sequence_number integer)''')
        self._conn.execute('''CREATE INDEX IF NOT EXISTS
                              data_index ON updates (node_id, session_id)''')
        self._conn.commit()

    @property
    def tarnames(self):
        cur = self._conn.execute('SELECT DISTINCT tarname FROM updates')
        filenames_processed = set()
        for row in cur:
            filenames_processed.add(row['tarname'])
        return filenames_processed

    def index(self, *args):
        self._conn.execute('''INSERT INTO updates
                              (tarname,
                               filename,
                               node_id,
                               anonymization_context,
                               session_id,
                               sequence_number)
                              VALUES (?, ?, ?, ?, ?, ?)''', args)

    @property
    def sessions(self):
        cur = self._conn.execute('''SELECT DISTINCT
                                    node_id, anonymization_context, session_id
                                    FROM updates
                                    GROUP BY node_id, anonymization_context, session_id
                                    ORDER BY count(rowid) DESC''')
        sessions = list()
        for row in cur:
            sessions.append(Session(
                node_id=row['node_id'],
                anonymization_context=row['anonymization_context'],
                id=row['session_id']))
        return sessions

    def session_data(self, session):
        cur = self._conn.execute('''SELECT DISTINCT tarname, filename
                                    FROM updates
                                    WHERE node_id = ?
                                    AND session_id = ?
                                    ORDER BY sequence_number''',
                                 (session.node_id,
                                  session.id))
        data = []
        for row in cur:
            data.append((row['tarname'], row['filename']))
        return data

    def __del__(self):
        self._conn.commit()
