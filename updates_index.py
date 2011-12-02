from collections import namedtuple
import sqlite3

Session = namedtuple('Session', ['node_id', 'anonymization_context', 'id'])

class UpdatesIndex(object):
    def __init__(self, filename):
        self._conn = sqlite3.connect(filename)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute('''CREATE TABLE IF NOT EXISTS tarnames
                              (tarname text PRIMARY KEY)''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS sessions
                              (node_id text,
                               anonymization_context text,
                               session_id integer,
                               bytes_transferred integer,
                               UNIQUE (node_id,
                                       anonymization_context,
                                       session_id))''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS updates
                              (node_id text,
                               anonymization_context text,
                               session_id integer,
                               sequence_number integer,
                               tarname text,
                               filename text,
                               UNIQUE (tarname, filename),
                               UNIQUE (node_id,
                                       anonymization_context,
                                       session_id,
                                       sequence_number))''')
        self._conn.execute('''CREATE INDEX IF NOT EXISTS
                              data_index ON updates (node_id,
                                                     anonymization_context,
                                                     session_id)''')
        self._conn.commit()

    @property
    def tarnames(self):
        cur = self._conn.execute('SELECT tarname FROM tarnames')
        filenames_processed = set()
        for row in cur:
            filenames_processed.add(row['tarname'])
        return filenames_processed

    def index(self,
              tarname,
              filename,
              node_id,
              anonymization_context,
              session_id,
              sequence_number,
              file_size):
        self._conn.execute('INSERT OR IGNORE INTO tarnames VALUES (?)',
                           (tarname,))
        self._conn.execute('''INSERT OR REPLACE INTO sessions
                              (node_id,
                               anonymization_context,
                               session_id,
                               bytes_transferred)
                              VALUES
                              (?, ?, ?,
                               coalesce((SELECT bytes_transferred + %d
                                         FROM sessions
                                         WHERE node_id = ?
                                         AND anonymization_context = ?
                                         AND session_id = ?), ?))'''
                                         % (file_size,),
                           (node_id, anonymization_context, session_id,
                            node_id, anonymization_context, session_id,
                            file_size))
        self._conn.execute('''INSERT INTO updates
                              (node_id,
                               anonymization_context,
                               session_id,
                               sequence_number,
                               tarname,
                               filename)
                              VALUES (?, ?, ?, ?, ?, ?)''',
                           (node_id,
                            anonymization_context,
                            session_id,
                            sequence_number,
                            tarname,
                            filename))

    @property
    def sessions(self):
        cur = self._conn.execute(
                '''SELECT node_id, anonymization_context, session_id
                   FROM sessions
                   ORDER BY bytes_transferred DESC''')
        for row in cur:
            yield Session(
                node_id=row['node_id'],
                anonymization_context=row['anonymization_context'],
                id=row['session_id'])

    def session_data(self, session):
        cur = self._conn.execute('''SELECT tarname, filename
                                    FROM updates
                                    WHERE node_id = ?
                                    AND anonymization_context = ?
                                    AND session_id = ?
                                    ORDER BY sequence_number''',
                                 (session.node_id,
                                  session.anonymization_context,
                                  session.id))
        data = []
        for row in cur:
            data.append((row['tarname'], row['filename']))
        return data

    def __del__(self):
        self._conn.commit()
