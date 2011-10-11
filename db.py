import psycopg2

class BismarkPassiveDatabase(object):
    def __init__(self, user, database):
        self._conn = psycopg2.connect(user=user, database=database)
        cur = self._conn.cursor()
        cur.execute('SET search_path TO bismark_passive')
        cur.close()
        self._conn.commit()

    def import_statistics(self,
                          node_id,
                          bytes_per_minute,
                          bytes_per_port_per_minute,
                          bytes_per_domain_per_minute):
        cur = self._conn.cursor()
        cur.execute('DELETE FROM bytes_per_minute WHERE node_id = %s',
                    (node_id, ));
        for rounded_timestamp, size in bytes_per_minute.items():
            cur.execute('''INSERT INTO bytes_per_minute
                           (node_id, timestamp, bytes_transferred)
                           VALUES (%s, %s, %s)''',
                        (node_id,
                         rounded_timestamp,
                         size))
        cur.execute('DELETE FROM bytes_per_port_per_minute WHERE node_id = %s',
                    (node_id, ));
        for (rounded_timestamp, port), size in bytes_per_port_per_minute.items():
            cur.execute('''INSERT INTO bytes_per_port_per_minute
                           (node_id, timestamp, port, bytes_transferred)
                           VALUES (%s, %s, %s, %s)''',
                        (node_id,
                         rounded_timestamp,
                         port,
                         size))
        cur.execute('DELETE FROM bytes_per_domain_per_minute WHERE node_id = %s',
                    (node_id, ));
        for (rounded_timestamp, domain), size in bytes_per_domain_per_minute.items():
            cur.execute('''INSERT INTO bytes_per_domain_per_minute
                           (node_id, timestamp, domain, bytes_transferred)
                           VALUES (%s, %s, %s, %s)''',
                        (node_id,
                         rounded_timestamp,
                         domain,
                         size))

        self._conn.commit()
