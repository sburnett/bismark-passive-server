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
        bytes_per_minute_args = []
        for rounded_timestamp, size in bytes_per_minute.items():
            bytes_per_minute_args.append((node_id, rounded_timestamp, size))
        cur.executemany('''INSERT INTO bytes_per_minute
                           (node_id, timestamp, bytes_transferred)
                           VALUES (%s, %s, %s)''',
                        bytes_per_minute_args)
        cur.execute('DELETE FROM bytes_per_port_per_minute WHERE node_id = %s',
                    (node_id, ));
        bytes_per_port_per_minute_args = []
        for (rounded_timestamp, port), size in \
                bytes_per_port_per_minute.items():
            bytes_per_port_per_minute_args.append(
                    (node_id, rounded_timestamp, port, size))
        cur.executemany('''INSERT INTO bytes_per_port_per_minute
                           (node_id, timestamp, port, bytes_transferred)
                           VALUES (%s, %s, %s, %s)''',
                        bytes_per_port_per_minute_args)
        cur.execute(
                'DELETE FROM bytes_per_domain_per_minute WHERE node_id = %s',
                (node_id, ));
        bytes_per_domain_per_minute_args = []
        for (rounded_timestamp, domain), size in \
                bytes_per_domain_per_minute.items():
            bytes_per_domain_per_minute_args.append(
                    (node_id, rounded_timestamp, domain, size))
        cur.executemany('''INSERT INTO bytes_per_domain_per_minute
                           (node_id, timestamp, domain, bytes_transferred)
                           VALUES (%s, %s, %s, %s)''',
                        bytes_per_domain_per_minute_args)

        self._conn.commit()
