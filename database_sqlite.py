import sqlite3

class BismarkPassiveSqliteDatabase(object):
    def __init__(self, filename):
        self._conn = sqlite3.connect(filename)
        self._conn.row_factory = sqlite3.Row

        self._conn.execute('''PRAGMA journal_mode = MEMORY''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS bytes_per_ip (
                               node_id text NOT NULL,
                               anonymization_context text NOT NULL,
                               ip text NOT NULL,
                               count integer NOT NULL,
                               UNIQUE (node_id, anonymization_context, ip)
                              )''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS packets_per_ip (
                               node_id text NOT NULL,
                               anonymization_context text NOT NULL,
                               ip text NOT NULL,
                               count integer NOT NULL,
                               UNIQUE (node_id, anonymization_context, ip)
                              )''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS bytes_per_flow (
                               node_id text NOT NULL,
                               bytes integer NOT NULL,
                               count integer NOT NULL,
                               UNIQUE (node_id, bytes)
                              )''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS bytes_per_port_per_flow (
                                node_id text NOT NULL,
                                port integer NOT NULL,
                                bytes integer NOT NULL,
                                count integer NOT NULL,
                                UNIQUE (node_id, port, bytes)
                              )''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS bytes_per_domain_per_flow (
                                  node_id text NOT NULL,
                                  domain text NOT NULL,
                                  bytes integer NOT NULL,
                                  count integer NOT NULL,
                                  UNIQUE (node_id, domain, bytes)
                              )''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS bytes_per_device_per_flow (
                                  node_id text NOT NULL,
                                  device text NOT NULL,
                                  bytes integer NOT NULL,
                                  count integer NOT NULL,
                                  UNIQUE (node_id, device, bytes)
                              )''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS packets_per_flow (
                                  node_id text NOT NULL,
                                  packets integer NOT NULL,
                                  count integer NOT NULL,
                                  UNIQUE (node_id, packets)
                              )''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS packets_per_port_per_flow (
                                  node_id text NOT NULL,
                                  port integer NOT NULL,
                                  packets integer NOT NULL,
                                  count integer NOT NULL,
                                  UNIQUE (node_id, port, packets)
                              )''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS packets_per_domain_per_flow (
                                  node_id text NOT NULL,
                                  domain text NOT NULL,
                                  packets integer NOT NULL,
                                  count integer NOT NULL,
                                  UNIQUE (node_id, domain, packets)
                              )''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS packets_per_device_per_flow (
                                  node_id text NOT NULL,
                                  device text NOT NULL,
                                  packets integer NOT NULL,
                                  count integer NOT NULL,
                                  UNIQUE (node_id, device, packets)
                              )''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS seconds_per_flow (
                                  node_id text NOT NULL,
                                  seconds integer NOT NULL,
                                  count integer NOT NULL,
                                  UNIQUE (node_id, seconds)
                              )''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS seconds_per_port_per_flow (
                                  node_id text NOT NULL,
                                  port integer NOT NULL,
                                  seconds integer NOT NULL,
                                  count integer NOT NULL,
                                  UNIQUE (node_id, port, seconds)
                              )''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS seconds_per_domain_per_flow (
                                  node_id text NOT NULL,
                                  domain text NOT NULL,
                                  seconds integer NOT NULL,
                                  count integer NOT NULL,
                                  UNIQUE (node_id, domain, seconds)
                              )''')
        self._conn.execute('''CREATE TABLE IF NOT EXISTS seconds_per_device_per_flow (
                                  node_id text NOT NULL,
                                  device text NOT NULL,
                                  seconds integer NOT NULL,
                                  count integer NOT NULL,
                                  UNIQUE (node_id, device, seconds)
                              )''')

    def import_bytes_per_ip(self, bytes_per_ip):
        self._conn.execute('DELETE FROM bytes_per_ip')
        args = []
        for (node_id, anonymization_id, ip), count in bytes_per_ip.iteritems():
            args.append((node_id, anonymization_id, ip, count))
        self._conn.executemany('''INSERT INTO bytes_per_ip
                                  (node_id, anonymization_context, ip, count)
                                  VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_packets_per_ip(self, packets_per_ip):
        self._conn.execute('DELETE FROM packets_per_ip')
        args = []
        for (node_id, anonymization_id, ip), count in packets_per_ip.iteritems():
            args.append((node_id, anonymization_id, ip, count))
        self._conn.executemany('''INSERT INTO packets_per_ip
                                  (node_id, anonymization_context, ip, count)
                                  VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_bytes_per_flow(self, bytes_per_flow):
        self._conn.execute('DELETE FROM bytes_per_flow')
        args = []
        for (node_id, size), count in bytes_per_flow.iteritems():
            args.append((node_id, size, count))
        self._conn.executemany('''INSERT INTO bytes_per_flow
                                  (node_id, bytes, count)
                                  VALUES (?, ?, ?)''', args)
        self._conn.commit()

    def import_packets_per_flow(self, packets_per_flow):
        cur = self._conn.cursor()
        args = []
        cur.execute('DELETE FROM packets_per_flow')
        for (node_id, size), count in packets_per_flow.iteritems():
            args.append((node_id, size, count))
        cur.executemany('''INSERT INTO packets_per_flow
                           (node_id, packets, count)
                           VALUES (?, ?, ?)''', args)
        self._conn.commit()

    def import_seconds_per_flow(self, seconds_per_flow):
        cur = self._conn.cursor()
        args = []
        cur.execute('DELETE FROM seconds_per_flow')
        for (node_id, size), count in seconds_per_flow.iteritems():
            args.append((node_id, size, count))
        cur.executemany('''INSERT INTO seconds_per_flow
                           (node_id, seconds, count)
                           VALUES (?, ?, ?)''', args)
        self._conn.commit()

    def import_bytes_per_port_per_flow(self, bytes_per_port_per_flow):
        cur = self._conn.cursor()
        args = []
        cur.execute('DELETE FROM bytes_per_port_per_flow')
        for (node_id, port, size), count in bytes_per_port_per_flow.iteritems():
            args.append((node_id, port, size, count))
        cur.executemany('''INSERT INTO bytes_per_port_per_flow
                           (node_id, port, bytes, count)
                           VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_packets_per_port_per_flow(self, packets_per_port_per_flow):
        cur = self._conn.cursor()
        args = []
        cur.execute('DELETE FROM packets_per_port_per_flow')
        for (node_id, port, size), count \
                in packets_per_port_per_flow.iteritems():
            args.append((node_id, port, size, count))
        cur.executemany('''INSERT INTO packets_per_port_per_flow
                           (node_id, port, packets, count)
                           VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_seconds_per_port_per_flow(self, seconds_per_port_per_flow):
        cur = self._conn.cursor()
        args = []
        cur.execute('DELETE FROM seconds_per_port_per_flow')
        for (node_id, port, duration), count \
                in seconds_per_port_per_flow.iteritems():
            args.append((node_id, port, duration, count))
        cur.executemany('''INSERT INTO seconds_per_port_per_flow
                           (node_id, port, seconds, count)
                           VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_bytes_per_domain_per_flow(self, bytes_per_domain_per_flow):
        cur = self._conn.cursor()
        args = []
        cur.execute('DELETE FROM bytes_per_domain_per_flow')
        for (node_id, domain, size), count \
                in bytes_per_domain_per_flow.iteritems():
            args.append((node_id, domain, size, count))
        cur.executemany('''INSERT INTO bytes_per_domain_per_flow
                           (node_id, domain, bytes, count)
                           VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_packets_per_domain_per_flow(self, packets_per_domain_per_flow):
        cur = self._conn.cursor()
        args = []
        cur.execute('DELETE FROM packets_per_domain_per_flow')
        for (node_id, domain, size), count \
                in packets_per_domain_per_flow.iteritems():
            args.append((node_id, domain, size, count))
        cur.executemany('''INSERT INTO packets_per_domain_per_flow
                           (node_id, domain, packets, count)
                           VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_seconds_per_domain_per_flow(self, seconds_per_domain_per_flow):
        cur = self._conn.cursor()
        args = []
        cur.execute('DELETE FROM seconds_per_domain_per_flow')
        for (node_id, domain, duration), count \
                in seconds_per_domain_per_flow.iteritems():
            args.append((node_id, domain, duration, count))
        cur.executemany('''INSERT INTO seconds_per_domain_per_flow
                           (node_id, domain, seconds, count)
                           VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_bytes_per_device_per_flow(self, bytes_per_device_per_flow):
        cur = self._conn.cursor()
        args = []
        cur.execute('DELETE FROM bytes_per_device_per_flow')
        for (node_id, device, size), count \
                in bytes_per_device_per_flow.iteritems():
            args.append((node_id, device, size, count))
        cur.executemany('''INSERT INTO bytes_per_device_per_flow
                           (node_id, device, bytes, count)
                           VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_packets_per_device_per_flow(self, packets_per_device_per_flow):
        cur = self._conn.cursor()
        args = []
        cur.execute('DELETE FROM packets_per_device_per_flow')
        for (node_id, device, size), count \
                in packets_per_device_per_flow.iteritems():
            args.append((node_id, device, size, count))
        cur.executemany('''INSERT INTO packets_per_device_per_flow
                           (node_id, device, packets, count)
                           VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_seconds_per_device_per_flow(self, seconds_per_device_per_flow):
        cur = self._conn.cursor()
        args = []
        cur.execute('DELETE FROM seconds_per_device_per_flow')
        for (node_id, device, duration), count \
                in seconds_per_device_per_flow.iteritems():
            args.append((node_id, device, duration, count))
        cur.executemany('''INSERT INTO seconds_per_device_per_flow
                           (node_id, device, seconds, count)
                           VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()
