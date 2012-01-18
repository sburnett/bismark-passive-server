import sqlite3

class BismarkPassiveSqliteDatabase(object):
    def __init__(self, filename):
        self._conn = sqlite3.connect(filename)
        self._conn.row_factory = sqlite3.Row

        self._conn.execute('''PRAGMA journal_mode = MEMORY''')

    def import_byte_statistics(self, data, oldest_timestamps):
        self._conn.execute('''CREATE TABLE IF NOT EXISTS byte_statistics (
                                  node_id text NOT NULL,
                                  anonymization_context text NOT NULL,
                                  eventstamp text NOT NULL,
                                  mac_address text NOT NULL,
                                  port integer NOT NULL,
                                  domain text NOT NULL,
                                  bytes_transferred integer NOT NULL
                              )''')
        args = []
        for (node_id,
             anonymization_context,
             eventstamp,
             mac_address,
             port,
             domain), bytes_transferred in data.iteritems():
            if eventstamp >= oldest_timestamps[node_id, anonymization_context]:
                args.append((node_id,
                             anonymization_context,
                             eventstamp,
                             mac_address,
                             port,
                             domain,
                             bytes_transferred))
        self._conn.executemany('''INSERT OR REPLACE INTO byte_statistics
                                  (node_id,
                                   anonymization_context,
                                   eventstamp,
                                   mac_address, 
                                   port,
                                   domain,
                                   bytes_transferred)
                                  VALUES (?, ?, ?, ?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_bytes_per_ip(self, bytes_per_ip):
        self._conn.execute('''CREATE TABLE IF NOT EXISTS bytes_per_ip (
                               node_id text NOT NULL,
                               anonymization_context text NOT NULL,
                               ip text NOT NULL,
                               count integer NOT NULL,
                               UNIQUE (node_id, anonymization_context, ip)
                              )''')
        self._conn.execute('DELETE FROM bytes_per_ip')
        args = []
        for (node_id, anonymization_id, ip), count in bytes_per_ip.iteritems():
            args.append((node_id, anonymization_id, ip, count))
        self._conn.executemany('''INSERT INTO bytes_per_ip
                                  (node_id, anonymization_context, ip, count)
                                  VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_packets_per_ip(self, packets_per_ip):
        self._conn.execute('''CREATE TABLE IF NOT EXISTS packets_per_ip (
                               node_id text NOT NULL,
                               anonymization_context text NOT NULL,
                               ip text NOT NULL,
                               count integer NOT NULL,
                               UNIQUE (node_id, anonymization_context, ip)
                              )''')
        self._conn.execute('DELETE FROM packets_per_ip')
        args = []
        for (node_id, anonymization_id, ip), count in packets_per_ip.iteritems():
            args.append((node_id, anonymization_id, ip, count))
        self._conn.executemany('''INSERT INTO packets_per_ip
                                  (node_id, anonymization_context, ip, count)
                                  VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_bytes_per_flow(self, bytes_per_flow):
        self._conn.execute('''CREATE TABLE IF NOT EXISTS bytes_per_flow (
                               node_id text NOT NULL,
                               bytes integer NOT NULL,
                               count integer NOT NULL,
                               UNIQUE (node_id, bytes)
                              )''')
        self._conn.execute('DELETE FROM bytes_per_flow')
        args = []
        for (node_id, size), count in bytes_per_flow.iteritems():
            args.append((node_id, size, count))
        self._conn.executemany('''INSERT INTO bytes_per_flow
                                  (node_id, bytes, count)
                                  VALUES (?, ?, ?)''', args)
        self._conn.commit()

    def import_packets_per_flow(self, packets_per_flow):
        self._conn.execute('''CREATE TABLE IF NOT EXISTS packets_per_flow (
                                  node_id text NOT NULL,
                                  packets integer NOT NULL,
                                  count integer NOT NULL,
                                  UNIQUE (node_id, packets)
                              )''')
        self._conn.execute('DELETE FROM packets_per_flow')
        args = []
        for (node_id, size), count in packets_per_flow.iteritems():
            args.append((node_id, size, count))
        self._conn.executemany('''INSERT INTO packets_per_flow
                                  (node_id, packets, count)
                                  VALUES (?, ?, ?)''', args)
        self._conn.commit()

    def import_seconds_per_flow(self, seconds_per_flow):
        self._conn.execute('''CREATE TABLE IF NOT EXISTS seconds_per_flow (
                                  node_id text NOT NULL,
                                  seconds integer NOT NULL,
                                  count integer NOT NULL,
                                  UNIQUE (node_id, seconds)
                              )''')
        self._conn.execute('DELETE FROM seconds_per_flow')
        args = []
        for (node_id, size), count in seconds_per_flow.iteritems():
            args.append((node_id, size, count))
        self._conn.executemany('''INSERT INTO seconds_per_flow
                                  (node_id, seconds, count)
                                  VALUES (?, ?, ?)''', args)
        self._conn.commit()

    def import_bytes_per_port_per_flow(self, bytes_per_port_per_flow):
        self._conn.execute('''CREATE TABLE IF NOT EXISTS bytes_per_port_per_flow (
                                node_id text NOT NULL,
                                port integer NOT NULL,
                                bytes integer NOT NULL,
                                count integer NOT NULL,
                                UNIQUE (node_id, port, bytes)
                              )''')
        self._conn.execute('DELETE FROM bytes_per_port_per_flow')
        args = []
        for (node_id, port, size), count in bytes_per_port_per_flow.iteritems():
            args.append((node_id, port, size, count))
        self._conn.executemany('''INSERT INTO bytes_per_port_per_flow
                                  (node_id, port, bytes, count)
                                  VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_packets_per_port_per_flow(self, packets_per_port_per_flow):
        self._conn.execute('''CREATE TABLE IF NOT EXISTS packets_per_port_per_flow (
                                  node_id text NOT NULL,
                                  port integer NOT NULL,
                                  packets integer NOT NULL,
                                  count integer NOT NULL,
                                  UNIQUE (node_id, port, packets)
                              )''')
        self._conn.execute('DELETE FROM packets_per_port_per_flow')
        args = []
        for (node_id, port, size), count \
                in packets_per_port_per_flow.iteritems():
            args.append((node_id, port, size, count))
        self._conn.executemany('''INSERT INTO packets_per_port_per_flow
                                  (node_id, port, packets, count)
                                  VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_seconds_per_port_per_flow(self, seconds_per_port_per_flow):
        self._conn.execute('''CREATE TABLE IF NOT EXISTS seconds_per_port_per_flow (
                                  node_id text NOT NULL,
                                  port integer NOT NULL,
                                  seconds integer NOT NULL,
                                  count integer NOT NULL,
                                  UNIQUE (node_id, port, seconds)
                              )''')
        self._conn.execute('DELETE FROM seconds_per_port_per_flow')
        args = []
        for (node_id, port, duration), count \
                in seconds_per_port_per_flow.iteritems():
            args.append((node_id, port, duration, count))
        self._conn.executemany('''INSERT INTO seconds_per_port_per_flow
                                  (node_id, port, seconds, count)
                                  VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_bytes_per_domain_per_flow(self, bytes_per_domain_per_flow):
        self._conn.execute('''CREATE TABLE IF NOT EXISTS bytes_per_domain_per_flow (
                                  node_id text NOT NULL,
                                  domain text NOT NULL,
                                  bytes integer NOT NULL,
                                  count integer NOT NULL,
                                  UNIQUE (node_id, domain, bytes)
                              )''')
        self._conn.execute('DELETE FROM bytes_per_domain_per_flow')
        args = []
        for (node_id, domain, size), count \
                in bytes_per_domain_per_flow.iteritems():
            args.append((node_id, domain, size, count))
        self._conn.executemany('''INSERT INTO bytes_per_domain_per_flow
                                  (node_id, domain, bytes, count)
                                  VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_packets_per_domain_per_flow(self, packets_per_domain_per_flow):
        self._conn.execute('''CREATE TABLE IF NOT EXISTS packets_per_domain_per_flow (
                                  node_id text NOT NULL,
                                  domain text NOT NULL,
                                  packets integer NOT NULL,
                                  count integer NOT NULL,
                                  UNIQUE (node_id, domain, packets)
                              )''')
        self._conn.execute('DELETE FROM packets_per_domain_per_flow')
        args = []
        for (node_id, domain, size), count \
                in packets_per_domain_per_flow.iteritems():
            args.append((node_id, domain, size, count))
        self._conn.executemany('''INSERT INTO packets_per_domain_per_flow
                                  (node_id, domain, packets, count)
                                  VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_seconds_per_domain_per_flow(self, seconds_per_domain_per_flow):
        self._conn.execute('''CREATE TABLE IF NOT EXISTS seconds_per_domain_per_flow (
                                  node_id text NOT NULL,
                                  domain text NOT NULL,
                                  seconds integer NOT NULL,
                                  count integer NOT NULL,
                                  UNIQUE (node_id, domain, seconds)
                              )''')
        self._conn.execute('DELETE FROM seconds_per_domain_per_flow')
        args = []
        for (node_id, domain, duration), count \
                in seconds_per_domain_per_flow.iteritems():
            args.append((node_id, domain, duration, count))
        self._conn.executemany('''INSERT INTO seconds_per_domain_per_flow
                                  (node_id, domain, seconds, count)
                                  VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_bytes_per_device_per_flow(self, bytes_per_device_per_flow):
        self._conn.execute('''CREATE TABLE IF NOT EXISTS bytes_per_device_per_flow (
                                  node_id text NOT NULL,
                                  device text NOT NULL,
                                  bytes integer NOT NULL,
                                  count integer NOT NULL,
                                  UNIQUE (node_id, device, bytes)
                              )''')
        self._conn.execute('DELETE FROM bytes_per_device_per_flow')
        args = []
        for (node_id, device, size), count \
                in bytes_per_device_per_flow.iteritems():
            args.append((node_id, device, size, count))
        self._conn.executemany('''INSERT INTO bytes_per_device_per_flow
                                  (node_id, device, bytes, count)
                                  VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_packets_per_device_per_flow(self, packets_per_device_per_flow):
        self._conn.execute('''CREATE TABLE IF NOT EXISTS packets_per_device_per_flow (
                                  node_id text NOT NULL,
                                  device text NOT NULL,
                                  packets integer NOT NULL,
                                  count integer NOT NULL,
                                  UNIQUE (node_id, device, packets)
                              )''')
        self._conn.execute('DELETE FROM packets_per_device_per_flow')
        args = []
        for (node_id, device, size), count \
                in packets_per_device_per_flow.iteritems():
            args.append((node_id, device, size, count))
        self._conn.executemany('''INSERT INTO packets_per_device_per_flow
                                  (node_id, device, packets, count)
                                  VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_seconds_per_device_per_flow(self, seconds_per_device_per_flow):
        self._conn.execute('''CREATE TABLE IF NOT EXISTS seconds_per_device_per_flow (
                                  node_id text NOT NULL,
                                  device text NOT NULL,
                                  seconds integer NOT NULL,
                                  count integer NOT NULL,
                                  UNIQUE (node_id, device, seconds)
                              )''')
        self._conn.execute('DELETE FROM seconds_per_device_per_flow')
        args = []
        for (node_id, device, duration), count \
                in seconds_per_device_per_flow.iteritems():
            args.append((node_id, device, duration, count))
        self._conn.executemany('''INSERT INTO seconds_per_device_per_flow
                                  (node_id, device, seconds, count)
                                  VALUES (?, ?, ?, ?)''', args)
        self._conn.commit()

    def import_update_statistics(self, update_statistics, oldest_timestamps):
        self._conn.execute('''CREATE TABLE IF NOT EXISTS update_statistics (
                                  node_id text NOT NULL,
                                  eventstamp integer NOT NULL,
                                  file_format_version integer NOT NULL,
                                  pcap_dropped integer NOT NULL,
                                  iface_dropped integer NOT NULL,
                                  packet_series_dropped integer NOT NULL,
                                  flow_table_dropped integer NOT NULL,
                                  dropped_a_records integer NOT NULL,
                                  dropped_cname_records integer NOT NULL,
                                  packet_series_size integer NOT NULL,
                                  flow_table_size integer NOT NULL,
                                  a_records_size integer NOT NULL,
                                  cname_records_size integer NOT NULL,
                                  UNIQUE (node_id, eventstamp)
                              )''')
        args = []
        for (node_id, eventstamp), statistics in update_statistics.iteritems():
            if eventstamp >= oldest_timestamps[node_id]:
                args.append(
                        (node_id, eventstamp,
                            statistics.file_format_version,
                            statistics.pcap_dropped,
                            statistics.iface_dropped,
                            statistics.packet_series_dropped,
                            statistics.flow_table_dropped,
                            statistics.dropped_a_records,
                            statistics.dropped_cname_records,
                            statistics.packet_series_size,
                            statistics.flow_table_size,
                            statistics.a_records_size,
                            statistics.cname_records_size))
        self._conn.executemany('''INSERT OR REPLACE INTO update_statistics
                                  (node_id,
                                   eventstamp,
                                   file_format_version,
                                   pcap_dropped,
                                   iface_dropped,
                                   packet_series_dropped,
                                   flow_table_dropped,
                                   dropped_a_records,
                                   dropped_cname_records,
                                   packet_series_size,
                                   flow_table_size,
                                   a_records_size,
                                   cname_records_size)
                                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                              args)
        self._conn.commit()
