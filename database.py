import psycopg2
import psycopg2.extensions

class BismarkPassiveDatabase(object):
    def __init__(self, user, database):
        self._conn = psycopg2.connect(user=user, database=database)
        cur = self._conn.cursor()
        cur.execute('SET search_path TO bismark_passive')
        cur.close()
        self._conn.commit()

    def import_bytes_per_minute(self, data, oldest_timestamps):
        args = []
        for (node_id, eventstamp), size in data.iteritems():
            if eventstamp >= oldest_timestamps[node_id]:
                args.append((node_id, eventstamp, size))
        cur = self._conn.cursor()
        cur.executemany('SELECT merge_bytes_per_minute(%s, %s, %s)', args)
        self._conn.commit()

    def import_bytes_per_port_per_minute(self, data, oldest_timestamps):
        args = []
        for (node_id, eventstamp, port), size in data.iteritems():
            if eventstamp >= oldest_timestamps[node_id]:
                args.append((node_id, eventstamp, port, size))
        cur = self._conn.cursor()
        cur.executemany('''SELECT merge_bytes_per_port_per_minute
                           (%s, %s, %s, %s)''',
                        args)
        self._conn.commit()

    def import_bytes_per_domain_per_minute(self, data, oldest_timestamps):
        args = []
        for (node_id, eventstamp, domain), size in data.iteritems():
            if eventstamp >= oldest_timestamps[node_id]:
                args.append((node_id, eventstamp, domain, size))
        cur = self._conn.cursor()
        cur.executemany('''SELECT merge_bytes_per_domain_per_minute
                           (%s, %s, %s, %s)''',
                        args)
        self._conn.commit()

    def import_bytes_per_device_per_minute(self, data, oldest_timestamps):
        args = []
        for (node_id, anonymization_id, eventstamp, mac_address), size \
                in data.iteritems():
            if eventstamp >= oldest_timestamps[node_id, anonymization_id]:
                args.append((node_id,
                             anonymization_id,
                             eventstamp,
                             mac_address,
                             size))
        cur = self._conn.cursor()
        cur.executemany('''SELECT merge_bytes_per_device_per_minute
                           (%s, %s, %s, %s, %s)''',
                        args)
        self._conn.commit()

    def import_bytes_per_device_per_port_per_minute(self,
                                                    data,
                                                    oldest_timestamps):
        args = []
        for (node_id, anonymization_id, eventstamp, mac_address, port), size \
                in data.iteritems():
            if eventstamp >= oldest_timestamps[node_id, anonymization_id]:
                args.append((node_id,
                             anonymization_id,
                             eventstamp,
                             mac_address,
                             port,
                             size))
        cur = self._conn.cursor()
        cur.executemany('''SELECT merge_bytes_per_device_per_port_per_minute
                           (%s, %s, %s, %s, %s, %s)''',
                        args)
        self._conn.commit()
                            
    def import_bytes_per_device_per_domain_per_minute(self,
                                                      data,
                                                      oldest_timestamps):
        args = []
        for (node_id, anonymization_id, eventstamp, mac_address, domain), size \
                in data.iteritems():
            if eventstamp >= oldest_timestamps[node_id, anonymization_id]:
                args.append((node_id,
                             anonymization_id,
                             eventstamp,
                             mac_address,
                             domain,
                             size))
        cur = self._conn.cursor()
        cur.executemany('''SELECT merge_bytes_per_device_per_domain_per_minute
                           (%s, %s, %s, %s, %s, %s)''',
                        args)
        self._conn.commit()

    def refresh_matviews(self, oldest_timestamps):
        cur = self._conn.cursor()
        for key, oldest_timestamp in oldest_timestamps.iteritems():
            if isinstance(key, tuple) and len(key) == 2:
                (node_id, anonymization_id) = key
                cur.callproc('refresh_matviews_context_latest',
                             (node_id, anonymization_id, oldest_timestamp))
            elif not isinstance(key, tuple):
                cur.callproc('refresh_matviews_node_latest',
                             (key, oldest_timestamp))
            else:
                raise ValueError('Invalid key', key)
        self._conn.commit()

    def import_size_statistics(self,
                               packet_size_per_port_tcp,
                               packet_size_per_port_udp):
        cur = self._conn.cursor()
        args = []
        for (node_id, port, size), (inbound, outbound) \
                in packet_size_per_port_tcp.iteritems():
            args.append((node_id, port, 'tcp', size, inbound, outbound))
        for (node_id, port, size), (inbound, outbound) \
                in packet_size_per_port_udp.iteritems():
            args.append((node_id, port, 'udp', size, inbound, outbound))
        cur.executemany(
                'SELECT merge_packet_size_per_port(%s, %s, %s, %s, %s, %s)',
                args)
        self._conn.commit()

    def import_update_statistics(self, update_statistics, oldest_timestamps):
        cur = self._conn.cursor()
        args = []
        for (node_id, eventstamp), statistics in update_statistics.iteritems():
            if eventstamp >= oldest_timestamps[node_id]:
                args.append(
                        (node_id, eventstamp,
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
        cur.executemany('''SELECT merge_update_statistics
                           (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                        args)
        self._conn.commit()

    def import_bytes_per_ip(self, bytes_per_ip):
        cur = self._conn.cursor()
        args = []
        for (node_id, anonymization_id, ip), count in bytes_per_ip.iteritems():
            args.append((node_id, anonymization_id, ip, count))
        cur.executemany('SELECT merge_bytes_per_ip (%s, %s, %s, %s)', args)
        self._conn.commit()

    def import_packets_per_ip(self, packets_per_ip):
        cur = self._conn.cursor()
        args = []
        for (node_id, anonymization_id, ip), count \
                in packets_per_ip.iteritems():
            args.append((node_id, anonymization_id, ip, count))
        cur.executemany('SELECT merge_packets_per_ip (%s, %s, %s, %s)', args)
        self._conn.commit()

    def import_bytes_per_flow(self, bytes_per_flow):
        cur = self._conn.cursor()
        args = []
        cur.execute('DELETE FROM bytes_per_flow')
        for (node_id, size), count in bytes_per_flow.iteritems():
            args.append((node_id, size, count))
        cur.executemany('''INSERT INTO bytes_per_flow
                           (node_id, bytes, count)
                           VALUES (%s, %s, %s)''', args)
        self._conn.commit()

    def import_packets_per_flow(self, packets_per_flow):
        cur = self._conn.cursor()
        args = []
        cur.execute('DELETE FROM packets_per_flow')
        for (node_id, size), count in packets_per_flow.iteritems():
            args.append((node_id, size, count))
        cur.executemany('''INSERT INTO packets_per_flow
                           (node_id, packets, count)
                           VALUES (%s, %s, %s)''', args)
        self._conn.commit()

    def import_seconds_per_flow(self, seconds_per_flow):
        cur = self._conn.cursor()
        args = []
        cur.execute('DELETE FROM seconds_per_flow')
        for (node_id, size), count in seconds_per_flow.iteritems():
            args.append((node_id, size, count))
        cur.executemany('''INSERT INTO seconds_per_flow
                           (node_id, seconds, count)
                           VALUES (%s, %s, %s)''', args)
        self._conn.commit()

    def import_bytes_per_port_per_flow(self, bytes_per_port_per_flow):
        cur = self._conn.cursor()
        args = []
        cur.execute('DELETE FROM bytes_per_port_per_flow')
        for (node_id, port, size), count in bytes_per_port_per_flow.iteritems():
            args.append((node_id, port, size, count))
        cur.executemany('''INSERT INTO bytes_per_port_per_flow
                           (node_id, port, bytes, count)
                           VALUES (%s, %s, %s, %s)''', args)
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
                           VALUES (%s, %s, %s, %s)''', args)
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
                           VALUES (%s, %s, %s, %s)''', args)
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
                           VALUES (%s, %s, %s, %s)''', args)
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
                           VALUES (%s, %s, %s, %s)''', args)
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
                           VALUES (%s, %s, %s, %s)''', args)
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
                           VALUES (%s, %s, %s, %s)''', args)
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
                           VALUES (%s, %s, %s, %s)''', args)
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
                           VALUES (%s, %s, %s, %s)''', args)
        self._conn.commit()
