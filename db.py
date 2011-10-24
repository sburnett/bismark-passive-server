import psycopg2

class BismarkPassiveDatabase(object):
    def __init__(self, user, database):
        self._conn = psycopg2.connect(user=user, database=database)
        cur = self._conn.cursor()
        cur.execute('SET search_path TO bismark_passive')
        cur.close()
        self._conn.commit()

    def import_byte_statistics(self,
                               node_id,
                               bytes_per_minute,
                               bytes_per_port_per_minute,
                               bytes_per_domain_per_minute,
                               bytes_per_device_per_minute,
                               bytes_per_device_per_port_per_minute,
                               bytes_per_device_per_domain_per_minute):
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

        cur.execute(
                'DELETE FROM bytes_per_device_per_minute WHERE node_id = %s',
                (node_id, ));
        bytes_per_device_per_minute_args = []
        for (rounded_timestamp, mac_address), size in \
                bytes_per_device_per_minute.items():
            bytes_per_device_per_minute_args.append(
                    (node_id, rounded_timestamp, mac_address, size))
        cur.executemany('''INSERT INTO bytes_per_device_per_minute
                           (node_id, timestamp, mac_address, bytes_transferred)
                           VALUES (%s, %s, %s, %s)''',
                        bytes_per_device_per_minute_args)

        cur.execute(
                '''DELETE FROM bytes_per_device_per_port_per_minute
                   WHERE node_id = %s''',
                (node_id, ));
        bytes_per_device_per_port_per_minute_args = []
        for (rounded_timestamp, mac_address, port), size in \
                bytes_per_device_per_port_per_minute.items():
            bytes_per_device_per_port_per_minute_args.append(
                    (node_id, rounded_timestamp, mac_address, port, size))
        cur.executemany('''INSERT INTO bytes_per_device_per_port_per_minute
                           (node_id, timestamp, mac_address, port, bytes_transferred)
                           VALUES (%s, %s, %s, %s, %s)''',
                        bytes_per_device_per_port_per_minute_args)

        cur.execute(
                '''DELETE FROM bytes_per_device_per_domain_per_minute
                   WHERE node_id = %s''',
                (node_id, ));
        bytes_per_device_per_domain_per_minute_args = []
        for (rounded_timestamp, mac_address, domain), size in \
                bytes_per_device_per_domain_per_minute.items():
            bytes_per_device_per_domain_per_minute_args.append(
                    (node_id, rounded_timestamp, mac_address, domain, size))
        cur.executemany('''INSERT INTO bytes_per_device_per_domain_per_minute
                           (node_id, timestamp, mac_address, domain, bytes_transferred)
                           VALUES (%s, %s, %s, %s, %s)''',
                        bytes_per_device_per_domain_per_minute_args)

        self._conn.commit()

    def import_update_statistics(self,
                                 node_id,
                                 update_statistics):
        cur = self._conn.cursor()
        cur.execute('DELETE FROM update_statistics WHERE node_id = %s',
                    (node_id, ))
        update_statistics_args = []
        for timestamp, statistics in update_statistics.items():
            update_statistics_args.append(
                    (node_id, timestamp,
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
        cur.executemany('''INSERT INTO update_statistics
                           (node_id, timestamp, pcap_dropped, iface_dropped,
                            packet_series_dropped, flow_table_dropped,
                            dropped_a_records, dropped_cname_records,
                            packet_series_size, flow_table_size,
                            a_records_size, cname_records_size)
                           VALUES
                           (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                        update_statistics_args)

        self._conn.commit()

