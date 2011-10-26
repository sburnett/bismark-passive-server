import psycopg2
import psycopg2.extensions

class BismarkPassiveDatabase(object):
    def __init__(self, user, database):
        self._conn = psycopg2.connect(user=user, database=database)
        cur = self._conn.cursor()
        cur.execute('SET search_path TO bismark_passive')
        cur.close()
        self._conn.commit()

    def import_node_byte_statistics(self,
                                    node_id,
                                    oldest_updated_timestamp,
                                    bytes_per_minute,
                                    bytes_per_port_per_minute,
                                    bytes_per_domain_per_minute):
        cur = self._conn.cursor()

        bytes_per_minute_args = []
        for rounded_eventstamp, size in bytes_per_minute.items():
            if rounded_eventstamp >= oldest_updated_timestamp:
                bytes_per_minute_args.append((node_id,
                                              rounded_eventstamp,
                                              size))
        cur.executemany('SELECT merge_bytes_per_minute(%s, %s, %s)',
                        bytes_per_minute_args)

        bytes_per_port_per_minute_args = []
        for (rounded_eventstamp, port), size in \
                bytes_per_port_per_minute.items():
            if rounded_eventstamp >= oldest_updated_timestamp:
                bytes_per_port_per_minute_args.append((node_id,
                                                       rounded_eventstamp,
                                                       port,
                                                       size))
        cur.executemany(
                'SELECT merge_bytes_per_port_per_minute(%s, %s, %s, %s)',
                bytes_per_port_per_minute_args)

        bytes_per_domain_per_minute_args = []
        for (rounded_eventstamp, domain), size in \
                bytes_per_domain_per_minute.items():
            if rounded_eventstamp >= oldest_updated_timestamp:
                bytes_per_domain_per_minute_args.append((node_id,
                                                         rounded_eventstamp,
                                                         domain,
                                                         size))
        cur.executemany(
                'SELECT merge_bytes_per_domain_per_minute(%s, %s, %s, %s)',
                bytes_per_domain_per_minute_args)

        cur.execute('SELECT refresh_matviews_node_latest(%s, %s)',
                    (node_id, oldest_updated_timestamp))

        self._conn.commit()

    def import_context_byte_statistics(self,
                                       node_id,
                                       anonymization_context,
                                       oldest_updated_timestamp,
                                       bytes_per_device_per_minute,
                                       bytes_per_device_per_port_per_minute,
                                       bytes_per_device_per_domain_per_minute):
        cur = self._conn.cursor()

        bytes_per_device_per_minute_args = []
        for (rounded_eventstamp, mac_address), size in \
                bytes_per_device_per_minute.items():
            if rounded_eventstamp >= oldest_updated_timestamp: 
                bytes_per_device_per_minute_args.append((node_id,
                                                         anonymization_context,
                                                         rounded_eventstamp,
                                                         mac_address,
                                                         size))
        cur.executemany(
                'SELECT merge_bytes_per_device_per_minute(%s, %s, %s, %s, %s)',
                bytes_per_device_per_minute_args)

        bytes_per_device_per_port_per_minute_args = []
        for (rounded_eventstamp, mac_address, port), size in \
                bytes_per_device_per_port_per_minute.items():
            if rounded_eventstamp >= oldest_updated_timestamp:
                bytes_per_device_per_port_per_minute_args.append(
                        (node_id,
                         anonymization_context,
                         rounded_eventstamp,
                         mac_address,
                         port,
                         size))
        cur.executemany('''SELECT merge_bytes_per_device_per_port_per_minute
                           (%s, %s, %s, %s, %s, %s)''',
                        bytes_per_device_per_port_per_minute_args)

        bytes_per_device_per_domain_per_minute_args = []
        for (rounded_eventstamp, mac_address, domain), size in \
                bytes_per_device_per_domain_per_minute.items():
            if rounded_eventstamp >= oldest_updated_timestamp:
                bytes_per_device_per_domain_per_minute_args.append(
                        (node_id,
                         anonymization_context,
                         rounded_eventstamp,
                         mac_address,
                         domain,
                         size))
        cur.executemany('''SELECT merge_bytes_per_device_per_domain_per_minute
                           (%s, %s, %s, %s, %s, %s)''',
                        bytes_per_device_per_domain_per_minute_args)

        cur.execute('SELECT refresh_matviews_context_latest(%s, %s, %s)',
                    (node_id, anonymization_context, oldest_updated_timestamp))

        self._conn.commit()

    def import_update_statistics(self,
                                 node_id,
                                 oldest_timestamp,
                                 update_statistics):
        cur = self._conn.cursor()
        update_statistics_args = []
        for eventstamp, statistics in update_statistics.items():
            if eventstamp >= oldest_timestamp:
                update_statistics_args.append(
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
                        update_statistics_args)

        self._conn.commit()
