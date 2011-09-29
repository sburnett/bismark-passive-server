import psycopg2
import psycopg2.extensions

class BismarkPassiveDatabase(object):
    UNANONYMIZED_SIGNATURE = ''

    def __init__(self, user, database):
        self._conn = psycopg2.connect(user=user, database=database)
        cur = self._conn.cursor()
        cur.execute('SET search_path TO passive')
        cur.close()
        self._conn.commit()

    def _execute_command(self, command, args):
        cur = self._conn.cursor()
        cur.execute('SELECT %s;' % command, args)
        result = cur.fetchone()[0]
        cur.close()
        return result

    def _merge_node(self, *args):
        return self._execute_command('merge_node (%s)', args)
    def _merge_whitelisted_domain(self, *args):
        return self._execute_command('merge_whitelisted_domain (%s, %s)', args)
    def _merge_anonymization_context(self, *args):
        return self._execute_command(
                'merge_anonymization_context (%s, %s)', args)
    def _merge_session(self, *args):
        return self._execute_command('merge_session (%s, %s)', args)
    def _merge_update(self, *args):
        return self._execute_command(
                'merge_update (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                args)
    def _merge_mac_address(self, *args):
        return self._execute_command('merge_mac_address (%s, %s)', args)
    def _merge_ip_address(self, *args):
        return self._execute_command('merge_ip_address (%s, %s)', args)
    def _merge_domain_name(self, *args):
        return self._execute_command('merge_domain_name (%s, %s)', args)
    def _merge_local_address(self, *args):
        return self._execute_command(
                'merge_local_address (%s, %s, %s, %s)', args)
    def _merge_dns_a_record(self, *args):
        return self._execute_command(
                'merge_dns_a_record (%s, %s, %s, %s)', args)
    def _merge_dns_cname_record(self, *args):
        return self._execute_command(
                'merge_dns_cname_record (%s, %s, %s, %s)', args)
    def _merge_flow(self, *args):
        return self._execute_command(
                'merge_flow (%s, %s, %s, %s, %s, %s, %s)', args)
    def _merge_packet(self, *args):
        return self._execute_command(
                'merge_packet (%s, %s, timestamp %s, %s)', args)
    def _lookup_local_address_for_session(self, *args):
        return self._execute_command(
                'lookup_local_address_for_session (%s, %s)', args)
    def _lookup_flow_for_session(self, *args):
        return self._execute_command('lookup_flow_for_session (%s, %s)', args)
    def _check_out_of_order(self, *args):
        return self._execute_command('count_out_of_order (%s, %s)', args)
    def _compute_histograms(self, *args):
        return self._execute_command('compute_histograms ()', args)

    def import_update(self, parsed_update):
        node_id = self._merge_node(parsed_update.bismark_id)
        if parsed_update.anonymized:
            signature = parsed_update.anonymization_signature
        else:
            signature = self.UNANONYMIZED_SIGNATURE
        anonymization_context_id = self._merge_anonymization_context(node_id,
                                                                     signature)
        unanonymized_context_id = self._merge_anonymization_context(
                node_id,
                self.UNANONYMIZED_SIGNATURE)
        session_id = self._merge_session(anonymization_context_id,
                                         str(parsed_update.creation_time))
        if self._check_out_of_order(session_id,
                                    parsed_update.sequence_number) > 0:
            raise ValueError('Attempting to process an old update!')
        (pcap_received, pcap_dropped, iface_dropped) = (-1, -1, -1)
        try:
            pcap_received = parsed_update.pcap_received
            pcap_dropped = parsed_update.pcap_dropped
            iface_dropped = parsed_update.iface_dropped
        except:
            pass
        update_id = self._merge_update(session_id,
                                       parsed_update.sequence_number,
                                       parsed_update.packet_series_dropped,
                                       parsed_update.flow_table_size,
                                       parsed_update.flow_table_expired,
                                       parsed_update.flow_table_dropped,
                                       parsed_update.dropped_a_records,
                                       parsed_update.dropped_cname_records,
                                       parsed_update.pcap_received,
                                       parsed_update.pcap_dropped,
                                       parsed_update.iface_dropped)
        self._conn.commit()

        for domain in parsed_update.whitelist:
            self._merge_whitelisted_domain(session_id, domain)

        for idx, entry in enumerate(parsed_update.addresses):
            mac_address_id = self._merge_mac_address(anonymization_context_id,
                                                     entry.mac_address)
            ip_address_id = self._merge_ip_address(anonymization_context_id,
                                                   entry.ip_address)
            remote_id = (parsed_update.address_table_first_id + idx) \
                    % parsed_update.address_table_size
            local_address_id = self._merge_local_address(remote_id,
                                                         update_id,
                                                         mac_address_id,
                                                         ip_address_id)

        for entry in parsed_update.a_records:
            if entry.anonymized:
                context_id = anonymization_context_id
            else:
                context_id = unanonymized_context_id
            local_address_id = self._lookup_local_address_for_session(
                    session_id,
                    entry.address_id)
            domain_name_id = self._merge_domain_name(context_id, entry.domain)
            ip_address_id = self._merge_ip_address(context_id, entry.ip_address)
            self._merge_dns_a_record(update_id,
                                     local_address_id,
                                     domain_name_id,
                                     ip_address_id)

        for entry in parsed_update.cname_records:
            if entry.anonymized:
                context_id = anonymization_context_id
            else:
                context_id = unanonymized_context_id
            local_address_id = self._lookup_local_address_for_session(
                    session_id,
                    entry.address_id)
            domain_name_id = self._merge_domain_name(context_id, entry.domain)
            cname_id = self._merge_domain_name(context_id, entry.cname)
            self._merge_dns_cname_record(update_id,
                                         local_address_id,
                                         domain_name_id,
                                         cname_id)

        for entry in parsed_update.flow_table:
            if entry.source_ip_anonymized:
                source_context_id = anonymization_context_id
            else:
                source_context_id = unanonymized_context_id
            source_ip_id = self._merge_ip_address(source_context_id,
                                                  entry.source_ip)
            if entry.destination_ip_anonymized:
                destination_context_id = anonymization_context_id
            else:
                destination_context_id = unanonymized_context_id
            destination_ip_id = self._merge_ip_address(destination_context_id,
                                                       entry.destination_ip)
            self._merge_flow(entry.flow_id,
                             update_id,
                             source_ip_id,
                             destination_ip_id,
                             entry.transport_protocol,
                             entry.source_port,
                             entry.destination_port)

        for entry in parsed_update.packet_series:
            flow_id = self._lookup_flow_for_session(session_id, entry.flow_id)
            self._merge_packet(update_id,
                               flow_id,
                               psycopg2.TimestampFromTicks(entry.timestamp / 1e6),
                               entry.size)

        self._conn.commit()

    def compute_histograms(self):
        self._compute_histograms()
