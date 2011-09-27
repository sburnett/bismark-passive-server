import psycopg2

class BismarkPassiveDatabase(object):
    UNANONYMIZED_SIGNATURE = ''

    merge_node_text = 'SELECT merge_node (%s)'
    merge_anonymization_context_text = \
            'SELECT merge_anonymization_context (%s, %s)'
    merge_session_text = 'SELECT merge_session (%s, %s)'
    merge_update_text = \
            'SELECT merge_update (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    merge_mac_address_text = 'SELECT merge_mac_address (%s, %s)'
    merge_ip_address_text = 'SELECT merge_ip_address (%s, %s)'
    merge_domain_name_text = 'SELECT merge_domain_name (%s, %s)'
    merge_local_address_text = 'SELECT merge_local_address (%s, %s, %s, %s)'
    merge_dns_a_record_text = 'SELECT merge_dns_a_record (%s, %s, %s, %s)'
    merge_dns_cname_record_text = \
            'SELECT merge_dns_cname_record (%s, %s, %s, %s)'
    merge_flow_text = 'SELECT merge_flow (%s, %s, %s, %s, %s, %s, %s)'
    merge_packet_text = 'SELECT merge_packet (%s, %s, timestamp %s, %s)'
    lookup_local_address_for_session = \
            'SELECT lookup_local_address_for_session (%s, %s)'
    lookup_flow_for_session = 'SELECT lookup_flow_for_session (%s, %s)'
    check_out_of_order_text = 'SELECT count_out_of_order (%s, %s)'
    compute_histograms_text = 'SELECT compute_histograms ()'

    def __init__(self, user, database):
        self._conn = psycopg2.connect(user=user, database=database)

    def _execute_command(self, command, *args):
        cur = self._conn.cursor()
        cur.execute(command, args)
        result = cur.fetchone()[0]
        cur.close()
        return result

    def import_update(self, parsed_update):
        node_id = self._execute_command(self.merge_node_text,
                                        parsed_update.bismark_id)
        if parsed_update.anonymized:
            signature = parsed_update.anonymization_signature
        else:
            signature = self.UNANONYMIZED_SIGNATURE
        anonymization_context_id = self._execute_command(
                self.merge_anonymization_context_text,
                node_id,
                signature)
        unanonymized_context_id = self._execute_command(
                self.merge_anonymization_context_text,
                node_id,
                self.UNANONYMIZED_SIGNATURE)
        session_id = self._execute_command(
                self.merge_session_text,
                anonymization_context_id,
                str(parsed_update.creation_time))
        if self._execute_command(self.check_out_of_order_text,
                                 session_id,
                                 parsed_update.sequence_number) > 0:
            raise ValueError('Attempting to process an old update!')
        (pcap_received, pcap_dropped, iface_dropped) = (-1, -1, -1)
        try:
            pcap_received = parsed_update.pcap_received
            pcap_dropped = parsed_update.pcap_dropped
            iface_dropped = parsed_update.iface_dropped
        except:
            pass
        update_id = self._execute_command(
                self.merge_update_text,
                session_id,
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

        for idx, entry in enumerate(parsed_update.addresses):
            mac_address_id = self._execute_command(self.merge_mac_address_text,
                                                   anonymization_context_id,
                                                   entry.mac_address)
            ip_address_id = self._execute_command(self.merge_ip_address_text,
                                                  anonymization_context_id,
                                                  entry.ip_address)
            remote_id = (parsed_update.address_table_first_id + idx) \
                    % parsed_update.address_table_size
            local_address_id = self._execute_command(
                    self.merge_local_address_text,
                    remote_id,
                    update_id,
                    mac_address_id,
                    ip_address_id)

        for entry in parsed_update.a_records:
            if entry.anonymized:
                context_id = anonymization_context_id
            else:
                context_id = unanonymized_context_id
            local_address_id = self._execute_command(
                    self.lookup_local_address_for_session,
                    session_id,
                    entry.address_id)
            domain_name_id = self._execute_command(self.merge_domain_name_text,
                                                   context_id,
                                                   entry.domain)
            ip_address_id = self._execute_command(self.merge_ip_address_text,
                                                  context_id,
                                                  entry.ip_address)
            self._execute_command(self.merge_dns_a_record_text,
                                  update_id,
                                  local_address_id,
                                  domain_name_id,
                                  ip_address_id)

        for entry in parsed_update.cname_records:
            if entry.anonymized:
                context_id = anonymization_context_id
            else:
                context_id = unanonymized_context_id
            local_address_id = self._execute_command(
                    self.lookup_local_address_for_session,
                    session_id,
                    entry.address_id)
            domain_name_id = self._execute_command(self.merge_domain_name_text,
                                                   context_id,
                                                   entry.domain)
            cname_id = self._execute_command(self.merge_domain_name_text,
                                             context_id,
                                             entry.cname)
            self._execute_command(self.merge_dns_cname_record_text,
                                  update_id,
                                  local_address_id,
                                  domain_name_id,
                                  cname_id)

        for entry in parsed_update.flow_table:
            if entry.source_ip_anonymized:
                source_context_id = anonymization_context_id
            else:
                source_context_id = unanonymized_context_id
            source_ip_id = self._execute_command(self.merge_ip_address_text,
                                                 source_context_id,
                                                 entry.source_ip)
            if entry.destination_ip_anonymized:
                destination_context_id = anonymization_context_id
            else:
                destination_context_id = unanonymized_context_id
            destination_ip_id = self._execute_command(
                    self.merge_ip_address_text,
                    destination_context_id,
                    entry.destination_ip)
            self._execute_command(self.merge_flow_text,
                                  entry.flow_id,
                                  update_id,
                                  source_ip_id,
                                  destination_ip_id,
                                  entry.transport_protocol,
                                  entry.source_port,
                                  entry.destination_port)

        for entry in parsed_update.packet_series:
            flow_id = self._execute_command(self.lookup_flow_for_session,
                                            session_id,
                                            entry.flow_id)
            self._execute_command(self.merge_packet_text,
                                  update_id,
                                  flow_id,
                                  psycopg2.TimestampFromTicks(entry.timestamp / 1e6),
                                  entry.size)
        self._conn.commit()

    def compute_histograms(self):
        self._execute_command(self.compute_histograms_text)
