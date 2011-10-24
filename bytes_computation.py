from collections import defaultdict, namedtuple
import re

import db
from session_computations import \
        SessionProcessor, SessionAggregator, merge_timeseries

class BytesSessionProcessor(SessionProcessor):
    def __init__(self):
        super(BytesSessionProcessor, self).__init__()

        self._bytes_per_minute = defaultdict(int)
        self._bytes_per_port_per_minute = defaultdict(int)
        self._bytes_per_domain_per_minute = defaultdict(int)
        self._bytes_per_device_per_minute = defaultdict(int)
        self._bytes_per_device_per_port_per_minute = defaultdict(int)
        self._bytes_per_device_per_domain_per_minute = defaultdict(int)

        self._whitelist = set()
        self._address_map = {}
        self._mac_address_map = {}
        self._flows = {}
        self._flow_ip_map = defaultdict(set)
        self._dns_map_ip = defaultdict(set)
        self._dns_a_map_domain = defaultdict(list)

    def process_flow(self, flow):
        self._flows[flow.flow_id] = flow
        if flow.source_ip in self._address_map \
                and not flow.destination_ip_anonymized:
            key = (self._address_map[flow.source_ip], flow.destination_ip)
        elif flow.destination_ip in self._address_map \
                and not flow.source_ip_anonymized:
            key = (self._address_map[flow.destination_ip], flow.source_ip)
        else:
            return
        self._flow_ip_map[key]
        if key in self._dns_map_ip:
            self._flow_ip_map[key].update(self._dns_map_ip[key])

    def process_a_record(self, a_record, a_packet):
        if a_record.anonymized:
            return
        domain_key = (a_record.address_id, a_record.domain)
        self._dns_a_map_domain[domain_key].append(a_record)
        for domain, pattern in self._whitelist:
            if pattern.search(a_record.domain) is not None:
                ip_key = (a_record.address_id, a_record.ip_address)
                domain_record = (domain,
                                 a_packet.timestamp,
                                 a_packet.timestamp + a_record.ttl)
                self._dns_map_ip[ip_key].add(domain_record)
                if ip_key in self._flow_ip_map:
                    self._flow_ip_map[ip_key].add(domain_record)

    def process_cname_record(self, cname_record, packet_series):
        if cname_record.anonymized:
            return
        try:
            cname_packet = packet_series[cname_record.packet_id]
        except IndexError:
            return
        domain_key = (cname_record.address_id, cname_record.cname)
        a_records = self._dns_a_map_domain.get(domain_key)
        if a_records is None:
            return
        for domain, pattern in self._whitelist:
            if pattern.search(cname_record.domain) is not None:
                for a_record in a_records:
                    try:
                        a_packet = packet_series[a_record.packet_id]
                    except IndexError:
                        continue
                    start_timestamp = max(cname_packet.timestamp,
                                          a_packet.timestamp)
                    end_timestamp = min(
                            cname_packet.timestamp + cname_record.ttl,
                            a_packet.timestamp + a_record.ttl)
                    if start_timestamp > end_timestamp:
                        continue
                    ip_key = (a_record.address_id, a_record.ip_address)
                    domain_record = (domain, start_timestamp, end_timestamp)
                    self._dns_map_ip[ip_key].add(domain_record)
                    if ip_key in self._flow_ip_map:
                        self._flow_ip_map[ip_key].add(domain_record)

    def process_packet(self, packet):
        rounded_timestamp = packet.timestamp.replace(second=0, microsecond=0)
        self._bytes_per_minute[rounded_timestamp] += packet.size

        flow = self._flows.get(packet.flow_id)
        if flow is not None:
            port_key = []
            if flow.source_ip in self._address_map \
                    and flow.destination_ip not in self._address_map:
                port_key = (rounded_timestamp, flow.destination_port)
            elif flow.destination_ip in self._address_map \
                    and flow.source_ip not in self._address_map:
                port_key = (rounded_timestamp, flow.source_port)
            else:
                port_key = (rounded_timestamp, -1)
            self._bytes_per_port_per_minute[port_key] += packet.size

            device_keys = []
            if flow.source_ip in self._mac_address_map:
                device_keys.append((rounded_timestamp,
                                    self._mac_address_map[flow.source_ip]))
            if flow.destination_ip in self._mac_address_map:
                device_keys.append(
                        (rounded_timestamp,
                         self._mac_address_map[flow.destination_ip]))
            if device_keys == []:
                device_keys = [(rounded_timestamp, 'unknown')]
            for device_key in device_keys:
                self._bytes_per_device_per_minute[device_key] += packet.size

            for device_key in device_keys:
                device_port_key = (rounded_timestamp,
                                   device_key[1],
                                   port_key[1])
                self._bytes_per_device_per_port_per_minute[device_port_key] \
                        += packet.size

            key = None
            if flow.source_ip in self._address_map \
                    and not flow.destination_ip_anonymized:
                key = (self._address_map[flow.source_ip], flow.destination_ip)
            elif flow.destination_ip in self._address_map \
                    and not flow.source_ip_anonymized:
                key = (self._address_map[flow.destination_ip], flow.source_ip)
            if key is not None and key in self._flow_ip_map:
                for domain, start_time, end_time in self._flow_ip_map[key]:
                    if packet.timestamp < start_time \
                            or packet.timestamp > end_time:
                        continue
                    domain_key = (rounded_timestamp, domain)
                    self._bytes_per_domain_per_minute[domain_key] \
                            += packet.size
                    device_domain_key = (rounded_timestamp,
                                         device_key[1],
                                         domain)
                    self._bytes_per_device_per_domain_per_minute[
                            device_domain_key] += packet.size
            else:
                domain_key = (rounded_timestamp, 'unknown')
                self._bytes_per_domain_per_minute[domain_key] += packet.size
                device_domain_key \
                        = (rounded_timestamp, device_key[1], 'unknown')
                self._bytes_per_device_per_domain_per_minute[
                        device_domain_key] += packet.size
        else:
            self._bytes_per_port_per_minute[rounded_timestamp, -1] \
                    += packet.size
            self._bytes_per_domain_per_minute[rounded_timestamp, 'unknown'] \
                    += packet.size
            self._bytes_per_device_per_minute[rounded_timestamp, 'unknown'] \
                    += packet.size
            self._bytes_per_device_per_port_per_minute[
                    rounded_timestamp, 'unknown', -1] != packet.size
            self._bytes_per_device_per_domain_per_minute[
                    rounded_timestamp, 'unknown', 'unknown'] != packet.size

    def process_update(self, update):
        for domain in update.whitelist:
            self._whitelist.add((domain, re.compile(r'(^|\.)%s$' % domain)))

        for offset, address in enumerate(update.addresses):
            index = (update.address_table_first_id + offset) \
                    % update.address_table_size
            self._address_map[address.ip_address] = index
            self._mac_address_map[address.ip_address] = address.mac_address

        for flow in update.flow_table:
            self.process_flow(flow)

        for a_record in update.a_records:
            try:
                a_packet = update.packet_series[a_record.packet_id]
            except IndexError:
                continue
            self.process_a_record(a_record, a_packet)

        for cname_record in update.cname_records:
            self.process_cname_record(cname_record, update.packet_series)

        for packet in update.packet_series:
            self.process_packet(packet)

    @property
    def results(self):
        return { 'bytes_per_minute': self._bytes_per_minute,
                 'bytes_per_port_per_minute': self._bytes_per_port_per_minute,
                 'bytes_per_domain_per_minute': \
                         self._bytes_per_domain_per_minute,
                 'bytes_per_device_per_minute': \
                         self._bytes_per_device_per_minute,
                 'bytes_per_device_per_port_per_minute': \
                         self._bytes_per_device_per_port_per_minute,
                 'bytes_per_device_per_domain_per_minute': \
                         self._bytes_per_device_per_domain_per_minute }

class BytesSessionAggregator(SessionAggregator):
    ResultsRecord = namedtuple('ResultsRecord',
                               ['bytes_per_minute',
                                'bytes_per_port_per_minute',
                                'bytes_per_domain_per_minute'])
    ContextResultsRecord = namedtuple(
            'ContextResultsRecord',
            ['bytes_per_device_per_minute',
             'bytes_per_device_per_port_per_minute',
             'bytes_per_device_per_domain_per_minute'])

    def __init__(self, username, database):
        super(BytesSessionAggregator, self).__init__()

        self._username = username
        self._database = database

        self._node_statistics = defaultdict(lambda:
                self.ResultsRecord(
                    bytes_per_minute=defaultdict(int),
                    bytes_per_port_per_minute=defaultdict(int),
                    bytes_per_domain_per_minute=defaultdict(int)))
        self._nodes_updated = set()
        self._context_statistics = defaultdict(lambda:
                self.ContextResultsRecord(
                    bytes_per_device_per_minute=defaultdict(int),
                    bytes_per_device_per_port_per_minute=defaultdict(int),
                    bytes_per_device_per_domain_per_minute=defaultdict(int)))
        self._contexts_updated = set()

    def augment_results(self,
                        node_id,
                        anonymization_id,
                        session_id,
                        results,
                        updated):
        merge_timeseries(results['bytes_per_minute'],
                         self._node_statistics[node_id]\
                                 .bytes_per_minute)
        merge_timeseries(results['bytes_per_port_per_minute'],
                         self._node_statistics[node_id]\
                                 .bytes_per_port_per_minute)
        merge_timeseries(results['bytes_per_domain_per_minute'],
                         self._node_statistics[node_id]\
                                 .bytes_per_domain_per_minute)
        merge_timeseries(results['bytes_per_device_per_minute'],
                         self._context_statistics[node_id, anonymization_id]\
                                 .bytes_per_device_per_minute)
        merge_timeseries(
                results['bytes_per_device_per_port_per_minute'],
                self._context_statistics[node_id, anonymization_id]\
                        .bytes_per_device_per_port_per_minute)
        merge_timeseries(
                results['bytes_per_device_per_domain_per_minute'],
                self._context_statistics[node_id, anonymization_id]\
                        .bytes_per_device_per_domain_per_minute)
        if updated:
            self._nodes_updated.add(node_id)
            self._contexts_updated.add((node_id, anonymization_id))

    def store_results(self):
        database = db.BismarkPassiveDatabase(self._username, self._database)
        for node_id, record in self._node_statistics.items():
            if node_id in self._nodes_updated:
                database.import_node_byte_statistics(
                        node_id,
                        record.bytes_per_minute,
                        record.bytes_per_port_per_minute,
                        record.bytes_per_domain_per_minute)
        for (node_id, anonymization_context), record \
                in self._context_statistics.items():
            if (node_id, anonymization_context) in self._contexts_updated:
                database.import_context_byte_statistics(
                        node_id,
                        anonymization_context,
                        record.bytes_per_device_per_minute,
                        record.bytes_per_device_per_port_per_minute,
                        record.bytes_per_device_per_domain_per_minute)
        database.refresh_memoization()
