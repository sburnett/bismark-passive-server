import re

from session_processor import ProcessorCoordinator, SessionProcessor
import utils

class CorrelationSessionProcessor(SessionProcessor):
    def __init__(self):
        super(CorrelationSessionProcessor, self).__init__()

    def process_update(self, context, update):
        for domain in update.whitelist:
            context.whitelist.add((domain, re.compile(r'(^|\.)%s$' % domain)))

        for offset, address in enumerate(update.addresses):
            index = (update.address_table_first_id + offset) \
                    % update.address_table_size
            context.address_map[address.ip_address] = index
            context.mac_address_map[address.ip_address] = address.mac_address

        for flow in update.flow_table:
            self.process_flow(context, flow)

        for a_record in update.a_records:
            try:
                a_packet = update.packet_series[a_record.packet_id]
            except IndexError:
                continue
            self.process_a_record(context, a_record, a_packet)

        for cname_record in update.cname_records:
            self.process_cname_record(
                    context, cname_record, update.packet_series)

    def process_flow(self, context, flow):
        context.flows[flow.flow_id] = flow
        if flow.source_ip in context.address_map \
                and not flow.destination_ip_anonymized:
            key = (context.address_map[flow.source_ip], flow.destination_ip)
        elif flow.destination_ip in context.address_map \
                and not flow.source_ip_anonymized:
            key = (context.address_map[flow.destination_ip], flow.source_ip)
        else:
            return
        context.flow_ip_map[key]
        if key in context.dns_map_ip:
            context.flow_ip_map[key].update(context.dns_map_ip[key])

    def process_a_record(self, context, a_record, a_packet):
        if a_record.anonymized:
            return
        domain_key = (a_record.address_id, a_record.domain)
        context.dns_a_map_domain[domain_key].append(a_record)
        for domain, pattern in context.whitelist:
            if pattern.search(a_record.domain) is not None:
                ip_key = (a_record.address_id, a_record.ip_address)
                domain_record = (domain,
                                 a_packet.timestamp,
                                 a_packet.timestamp + a_record.ttl)
                context.dns_map_ip[ip_key].add(domain_record)
                if ip_key in context.flow_ip_map:
                    context.flow_ip_map[ip_key].add(domain_record)

    def process_cname_record(self, context, cname_record, packet_series):
        if cname_record.anonymized:
            return
        try:
            cname_packet = packet_series[cname_record.packet_id]
        except IndexError:
            return
        domain_key = (cname_record.address_id, cname_record.cname)
        a_records = context.dns_a_map_domain.get(domain_key)
        if a_records is None:
            return
        for domain, pattern in context.whitelist:
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
                    context.dns_map_ip[ip_key].add(domain_record)
                    if ip_key in context.flow_ip_map:
                        context.flow_ip_map[ip_key].add(domain_record)

class CorrelationProcessorCoordinator(ProcessorCoordinator):
    persistent_state = dict(
            whitelist=(set, None),
            address_map=(dict, None),
            mac_address_map=(dict, None),
            flows=(dict, None),
            flow_ip_map=(utils.initialize_set_dict, None),
            dns_map_ip=(utils.initialize_set_dict, None),
            dns_a_map_domain=(utils.initialize_list_dict, None)
            )
    ephemeral_state = dict()

    def __init__(self):
        super(CorrelationProcessorCoordinator, self).__init__()

    def create_processor(self, session):
        return CorrelationSessionProcessor()
