""" This module maintains the following useful state in the session context:

    flows maps flow IDs to a tuple of a flow object (containing IP addresses,
          port numbers, etc.) and a set of domains corresponding to
          the DNS mappings for the flow's source and destination IPs
          that were valid when the flow was created.
    address_map maps LAN IP addresses to indices in update.addresses.
                These indices are used in several places as placeholders
                for local MAC addresses (anywhere you see "address_id").
    mac_address_map maps LAN IP address to local MAC addresses; this
                    is a shortcut for address_map if all you need is
                    to look up the MAC address of a local device.
    dns_ip_map maps an address_id (i.e., a local device) and a
                remote IP address to a set of domain names and
                valid time windows for those mappings. This is
                the set of valid DNS mappings for a device.
"""

from collections import defaultdict
import re

from session_processor import PersistentSessionProcessor

class FlowCorrelationSessionProcessor(PersistentSessionProcessor):
    """Builds a table mapping flow IDs from update traces to flow objects."""

    def initialize_context(self, context):
        context.flows = dict()

    def process_update_persistent(self, context, update):
        for flow in update.flow_table:
            context.flows[flow.flow_id] = flow

class MacAddressCorrelationSessionProcessor(PersistentSessionProcessor):
    """Builds tables mapping IPs to MAC addresses and address table indices."""

    def initialize_context(self, context):
        context.ip_to_mac_address_map = dict()
        context.ip_to_mac_address_index_map = dict()

    def process_update_persistent(self, context, update):
        for offset, address in enumerate(update.addresses):
            index = (update.address_table_first_id + offset) \
                    % update.address_table_size
            context.ip_to_mac_address_index_map[address.ip_address] = index
            context.ip_to_mac_address_map[address.ip_address] = \
                    address.mac_address

class DomainNameCorrelationSessionProcessor(PersistentSessionProcessor):
    """Builds a table mapping address table IDs and IP addresses to domain
    names.  This represents the set of valid DNS mappings for a device to a
    remote IP address."""

    def initialize_context(self, context):
        context.whitelist = set()
        context.dns_ip_map = defaultdict(set)
        context._dns_a_map_domain = defaultdict(list)

    def process_update_persistent(self, context, update):
        for domain in update.whitelist:
            context.whitelist.add((domain, re.compile(r'(^|\.)%s$' % domain)))
        for a_record in update.a_records:
            try:
                a_packet = update.packet_series[a_record.packet_id]
            except IndexError:
                continue
            self.process_a_record(context, a_record, a_packet)
        for cname_record in update.cname_records:
            self.process_cname_record(
                    context, cname_record, update.packet_series)
        self.cleanup_dns_ip_map(context, update.timestamp)

    def process_a_record(self, context, a_record, a_packet):
        domain_key = (a_record.address_id, a_record.anonymized, a_record.domain)
        context._dns_a_map_domain[domain_key].append(a_record)
        if not a_record.anonymized:
            for domain, pattern in context.whitelist:
                if pattern.search(a_record.domain) is not None:
                    ip_key = (a_record.address_id, a_record.ip_address)
                    domain_record = (domain,
                                     a_packet.timestamp,
                                     a_packet.timestamp + a_record.ttl)
                    context.dns_ip_map[ip_key].add(domain_record)

    def process_cname_record(self, context, cname_record, packet_series):
        if cname_record.domain_anonymized:
            return
        try:
            cname_packet = packet_series[cname_record.packet_id]
        except IndexError:
            return
        domain_key = (cname_record.address_id,
                      cname_record.cname_anonymized,
                      cname_record.cname)
        a_records = context._dns_a_map_domain.get(domain_key)
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
                    context.dns_ip_map[ip_key].add(domain_record)

    def cleanup_dns_ip_map(self, context, timestamp):
        for key, mappings in context.dns_ip_map.items():
             mappings.difference_update(filter(
                 lambda (domain, start, end): end < timestamp,
                 mappings))
