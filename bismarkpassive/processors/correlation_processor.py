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
    ip_to_domain_map maps an address_id (i.e., a local device) and a
                remote IP address to a set of domain names and
                valid time windows for those mappings. This is
                the set of valid DNS mappings for a device.
"""

from collections import defaultdict
from datetime import datetime, timedelta
from itertools import ifilter
import re

from bismarkpassive import PersistentSessionProcessor

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

class DomainCorrelationSessionProcessor(PersistentSessionProcessor):
    """Builds a table mapping address table IDs and IP addresses to domain
    names. This represents the set of valid DNS mappings for each device."""

    def initialize_context(self, context):
        context.ip_to_domain_map = defaultdict(set)
        context._domain_to_a_record_map = defaultdict(list)
        context._latest_timestamp = datetime.min

    def process_update_persistent(self, context, update):
        for a_record in update.a_records:
            try:
                a_packet = update.packet_series[a_record.packet_id]
            except IndexError:
                continue
            self.process_a_record(context, a_record, a_packet)
        for cname_record in update.cname_records:
            self.process_cname_record(
                    context, cname_record, update.packet_series)
        if update.timestamp - context._latest_timestamp > timedelta(seconds=30):
            self.garbage_collect_tables(context)
        context._latest_timestamp = update.timestamp

    def process_a_record(self, context, a_record, a_packet):
        domain_key = (a_record.address_id, a_record.anonymized, a_record.domain)
        context._domain_to_a_record_map[domain_key].append(
                (a_record, a_packet.timestamp))
        ip_key = (a_record.address_id, a_record.anonymized, a_record.ip_address)
        domain_record = (a_record.anonymized,
                         a_record.domain,
                         a_packet.timestamp,
                         a_packet.timestamp + a_record.ttl)
        context.ip_to_domain_map[ip_key].add(domain_record)

    def process_cname_record(self, context, cname_record, packet_series):
        try:
            cname_packet = packet_series[cname_record.packet_id]
        except IndexError:
            return
        domain_key = (cname_record.address_id,
                      cname_record.cname_anonymized,
                      cname_record.cname)
        try:
            a_records = context._domain_to_a_record_map[domain_key]
        except KeyError:
            return
        for a_record, a_timestamp in a_records:
            start_timestamp = max(cname_packet.timestamp, a_timestamp)
            end_timestamp = min(cname_packet.timestamp + cname_record.ttl,
                                a_timestamp + a_record.ttl)
            if start_timestamp > end_timestamp:
                continue
            ip_key = (a_record.address_id,
                      a_record.anonymized,
                      a_record.ip_address)
            domain_record = (cname_record.domain_anonymized,
                             cname_record.domain,
                             start_timestamp,
                             end_timestamp)
            context.ip_to_domain_map[ip_key].add(domain_record)
    
    def garbage_collect_tables(self, context):
        for key in context.ip_to_domain_map.keys():
            new_mappings = set(ifilter(
                 lambda (anon, domain, start, end): end >= context._latest_timestamp,
                 context.ip_to_domain_map[key]))
            if new_mappings:
                context.ip_to_domain_map[key] = new_mappings
            else:
                del context.ip_to_domain_map[key]
        for key in context._domain_to_a_record_map.keys():
            new_mappings = filter(
                lambda (record, timestamp): timestamp + record.ttl >= context._latest_timestamp,
                context._domain_to_a_record_map[key])
            if new_mappings:
                context._domain_to_a_record_map[key] = new_mappings
            else:
                del context._domain_to_a_record_map[key]

    def complete_session_persistent(self, context):
        self.garbage_collect_tables(context)

class WhitelistedDomainCorrelationSessionProcessor(PersistentSessionProcessor):
    """Builds a table mapping address table IDs and IP addresses to whitelisted
    domain names, without their subdomains. This represents the set of valid DNS
    mappings to whitelisted domains for each device."""

    def initialize_context(self, context):
        context.whitelist = set()
        context.ip_to_domain_map = defaultdict(set)
        context._domain_to_a_record_map = defaultdict(list)
        context._latest_timestamp = datetime.min

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
        #if update.timestamp - context._latest_timestamp > timedelta(minutes=15):
        #    self.garbage_collect_tables(context)
        self._latest_timestamp = update.timestamp

    def process_a_record(self, context, a_record, a_packet):
        domain_key = (a_record.address_id, a_record.anonymized, a_record.domain)
        context._domain_to_a_record_map[domain_key].append(
                (a_record, a_packet.timestamp))
        if not a_record.anonymized:
            for domain, pattern in context.whitelist:
                if pattern.search(a_record.domain) is not None:
                    ip_key = (a_record.address_id, a_record.anonymized, a_record.ip_address)
                    domain_record = (0,
                                     domain,
                                     a_packet.timestamp,
                                     a_packet.timestamp + a_record.ttl)
                    context.ip_to_domain_map[ip_key].add(domain_record)

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
        a_records = context._domain_to_a_record_map.get(domain_key)
        if a_records is None:
            return
        for domain, pattern in context.whitelist:
            if pattern.search(cname_record.domain) is not None:
                for a_record, a_timestamp in a_records:
                    start_timestamp = max(cname_packet.timestamp, a_timestamp)
                    end_timestamp = min(
                            cname_packet.timestamp + cname_record.ttl,
                            a_timestamp + a_record.ttl)
                    if start_timestamp > end_timestamp:
                        continue
                    ip_key = (a_record.address_id, a_record.anonymized, a_record.ip_address)
                    domain_record = (0, domain, start_timestamp, end_timestamp)
                    context.ip_to_domain_map[ip_key].add(domain_record)

    def garbage_collect_tables(self, context):
        for key in context.ip_to_domain_map.keys():
            new_mappings = set(ifilter(
                 lambda (anon, domain, start, end): end >= context._latest_timestamp,
                 context.ip_to_domain_map[key]))
            if new_mappings:
                context.ip_to_domain_map[key] = new_mappings
            else:
                del context.ip_to_domain_map[key]
        for key in context._domain_to_a_record_map.keys():
            new_mappings = filter(
                lambda (record, timestamp): timestamp + record.ttl >= context._latest_timestamp,
                context._domain_to_a_record_map[key])
            if new_mappings:
                context._domain_to_a_record_map[key] = new_mappings
            else:
                del context._domain_to_a_record_map[key]

    def complete_session_persistent(self, context):
        self.garbage_collect_tables(context)

class FlowToDomainCorrelationSessionProcessor(PersistentSessionProcessor):
    """Builds a map from flows to domains for that flow."""

    def initialize_context(self, context):
        context.flow_to_domain_map = dict()

    def process_update_persistent(self, context, update):
        new_flows = set()
        for entry in update.flow_table:
            new_flows.add(entry.flow_id)

        first_timestamps = dict()
        for packet in update.packet_series:
            if packet.flow_id not in first_timestamps \
                    and packet.flow_id in new_flows:
                first_timestamps[packet.flow_id] = packet.timestamp

        for flow in update.flow_table:
            context.flow_to_domain_map[flow.flow_id] = []

            if flow.source_ip in context.ip_to_mac_address_index_map:
                address_id = context.ip_to_mac_address_index_map[flow.source_ip]
                ip_anonymized = flow.destination_ip_anonymized
                ip_address = flow.destination_ip
            elif flow.destination_ip in context.ip_to_mac_address_index_map:
                address_id = context.ip_to_mac_address_index_map[flow.destination_ip]
                ip_anonymized = flow.source_ip_anonymized
                ip_address = flow.source_ip
            else:
                continue
            try:
                domains = context.ip_to_domain_map[
                        address_id, ip_anonymized, ip_address]
                first_timestamp = first_timestamps[flow.flow_id]
            except KeyError:
                continue
            for domain_anonymized, domain, start, end in domains:
                if first_timestamp < start or end < first_timestamp:
                    continue
                context.flow_to_domain_map[flow.flow_id].append(
                        (domain_anonymized, domain))
