#!/usr/bin/env python

import glob
import gzip
from optparse import OptionParser
import os.path
import re

import parser
import db

def process_session(filenames,
                    bytes_per_minute=None,
                    bytes_per_port_per_minute=None,
                    bytes_per_domain_per_minute=None):
    if bytes_per_minute is None:
        bytes_per_minute = {}
    if bytes_per_port_per_minute is None:
        bytes_per_port_per_minute = {}
    if bytes_per_domain_per_minute is None:
        bytes_per_domain_per_minute = {}

    whitelist = set()
    address_map = {}
    flows = {}
    flow_ip_map = {}
    dns_map_ip = {}
    dns_a_map_domain = {}
    for filename in filenames:
        print filename
        update_content = gzip.open(filename).read()
        update = parser.PassiveUpdate(update_content)

        for domain in update.whitelist:
            whitelist.add((domain, re.compile(r'(^|\.)%s$' % domain)))

        for offset, address in enumerate(update.addresses):
            index = (update.address_table_first_id + offset) \
                    % update.address_table_size
            address_map[address.ip_address] = index

        for flow in update.flow_table:
            flows[flow.flow_id] = flow
            if flow.source_ip in address_map \
                    and not flow.destination_ip_anonymized:
                key = (address_map[flow.source_ip], flow.destination_ip)
            elif flow.destination_ip in address_map \
                    and not flow.source_ip_anonymized:
                key = (address_map[flow.destination_ip], flow.source_ip)
            else:
                continue
            flow_ip_map.setdefault(key, set())
            if key in dns_map_ip:
                flow_ip_map[key].update(dns_map_ip[key])

        for a_record in update.a_records:
            if a_record.anonymized:
                continue
            domain_key = (a_record.address_id, a_record.domain)
            dns_a_map_domain.setdefault(domain_key, [])
            dns_a_map_domain[domain_key].append(a_record)
            for domain, pattern in whitelist:
                if pattern.search(a_record.domain) is not None:
                    ip_key = (a_record.address_id, a_record.ip_address)
                    dns_map_ip.setdefault(ip_key, set())
                    dns_map_ip[ip_key].add(domain)
                    if ip_key in flow_ip_map:
                        flow_ip_map[ip_key].add(domain)

        for cname_record in update.cname_records:
            domain_key = (cname_record.address_id, cname_record.cname)
            a_records = dns_a_map_domain.get(domain_key)
            if a_records is None:
                continue
            for domain, pattern in whitelist:
                if pattern.search(cname_record.domain) is not None:
                    for a_record in a_records:
                        ip_key = (a_record.address_id, a_record.ip_address)
                        dns_map_ip.setdefault(ip_key, set())
                        dns_map_ip[ip_key].add(domain)
                        if ip_key in flow_ip_map:
                            flow_ip_map[ip_key].add(domain)

        for packet in update.packet_series:
            rounded_timestamp = packet.timestamp.replace(second=0, microsecond=0)
            bytes_per_minute.setdefault(rounded_timestamp, 0)
            bytes_per_minute[rounded_timestamp] += packet.size

            flow = flows.get(packet.flow_id)
            if flow is not None:
                port_key = None
                if flow.source_ip in address_map:
                    port_key = (rounded_timestamp, flow.destination_port)
                elif flow.destination_ip in address_map:
                    port_key = (rounded_timestamp, flow.source_port)
                if port_key is not None:
                    bytes_per_port_per_minute.setdefault(port_key, 0)
                    bytes_per_port_per_minute[port_key] += packet.size

                key = None
                if flow.source_ip in address_map \
                        and not flow.destination_ip_anonymized:
                    key = (address_map[flow.source_ip], flow.destination_ip)
                elif flow.destination_ip in address_map \
                        and not flow.source_ip_anonymized:
                    key = (address_map[flow.destination_ip], flow.source_ip)
                if key is not None and key in flow_ip_map:
                    for domain in flow_ip_map[key]:
                        domain_key = (rounded_timestamp, domain)
                        bytes_per_domain_per_minute.setdefault(domain_key, 0)
                        bytes_per_domain_per_minute[domain_key] += packet.size

    return (bytes_per_minute,
            bytes_per_port_per_minute,
            bytes_per_domain_per_minute)

def process_node(nodedir):
    bytes_per_minute = {}
    bytes_per_port_per_minute = {}
    bytes_per_domain_per_minute = {}

    session_dirs = glob.glob(os.path.join(nodedir, '*', '*'))
    session_dirs.sort(key=lambda f: os.path.basename(f))
    for session_dir in session_dirs:
        if not os.path.isdir(session_dir):
            continue
        files = glob.glob(os.path.join(session_dir, '*.gz'))
        files.sort(key=lambda f: os.path.getmtime(f))
        (total, per_port, per_domain) \
                = process_session(files,
                                  bytes_per_minute,
                                  bytes_per_port_per_minute,
                                  bytes_per_domain_per_minute)
    return (bytes_per_minute,
            bytes_per_port_per_minute,
            bytes_per_domain_per_minute)

def process_index(directory):
    stats = {}
    bpdb = db.BismarkPassiveDatabase('sburnett', 'bismark_openwrt_live_v0_1')
    for nodedir in glob.glob(os.path.join(directory, '*')):
        if not os.path.isdir(nodedir):
            continue
        node_id = os.path.basename(nodedir)
        stats = process_node(nodedir)
        bpdb.import_statistics(node_id, *stats)

def parse_args():
    usage = 'usage: %prog [options] session_directory'
    parser = OptionParser(usage=usage)
    options, args = parser.parse_args()
    if len(args) != 1:
        parser.error('Missing required option')
    mandatory = { 'session_directory': args[0] }
    return options, mandatory

def main():
    (options, args) = parse_args()
    filenames = glob.glob(os.path.join(args['session_directory'], '*.gz'))
    filenames.sort(key=lambda f: os.path.getmtime(f))
    (bytes_per_minute,
            bytes_per_port_per_minute,
            bytes_per_domain_per_minute) = process_session(filenames)

if __name__ == '__main__':
    main()
