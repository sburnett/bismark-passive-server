#!/usr/bin/env python

from datetime import timedelta
import glob
import gzip
from optparse import OptionParser
import os.path
import re
import pickle

import index_traces
import parser
import db

def process_session_updates(updates):
    bytes_per_minute = {}
    bytes_per_port_per_minute = {}
    bytes_per_domain_per_minute = {}

    whitelist = set()
    address_map = {}
    flows = {}
    flow_ip_map = {}
    dns_map_ip = {}
    dns_a_map_domain = {}
    for idx, update in enumerate(updates):
        print 'Update', idx

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
            try:
                a_packet = update.packet_series[a_record.packet_id]
            except IndexError:
                continue
            ttl_delta = timedelta(seconds=a_record.ttl)
            for domain, pattern in whitelist:
                if pattern.search(a_record.domain) is not None:
                    ip_key = (a_record.address_id, a_record.ip_address)
                    domain_record = (domain,
                                     a_packet.timestamp,
                                     a_packet.timestamp + ttl_delta)
                    dns_map_ip.setdefault(ip_key, set())
                    dns_map_ip[ip_key].add(domain_record)
                    if ip_key in flow_ip_map:
                        flow_ip_map[ip_key].add(domain_record)

        for cname_record in update.cname_records:
            if cname_record.anonymized:
                continue
            try:
                cname_packet = update.packet_series[cname_record.packet_id]
            except IndexError:
                continue
            cname_ttl_delta = timedelta(seconds=cname_record.ttl)
            domain_key = (cname_record.address_id, cname_record.cname)
            a_records = dns_a_map_domain.get(domain_key)
            if a_records is None:
                continue
            for domain, pattern in whitelist:
                if pattern.search(cname_record.domain) is not None:
                    for a_record in a_records:
                        try:
                            a_packet = update.packet_series[a_record.packet_id]
                        except IndexError:
                            continue
                        a_ttl_delta = timedelta(seconds=a_record.ttl)
                        start_timestamp = max(cname_packet.timestamp,
                                              a_packet.timestamp)
                        end_timestamp = min(
                                cname_packet.timestamp + cname_ttl_delta,
                                a_packet.timestamp + a_ttl_delta)
                        if start_timestamp > end_timestamp:
                            continue
                        ip_key = (a_record.address_id, a_record.ip_address)
                        domain_record = (domain, start_timestamp, end_timestamp)
                        dns_map_ip.setdefault(ip_key, set())
                        dns_map_ip[ip_key].add(domain_record)
                        if ip_key in flow_ip_map:
                            flow_ip_map[ip_key].add(domain_record)

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
                    for domain, start_time, end_time in flow_ip_map[key]:
                        if packet.timestamp < start_time \
                                or packet.timestamp > end_time:
                            continue
                        domain_key = (rounded_timestamp, domain)
                        bytes_per_domain_per_minute.setdefault(domain_key, 0)
                        bytes_per_domain_per_minute[domain_key] += packet.size

    return (bytes_per_minute,
            bytes_per_port_per_minute,
            bytes_per_domain_per_minute)

def process_session(filenames):
    updates = []
    for filename in filenames:
        print filename
        update_content = gzip.open(filename).read()
        updates.append(parser.PassiveUpdate(update_content))
    return process_session_updates(updates)

def process_sessions(session_dirs):
    for session_dir in session_dirs:
        filenames = glob.glob(os.path.join(session_dir, '*.gz'))
        filenames.sort(key=lambda f: os.path.getmtime(f))
        stats = process_session(filenames)

        handle = open(os.path.join(session_dir, 'results.pickle'), 'wb')
        pickle.dump(stats, handle)
        handle.close()

def merge_timeseries(first, second):
    for key, value in first.items():
        second.setdefault(key, 0)
        second[key] += value
    return first

def write_results_to_database(index_directory):
    bpdb = db.BismarkPassiveDatabase('sburnett', 'bismark_openwrt_live_v0_1')
    for nodedir in glob.glob(os.path.join(index_directory, '*')):
        if not os.path.isdir(nodedir):
            continue

        bytes_per_minute = {}
        bytes_per_port_per_minute = {}
        bytes_per_domain_per_minute = {}

        session_dirs = glob.glob(os.path.join(nodedir, '*', '*'))
        session_dirs.sort(key=lambda f: os.path.basename(f))
        for session_dir in session_dirs:
            if not os.path.isdir(session_dir):
                continue
            filename = os.path.join(session_dir, 'results.pickle')
            if not os.path.exists(filename):
                process_sessions([session_dir])
            handle = open(filename, 'rb')
            bpm, bpppm, bpdpm = pickle.load(handle)
            merge_timeseries(bpm, bytes_per_minute)
            merge_timeseries(bpppm, bytes_per_port_per_minute)
            merge_timeseries(bpdpm, bytes_per_domain_per_minute)

        node_id = os.path.basename(nodedir)
        bpdb.import_statistics(node_id,
                               bytes_per_minute,
                               bytes_per_port_per_minute,
                               bytes_per_domain_per_minute)

def parse_args():
    usage = 'usage: %prog [options] updates_directory index_directory archive_directory'
    parser = OptionParser(usage=usage)
    options, args = parser.parse_args()
    if len(args) != 3:
        parser.error('Missing required option')
    mandatory = { 'updates_directory': args[0],
                  'index_directory': args[1],
                  'archive_directory': args[2] }
    return options, mandatory

def main():
    (options, args) = parse_args()
    indexed = index_traces.index_traces(args['updates_directory'],
                                        args['index_directory'],
                                        args['archive_directory'])
    process_sessions(indexed)
    write_results_to_database(args['index_directory'])

if __name__ == '__main__':
    main()
