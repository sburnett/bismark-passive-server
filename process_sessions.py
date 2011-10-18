#!/usr/bin/env python

from collections import namedtuple
import glob
import gzip
from optparse import OptionParser
import os.path
import re
import cPickle

import index_traces
import parser
import db

pickle_filename = 'update_processor.pickle'

UpdateStatistics = namedtuple('UpdateStatistics',
                              ['pcap_dropped',
                               'iface_dropped',
                               'packet_series_dropped',
                               'flow_table_dropped',
                               'dropped_a_records',
                               'dropped_cname_records',
                               'packet_series_size',
                               'flow_table_size',
                               'a_records_size',
                               'cname_records_size'])

class SessionProcessor(object):
    def __init__(self):
        self._filenames_processed = set()
        self._last_sequence_number_processed = -1

        self._bytes_per_minute = {}
        self._bytes_per_port_per_minute = {}
        self._bytes_per_domain_per_minute = {}
        self._update_statistics = {}

        self._whitelist = set()
        self._address_map = {}
        self._flows = {}
        self._flow_ip_map = {}
        self._dns_map_ip = {}
        self._dns_a_map_domain = {}

    bytes_per_minute = property(lambda self: self._bytes_per_minute)
    bytes_per_port_per_minute \
            = property(lambda self: self._bytes_per_port_per_minute)
    bytes_per_domain_per_minute \
            = property(lambda self: self._bytes_per_domain_per_minute)
    update_statistics = property(lambda self: self._update_statistics)

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
        self._flow_ip_map.setdefault(key, set())
        if key in self._dns_map_ip:
            self._flow_ip_map[key].update(self._dns_map_ip[key])

    def process_a_record(self, a_record, a_packet):
        if a_record.anonymized:
            return
        domain_key = (a_record.address_id, a_record.domain)
        self._dns_a_map_domain.setdefault(domain_key, [])
        self._dns_a_map_domain[domain_key].append(a_record)
        for domain, pattern in self._whitelist:
            if pattern.search(a_record.domain) is not None:
                ip_key = (a_record.address_id, a_record.ip_address)
                domain_record = (domain,
                                 a_packet.timestamp,
                                 a_packet.timestamp + a_record.ttl)
                self._dns_map_ip.setdefault(ip_key, set())
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
                    self._dns_map_ip.setdefault(ip_key, set())
                    self._dns_map_ip[ip_key].add(domain_record)
                    if ip_key in self._flow_ip_map:
                        self._flow_ip_map[ip_key].add(domain_record)

    def process_packet(self, packet):
        rounded_timestamp = packet.timestamp.replace(second=0, microsecond=0)
        self._bytes_per_minute.setdefault(rounded_timestamp, 0)
        self._bytes_per_minute[rounded_timestamp] += packet.size

        flow = self._flows.get(packet.flow_id)
        if flow is not None:
            port_key = None
            if flow.source_ip in self._address_map:
                port_key = (rounded_timestamp, flow.destination_port)
            elif flow.destination_ip in self._address_map:
                port_key = (rounded_timestamp, flow.source_port)
            if port_key is not None:
                self._bytes_per_port_per_minute.setdefault(port_key, 0)
                self._bytes_per_port_per_minute[port_key] += packet.size

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
                    self._bytes_per_domain_per_minute.setdefault(domain_key, 0)
                    self._bytes_per_domain_per_minute[domain_key] \
                            += packet.size

    def process_statistics(self, update):
        self._update_statistics[update.timestamp] = UpdateStatistics(
                pcap_dropped = update.pcap_dropped,
                iface_dropped = update.iface_dropped,
                packet_series_dropped = update.packet_series_dropped,
                flow_table_dropped = update.flow_table_dropped,
                dropped_a_records = update.dropped_a_records,
                dropped_cname_records = update.dropped_cname_records,
                packet_series_size = len(update.packet_series),
                flow_table_size = update.flow_table_size,
                a_records_size = len(update.a_records),
                cname_records_size = len(update.cname_records))

    def process_update(self, update):
        for domain in update.whitelist:
            self._whitelist.add((domain, re.compile(r'(^|\.)%s$' % domain)))

        for offset, address in enumerate(update.addresses):
            index = (update.address_table_first_id + offset) \
                    % update.address_table_size
            self._address_map[address.ip_address] = index

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

        self.process_statistics(update)

    def process_session(self, session_dir):
        print session_dir
        filenames = glob.glob(os.path.join(session_dir, '*.gz'))
        filenames.sort(key=lambda f: os.path.getmtime(f))
        processed_new_update = False
        for filename in filenames:
            basename = os.path.basename(filename)
            print ' ', basename,
            if basename in self._filenames_processed:
                print 'skipped (filename)'
                continue
            update_content = gzip.open(filename).read()
            update = parser.PassiveUpdate(update_content)
            if update.sequence_number \
                    == self._last_sequence_number_processed + 1:
                self.process_update(update)
                self._last_sequence_number_processed = update.sequence_number
                print 'processed'
            else:
                print 'skipped (sequence number)'
            self._filenames_processed.add(basename)
            processed_new_update = True
        return processed_new_update

def process_session(session_dir):
    pickle_path = os.path.join(session_dir, pickle_filename)
    try:
        processor = cPickle.load(open(pickle_path, 'rb'))
    except:
        processor = SessionProcessor()
    if processor.process_session(session_dir):
        cPickle.dump(processor, open(pickle_path, 'wb'))
        return processor, True
    else:
        return processor, False

def merge_timeseries(first, second):
    for key, value in first.items():
        second.setdefault(key, 0)
        second[key] += value

def write_results_to_database(index_directory, username, database):
    bpdb = db.BismarkPassiveDatabase(username, database)
    for nodedir in glob.glob(os.path.join(index_directory, '*')):
        if not os.path.isdir(nodedir):
            continue

        bytes_per_minute = {}
        bytes_per_port_per_minute = {}
        bytes_per_domain_per_minute = {}
        update_statistics = {}

        need_to_import = False
        session_dirs = glob.glob(os.path.join(nodedir, '*', '*'))
        session_dirs.sort(key=lambda f: os.path.basename(f))
        for session_dir in session_dirs:
            if not os.path.isdir(session_dir):
                continue
            processor, new_updates = process_session(session_dir)
            if new_updates:
                need_to_import = True
            merge_timeseries(processor.bytes_per_minute, bytes_per_minute)
            merge_timeseries(processor.bytes_per_port_per_minute,
                             bytes_per_port_per_minute)
            merge_timeseries(processor.bytes_per_domain_per_minute,
                             bytes_per_domain_per_minute)
            update_statistics.update(processor.update_statistics)

        if need_to_import:
            node_id = os.path.basename(nodedir)
            bpdb.import_statistics(node_id,
                                   bytes_per_minute,
                                   bytes_per_port_per_minute,
                                   bytes_per_domain_per_minute,
                                   update_statistics)

def parse_args():
    usage = 'usage: %prog [options]' \
            + ' updates_directory index_directory archive_directory'
    parser = OptionParser(usage=usage)
    parser.add_option('-u', '--username', action='store', dest='username',
                      default='sburnett', help='Database username')
    parser.add_option('-d', '--database', action='store', dest='database',
                      default='bismark_openwrt_live_v0_1',
                      help='Database name')
    options, args = parser.parse_args()
    if len(args) != 3:
        parser.error('Missing required option')
    ProcessArgs = namedtuple(
            'ProcessArgs',
            ['updates_directory', 'index_directory', 'archive_directory'])
    mandatory = ProcessArgs(updates_directory=args[0],
                            index_directory=args[1],
                            archive_directory=args[2])
    return options, mandatory

def main():
    (options, args) = parse_args()
    index_traces.index_traces(args.updates_directory,
                              args.index_directory,
                              args.archive_directory)
    write_results_to_database(args.index_directory,
                              options.username,
                              options.database)

if __name__ == '__main__':
    main()
