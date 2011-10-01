import datetime
from collections import namedtuple

PacketEntry = namedtuple('PacketSeries', ['timestamp', 'size', 'flow_id'])
FlowEntry = namedtuple('FlowTable',
                       ['flow_id', 'source_ip_anonymized', 'source_ip',
                           'destination_ip_anonymized', 'destination_ip',
                           'transport_protocol',
                           'source_port', 'destination_port'])
DnsAEntry = namedtuple('DnsTableA',
                       ['packet_id', 'address_id', 'anonymized',
                           'domain', 'ip_address', 'ttl'])
DnsCnameEntry = namedtuple('DnsCnameTable',
                           ['packet_id', 'address_id', 'anonymized',
                               'domain', 'cname', 'ttl'])
AddressEntry = namedtuple('AddressTable', ['mac_address', 'ip_address'])

def parse_sections(lines):
    sections = [[]]
    section_counter = 0
    for line in lines:
        if len(line) == 0:
            section_counter += 1
            while len(sections) <= section_counter:
                sections.append([])
        else:
            sections[section_counter].append(line)
    return sections

class PassiveUpdate(object):
    def __init__(self, contents):
        lines = contents.splitlines()
        sections = parse_sections(lines)

        sections = {
                'intro': sections[0],
                'whitelist': sections[1],
                'anonymization': sections[2],
                'packet_series': sections[3],
                'flow_table': sections[4],
                'dns_table_a': sections[5],
                'dns_table_cname': sections[6],
                'address_table': sections[7]
                }

        intro_ids = sections['intro'][0].split()
        self.bismark_id = intro_ids[0]
        self.creation_time = int(intro_ids[1])
        self.sequence_number = int(intro_ids[2])
        self.timestamp = datetime.datetime.utcfromtimestamp(int(intro_ids[3]))
        if len(sections['intro']) >= 2:
            intro_stats = [ int(w) for w in sections['intro'][1].split() ]
            self.pcap_received = intro_stats[0]
            self.pcap_dropped = intro_stats[1]
            self.iface_dropped = intro_stats[2]

        self.whitelist = list(sections['whitelist'])

        if sections['anonymization'][0] == 'UNANONYMIZED':
            self.anonymized = False
        else:
            self.anonymized = True
            self.anonymization_signature = sections['anonymization'][0]

        packet_stats = [ int(w) for w in sections['packet_series'][0].split() ]
        current_timestamp = packet_stats[0]
        self.packet_series_dropped = packet_stats[1]
        self.packet_series = []
        for line in sections['packet_series'][1:]:
            offset, size, flow_id = [ int(w) for w in line.split() ]
            current_timestamp += offset
            self.packet_series.append(PacketEntry(
                timestamp = datetime.datetime.utcfromtimestamp(current_timestamp / 1e6),
                size = size,
                flow_id = flow_id
                ))

        flow_stats = [ int(w) for w in sections['flow_table'][0].split() ]
        self.flow_table_baseline = flow_stats[0]
        self.flow_table_size = flow_stats[1]
        self.flow_table_expired = flow_stats[2]
        self.flow_table_dropped = flow_stats[3]
        self.flow_table = []
        for line in sections['flow_table'][1:]:
            (flow_id, source_ip_anonymized, source_ip,
                    destination_ip_anonymized, destination_ip,
                    transport_protocol,
                    source_port, destination_port) = line.split()
            self.flow_table.append(FlowEntry(
                flow_id = int(flow_id),
                source_ip_anonymized = int(source_ip_anonymized),
                source_ip = source_ip,
                destination_ip_anonymized = int(destination_ip_anonymized),
                destination_ip = destination_ip,
                transport_protocol = int(transport_protocol),
                source_port = int(source_port),
                destination_port = int(destination_port)
                ))

        dns_stats = [ int(w) for w in sections['dns_table_a'][0].split() ]
        self.dropped_a_records = dns_stats[0]
        self.dropped_cname_records = dns_stats[1]
        self.a_records = []
        for line in sections['dns_table_a'][1:]:
            packet_id, address_id, anonymized, domain, address, ttl \
                    = line.split()
            self.a_records.append(DnsAEntry(
                packet_id = int(packet_id),
                address_id = int(address_id),
                anonymized = int(anonymized),
                domain = domain,
                ip_address = address,
                ttl = datetime.timedelta(seconds=int(ttl)),
                ))

        self.cname_records = []
        for line in sections['dns_table_cname']:
            packet_id, address_id, anonymized, domain, cname, ttl \
                    = line.split()
            self.cname_records.append(DnsCnameEntry(
                packet_id = int(packet_id),
                address_id = int(address_id),
                anonymized = int(anonymized),
                domain = domain,
                cname = cname,
                ttl = datetime.timedelta(seconds=int(ttl)),
                ))

        address_stats = [ int(w) for w in sections['address_table'][0].split() ]
        self.address_table_first_id = address_stats[0]
        self.address_table_size = address_stats[1]
        self.addresses = []
        for line in sections['address_table'][1:]:
            mac, ip = line.split()
            self.addresses.append(AddressEntry(
                mac_address = mac,
                ip_address = ip,
                ))
