from collections import namedtuple

PacketEntry = namedtuple('PacketSeries', ['timestamp', 'size', 'flow_id'])
FlowEntry = namedtuple('FlowTable',
                       ['flow_id', 'source_ip',
                           'destination_ip', 'transport_protocol',
                           'source_port', 'destination_port'])
DnsAEntry = namedtuple('DnsTableA', ['address_id', 'domain', 'ip_address'])
DnsCnameEntry = namedtuple('DnsCnameTable', ['address_id', 'domain', 'cname'])
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
    def __init__(self, filename):
        contents = open(filename, 'r').read()
        lines = contents.splitlines()
        sections = parse_sections(lines)

        sections = {
                'intro': sections[0],
                'anonymization': sections[1],
                'packet_series': sections[2],
                'flow_table': sections[3],
                'dns_table_a': sections[4],
                'dns_table_cname': sections[5],
                'address_table': sections[6]
                }

        intro_stats = [ int(w) for w in sections['intro'][0].split() ]
        self.creation_time = intro_stats[0]
        self.pcap_received = intro_stats[1]
        self.pcap_dropped = intro_stats[2]
        self.iface_dropped = intro_stats[3]

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
                timestamp = current_timestamp,
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
            (flow_id, source_ip, destination_ip, transport_protocol,
                    source_port, destination_port) = line.split()
            self.flow_table.append(FlowEntry(
                flow_id = int(flow_id),
                source_ip = source_ip,
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
            address_id, domain, address = line.split()
            self.a_records.append(DnsAEntry(
                address_id = int(address_id),
                domain = domain,
                ip_address = address
                ))

        self.cname_records = []
        for line in sections['dns_table_cname']:
            address_id, domain, cname = line.split()
            self.cname_records.append(DnsCnameEntry(
                address_id = int(address_id),
                domain = domain,
                cname = cname
                ))

        address_stats = [ int(w) for w in sections['address_table'][0].split() ]
        self.address_table_first_id = address_stats[0]
        self.address_table_size = address_stats[1]
        self.address_table = []
        for line in sections['address_table'][1:]:
            mac, ip = line.split()
            self.addresses.append(AddressEntry(
                mac_address = mac,
                ip_address = ip
                ))
