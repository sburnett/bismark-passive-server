from collections import namedtuple
import datetime

PacketEntry = namedtuple('PacketEntry', ['timestamp', 'size', 'flow_id'])
FlowEntry = namedtuple('FlowEntry',
                       ['flow_id', 'source_ip_anonymized', 'source_ip',
                           'destination_ip_anonymized', 'destination_ip',
                           'transport_protocol',
                           'source_port', 'destination_port'])
DnsAEntry = namedtuple('DnsAEntry',
                       ['packet_id', 'address_id', 'anonymized',
                           'domain', 'ip_address', 'ttl'])
DnsCnameEntry = namedtuple('DnsCnameEntry',
                           ['packet_id', 'address_id',
                               'domain_anonymized', 'domain',
                               'cname_anonymized', 'cname', 'ttl'])
AddressEntry = namedtuple('AddressEntry', ['mac_address', 'ip_address'])
HttpUrlEntry = namedtuple('HttpUrlEntry', ['flow_id', 'hashed_url'])

##############################################################################
# IMPORTANT: These values must match the identifiers of the same names in
# constants.h in the bismark-passive package! These reservations are only
# valid for file formats 2 and greater.
##############################################################################
class ReservedFlowIndices(object):
    FLOW_ID_ERROR = 0
    FLOW_ID_AARP = 1
    FLOW_ID_ARP = 2
    FLOW_ID_AT = 3
    FLOW_ID_IPV6 = 4
    FLOW_ID_IPX = 5
    FLOW_ID_REVARP = 6
    FLOW_ID_FIRST_UNRESERVED = 7
    FLOW_ID_LAST_UNRESERVED = 65535
##############################################################################

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
    def __init__(self, contents, onlyheaders=False):
        lines = contents.splitlines()
        blocks = parse_sections(lines)

        sections = {
                'intro': blocks[0],
                'whitelist': blocks[1],
                'anonymization': blocks[2],
                'packet_series': blocks[3],
                'flow_table': blocks[4],
                'dns_table_a': blocks[5],
                'dns_table_cname': blocks[6],
                'address_table': blocks[7]
                }
        try:
            sections['drop_statistics'] = blocks[8]
        except IndexError:
            pass
        try:
            sections['http_urls'] = blocks[9]
        except IndexError:
            pass

        self.file_format_version = int(sections['intro'][0])
        self.build_id = sections['intro'][1]
        intro_ids = sections['intro'][2].split()
        self.bismark_id = intro_ids[0]
        self.creation_time = int(intro_ids[1])
        self.sequence_number = int(intro_ids[2])
        self.timestamp = datetime.datetime.utcfromtimestamp(int(intro_ids[3]))
        if len(sections['intro']) >= 4:
            intro_stats = [ int(w) for w in sections['intro'][3].split() ]
            self.pcap_received = intro_stats[0]
            self.pcap_dropped = intro_stats[1]
            self.iface_dropped = intro_stats[2]

        self.whitelist = list(sections['whitelist'])

        if sections['anonymization'][0] == 'UNANONYMIZED':
            self.anonymized = False
        else:
            self.anonymized = True
            self.anonymization_signature = sections['anonymization'][0]

        if onlyheaders:
            return

        packet_stats = [ int(w) for w in sections['packet_series'][0].split() ]
        current_timestamp = packet_stats[0]
        self.packet_series_dropped = packet_stats[1]
        self.packet_series = []
        for line in sections['packet_series'][1:]:
            offset, size, flow_id = [ int(w) for w in line.split() ]
            current_timestamp += offset
            self.packet_series.append(PacketEntry(
                timestamp = datetime.datetime.utcfromtimestamp(
                    current_timestamp / 1e6),
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
            try:
                packet_id, address_id, \
                        domain_anonymized, domain, \
                        cname_anonymized, cname, ttl \
                        = line.split()
            except ValueError:
                packet_id, address_id, domain_anonymized, domain, cname, ttl \
                        = line.split()
                cname_anonymized = domain_anonymized

            self.cname_records.append(DnsCnameEntry(
                packet_id = int(packet_id),
                address_id = int(address_id),
                domain_anonymized = int(domain_anonymized),
                domain = domain,
                cname_anonymized = int(cname_anonymized),
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

        if 'drop_statistics' in sections:
            self.dropped_packets = {}
            for idx, line in enumerate(sections['drop_statistics']):
                try:
                    size, count = line.split()
                except ValueError:
                    if self.file_format_version <= 3:
                        sections['http_urls'] = \
                                sections['drop_statistics'][idx:]
                        break
                    else:
                        raise
                self.dropped_packets[int(size)] = int(count)

        if 'http_urls' in sections and sections['http_urls'] != []:
            try:
                self.dropped_http_urls = int(sections['http_urls'][0])
                self.http_urls = []
                for line in sections['http_urls'][1:]:
                    flow_id, _, hashed_url = line.split()
                    self.http_urls.append(HttpUrlEntry(
                        flow_id = int(flow_id),
                        hashed_url = hashed_url,
                        ))
            except:
                if self.file_format_version <= 3:
                    pass
                else:
                    raise
