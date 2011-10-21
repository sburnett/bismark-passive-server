from bytes_computation import BytesSessionProcessor

from datetime import datetime, timedelta
import update_parser
import unittest

class MockPassiveUpdate(object):
    def __init__(self,
                 packet_series,
                 flow_table=None,
                 addresses=None,
                 whitelist=None,
                 a_records=None,
                 cname_records=None,
                 sequence_number=0
                 ):
        if whitelist is None:
            self.whitelist = []
        else:
            self.whitelist = whitelist
        self.packet_series = packet_series
        if addresses is None:
            self.addresses = []
        else:
            self.addresses = addresses
        self.address_table_first_id = 0
        self.address_table_size = 256
        if flow_table is None:
            self.flow_table = []
        else:
            self.flow_table = flow_table
        if a_records is None:
            self.a_records = []
        else:
            self.a_records = a_records
        if cname_records is None:
            self.cname_records = []
        else:
            self.cname_records = cname_records
        self.sequence_number = sequence_number

class TestBytesSessionProcessor(unittest.TestCase):
    def test_bytes(self):
        timestamp = datetime(1970, 1, 1, 0, 0, 0)
        packets = [ update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=12),
                        size=10,
                        flow_id=-1),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=23),
                        size=23,
                        flow_id=-2),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=59),
                        size=2,
                        flow_id=-2) ]
        update = MockPassiveUpdate(packets)
        processor = BytesSessionProcessor()
        processor.process_update(update)

        bytes_per_minute = processor.results['bytes_per_minute']
        self.assertTrue(bytes_per_minute[datetime(1970, 1, 1)] == 35)
        self.assertTrue(len(bytes_per_minute) == 1)

    def test_bytes_minute(self):
        timestamp = datetime(1970, 1, 1, 0, 0, 0)
        packets = [ update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=12),
                        size=10,
                        flow_id=-1),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=23, minute=2),
                        size=23,
                        flow_id=-2),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=59, minute=2),
                        size=2,
                        flow_id=-3) ]
        update = MockPassiveUpdate(packets)
        processor = BytesSessionProcessor()
        processor.process_update(update)

        bytes_per_minute = processor.results['bytes_per_minute']
        self.assertTrue(bytes_per_minute[datetime(1970, 1, 1, 0, 0)] == 10)
        self.assertTrue(bytes_per_minute[datetime(1970, 1, 1, 0, 2)] == 25)
        self.assertTrue(len(bytes_per_minute) == 2)

    def test_bytes_port_minute(self):
        timestamp = datetime(1970, 1, 1, 0, 0, 0)
        packets = [ update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=12),
                        size=10,
                        flow_id=1),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=23),
                        size=23,
                        flow_id=1),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=59),
                        size=2,
                        flow_id=2),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(minute=4),
                        size=10,
                        flow_id=2),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(minute=4, second=1),
                        size=20,
                        flow_id=2) ]
        flow_entry = update_parser.FlowEntry(flow_id=-1,
                                      source_ip_anonymized=1,
                                      source_ip=0,
                                      destination_ip_anonymized=1,
                                      destination_ip=1,
                                      transport_protocol=0,
                                      source_port=0,
                                      destination_port=0)
        flows = [ flow_entry._replace(flow_id=1,
                                      source_port=1,
                                      destination_port=2),
                  flow_entry._replace(flow_id=2,
                                      source_port=4,
                                      source_ip=1,
                                      destination_ip=0,
                                      destination_port=3) ]
        addresses = [ update_parser.AddressEntry(ip_address=1, mac_address=0) ]

        update = MockPassiveUpdate(packets, flows, addresses)
        processor = BytesSessionProcessor()
        processor.process_update(update)

        bytes_per_port_per_minute \
                = processor.results['bytes_per_port_per_minute']
        self.assertTrue(
                bytes_per_port_per_minute[datetime(1970, 1, 1), 1] == 33)
        self.assertTrue(
                bytes_per_port_per_minute[datetime(1970, 1, 1), 3] == 2)
        self.assertTrue(
                bytes_per_port_per_minute[datetime(1970, 1, 1, 0, 4), 3] == 30)
        self.assertTrue(len(bytes_per_port_per_minute) == 3)

    def test_bytes_domain_minute(self):
        whitelist = [ 'foo.com', 'bar.org', 'gorp.net' ]
        timestamp = datetime(1970, 1, 1, 0, 0, 0)
        packets = [ update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=10),
                        size=10,
                        flow_id=-1),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=11),
                        size=2,
                        flow_id=0),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=11),
                        size=10,
                        flow_id=1),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=11),
                        size=21,
                        flow_id=2),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=11),
                        size=35,
                        flow_id=3),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(minute=3),
                        size=37,
                        flow_id=0),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(minute=3),
                        size=50,
                        flow_id=1),
                    ]
        flow_entry = update_parser.FlowEntry(flow_id=-1,
                                      source_ip_anonymized=1,
                                      source_ip=0,
                                      destination_ip_anonymized=1,
                                      destination_ip=1,
                                      transport_protocol=0,
                                      source_port=0,
                                      destination_port=0)
        flows = [ flow_entry._replace(flow_id=0,
                                      source_ip=1,
                                      destination_ip=23,
                                      destination_ip_anonymized=0),
                  flow_entry._replace(flow_id=1,
                                      source_ip=32,
                                      source_ip_anonymized=0,
                                      destination_ip=2),
                  flow_entry._replace(flow_id=2,
                                      source_ip=2,
                                      destination_ip=23,
                                      destination_ip_anonymized=0),
                  flow_entry._replace(flow_id=3,
                                      source_ip=32,
                                      source_ip_anonymized=0,
                                      destination_ip=1),
                ]
        a_entries = [ update_parser.DnsAEntry(packet_id=0,
                                       address_id=0,
                                       anonymized=0,
                                       domain='www.foo.com',
                                       ip_address=23,
                                       ttl=timedelta(minutes=10)),
                      update_parser.DnsAEntry(packet_id=0,
                                       address_id=1,
                                       anonymized=0,
                                       domain='www.bar.org',
                                       ip_address=32,
                                       ttl=timedelta(minutes=10))
                      ]
        cname_entries = [ update_parser.DnsCnameEntry(packet_id=0,
                                               address_id=0,
                                               anonymized=0,
                                               domain='www.gorp.net',
                                               cname='www.foo.com',
                                               ttl=timedelta(minutes=10)) ]
        addresses = [ update_parser.AddressEntry(ip_address=1, mac_address=-1),
                      update_parser.AddressEntry(ip_address=2, mac_address=-1) ]

        update = MockPassiveUpdate(packets,
                                   flows,
                                   addresses,
                                   whitelist,
                                   a_entries,
                                   cname_entries)
        processor = BytesSessionProcessor()
        processor.process_update(update)

        bytes_per_domain_per_minute \
                = processor.results['bytes_per_domain_per_minute']
        self.assertTrue(
                bytes_per_domain_per_minute[
                    datetime(1970, 1, 1), 'foo.com'] == 2)
        self.assertTrue(
                bytes_per_domain_per_minute[
                    datetime(1970, 1, 1), 'bar.org'] == 10)
        self.assertTrue(
                bytes_per_domain_per_minute[
                    datetime(1970, 1, 1), 'gorp.net'] == 2)
        self.assertTrue(
                bytes_per_domain_per_minute[
                    datetime(1970, 1, 1, 0, 3), 'foo.com'] == 37)
        self.assertTrue(
                bytes_per_domain_per_minute[
                    datetime(1970, 1, 1, 0, 3), 'gorp.net'] == 37)
        self.assertTrue(
                bytes_per_domain_per_minute[
                    datetime(1970, 1, 1, 0, 3), 'bar.org'] == 50)
        self.assertTrue(len(bytes_per_domain_per_minute) == 6)

    def test_bytes_domain_anonymized(self):
        whitelist = [ 'foo.com', 'bar.org', 'gorp.net' ]
        timestamp = datetime(1970, 1, 1, 0, 0, 0)
        packets = [ update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=10),
                        size=10,
                        flow_id=-1),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=11),
                        size=2,
                        flow_id=0),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=11),
                        size=10,
                        flow_id=1),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=11),
                        size=10,
                        flow_id=2),
                    ]
        flow_entry = update_parser.FlowEntry(flow_id=-1,
                                      source_ip_anonymized=1,
                                      source_ip=0,
                                      destination_ip_anonymized=1,
                                      destination_ip=1,
                                      transport_protocol=0,
                                      source_port=0,
                                      destination_port=0)
        flows = [ flow_entry._replace(flow_id=0,
                                      source_ip=1,
                                      destination_ip=23),
                  flow_entry._replace(flow_id=1,
                                      source_ip=32,
                                      destination_ip=2),
                  flow_entry._replace(flow_id=2,
                                      source_ip=32,
                                      source_ip_anonymized=0,
                                      destination_ip=2),
                ]
        a_entries = [ update_parser.DnsAEntry(packet_id=0,
                                       address_id=0,
                                       anonymized=0,
                                       domain='www.foo.com',
                                       ip_address=23,
                                       ttl=timedelta(seconds=10)),
                      update_parser.DnsAEntry(packet_id=0,
                                       address_id=1,
                                       anonymized=0,
                                       domain='www.bar.org',
                                       ip_address=32,
                                       ttl=timedelta(seconds=10)),
                      ]
        cname_entries = [ update_parser.DnsCnameEntry(packet_id=0,
                                               address_id=0,
                                               anonymized=0,
                                               domain='www.gorp.net',
                                               cname='www.foo.com',
                                               ttl=timedelta(seconds=10)),
                          update_parser.DnsCnameEntry(packet_id=0,
                                               address_id=1,
                                               anonymized=1,
                                               domain='www.gorp.net',
                                               cname='www.bar.org',
                                               ttl=timedelta(seconds=10)),
                                               ]
        addresses = [ update_parser.AddressEntry(ip_address=1, mac_address=-1),
                      update_parser.AddressEntry(ip_address=2, mac_address=-1)
                    ]

        update = MockPassiveUpdate(packets,
                                   flows,
                                   addresses,
                                   whitelist,
                                   a_entries,
                                   cname_entries)
        processor = BytesSessionProcessor()
        processor.process_update(update)

        bytes_per_domain_per_minute \
                = processor.results['bytes_per_domain_per_minute']
        self.assertTrue(
                bytes_per_domain_per_minute[
                    datetime(1970, 1, 1), 'bar.org'] == 10)
        self.assertTrue(len(bytes_per_domain_per_minute) == 1)

    def test_bytes_domain_expire(self):
        whitelist = [ 'foo.com', 'bar.org', 'gorp.net' ]
        timestamp = datetime(1970, 1, 1, 0, 0, 0)
        packets = [ update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=10),
                        size=10,
                        flow_id=-1),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=9),
                        size=1,
                        flow_id=0),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=11),
                        size=2,
                        flow_id=0),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=15),
                        size=4,
                        flow_id=0),
                    update_parser.PacketEntry(
                        timestamp=timestamp.replace(second=16),
                        size=8,
                        flow_id=0),
                    ]
        flow_entry = update_parser.FlowEntry(flow_id=-1,
                                      source_ip_anonymized=1,
                                      source_ip=0,
                                      destination_ip_anonymized=1,
                                      destination_ip=1,
                                      transport_protocol=0,
                                      source_port=0,
                                      destination_port=0)
        flows = [ flow_entry._replace(flow_id=0,
                                      source_ip=1,
                                      destination_ip=23,
                                      destination_ip_anonymized=0),
                ]
        a_entries = [ update_parser.DnsAEntry(packet_id=0,
                                       address_id=0,
                                       anonymized=0,
                                       domain='www.foo.com',
                                       ip_address=23,
                                       ttl=timedelta(seconds=5)),
                      ]
        cname_entries = [ update_parser.DnsCnameEntry(packet_id=0,
                                               address_id=0,
                                               anonymized=0,
                                               domain='www.gorp.net',
                                               cname='www.foo.com',
                                               ttl=timedelta(minutes=10)),
                          update_parser.DnsCnameEntry(packet_id=0,
                                               address_id=0,
                                               anonymized=0,
                                               domain='www.bar.org',
                                               cname='www.foo.com',
                                               ttl=timedelta(seconds=4)) ]
        addresses = \
                [ update_parser.AddressEntry(ip_address=1, mac_address=-1) ]

        update = MockPassiveUpdate(packets,
                                   flows,
                                   addresses,
                                   whitelist,
                                   a_entries,
                                   cname_entries)
        processor = BytesSessionProcessor()
        processor.process_update(update)

        bytes_per_domain_per_minute \
                = processor.results['bytes_per_domain_per_minute']
        self.assertTrue(
                bytes_per_domain_per_minute[
                    datetime(1970, 1, 1), 'foo.com'] == 6)
        self.assertTrue(
                bytes_per_domain_per_minute[
                    datetime(1970, 1, 1), 'gorp.net'] == 6)
        self.assertTrue(
                bytes_per_domain_per_minute[
                    datetime(1970, 1, 1), 'bar.org'] == 2)
        self.assertTrue(len(bytes_per_domain_per_minute) == 3)

if __name__ == '__main__':
    unittest.main()
