import importer

from datetime import datetime
import parser
import schema
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import unittest

Base = declarative_base()

class DefaultUpdate(object):
    def __init__(self):
        self.bismark_id = 'BISMARKID'
        self.creation_time = 123
        self.sequence_number = 21
        self.pcap_received = 98
        self.pcap_dropped = 89
        self.iface_dropped = 99
        self.anonymized = True
        self.anonymization_signature = 'SIGNATURE'
        self.packet_series_dropped = 23
        self.packet_series = []
        self.flow_table_baseline = 34
        self.flow_table_size = 35
        self.flow_table_expired = 36
        self.flow_table_dropped = 37
        self.flow_table = []
        self.dropped_a_records = 41
        self.dropped_cname_records = 42
        self.a_records = []
        self.cname_records = []
        self.address_table_first_id = 51
        self.address_table_size = 52
        self.addresses = []

class AddressUpdate(DefaultUpdate):
    def __init__(self):
        super(AddressUpdate, self).__init__()
        self.addresses.append(parser.AddressEntry(mac_address='ABCDEF', ip_address='abc123'))
        self.addresses.append(parser.AddressEntry(mac_address='FEDCBA', ip_address='321cba'))

class DnsUpdate(AddressUpdate):
    def __init__(self):
        super(DnsUpdate, self).__init__()
        self.a_records.append(parser.DnsAEntry(address_id=self.address_table_first_id,
                                               domain='foo.com',
                                               ip_address='4321abc'))
        self.a_records.append(parser.DnsAEntry(address_id=self.address_table_first_id,
                                               domain='bar.org',
                                               ip_address='abc1234'))
        self.cname_records.append(parser.DnsCnameEntry(address_id=self.address_table_first_id,
                                                       domain='gorp.com',
                                                       cname='foo.com'))
        self.a_records.append(parser.DnsAEntry(address_id=(self.address_table_first_id + 1) % self.address_table_size,
                                               domain='bar.us',
                                               ip_address='abc12345'))

class FlowUpdate(DnsUpdate):
    def __init__(self):
        super(FlowUpdate, self).__init__()
        self.flow_table.append(parser.FlowEntry(flow_id=38, source_ip='abc123', destination_ip='345fed', transport_protocol=31, source_port=32, destination_port=33))
        self.flow_table.append(parser.FlowEntry(flow_id=39, source_ip='543def', destination_ip='321cba', transport_protocol=34, source_port=35, destination_port=36))

class PacketUpdate(FlowUpdate):
    def __init__(self):
        super(PacketUpdate, self).__init__()
        self.packet_series.append(parser.PacketEntry(timestamp=201, size=202, flow_id=38))
        self.packet_series.append(parser.PacketEntry(timestamp=214, size=210, flow_id=38))
        self.packet_series.append(parser.PacketEntry(timestamp=225, size=221, flow_id=39))

class SecondUpdate(PacketUpdate):
    def __init__(self):
        super(SecondUpdate, self).__init__()
        self.bismark_id = 'BISMARKID'
        self.creation_time = 123
        self.sequence_number = 22
        self.pcap_received = 104
        self.packet_series = [parser.PacketEntry(timestamp=901, size=902, flow_id=38),
                              parser.PacketEntry(timestamp=910, size=911, flow_id=920)]
        self.flow_table = [parser.FlowEntry(flow_id=920, source_ip='abc123', destination_ip='987bcd', transport_protocol=921, source_port=922, destination_port=923)]
        self.a_records = [parser.DnsAEntry(address_id=0, domain='blarg.org', ip_address='bcd987')]
        self.cname_records = [parser.DnsCnameEntry(address_id=1, domain='bar.us', cname='bar.com')]
        self.address_table_first_id = 1
        self.address_table_size = 52
        self.addresses = [parser.AddressEntry(mac_address='9123BEAD', ip_address='9876543ab')]


class PassiveUpdateImporterTests(unittest.TestCase):
    def setUp(self):
        self.session = schema.init_session('sqlite:///:memory:')
        self.importer = importer.PassiveUpdateImporter(self.session)

    def test_default_update(self):
        update = DefaultUpdate()
        self.importer.import_update(update)

        my_node = self.session.query(schema.Node).one()
        self.assertTrue(my_node.name == update.bismark_id)
        my_anonymization_context = self.session.query(schema.AnonymizationContext).one()
        self.assertTrue(my_anonymization_context.node == my_node)
        self.assertTrue(my_anonymization_context.signature == update.anonymization_signature)
        my_session = self.session.query(schema.Session).one()
        self.assertTrue(my_session.key == update.creation_time)
        self.assertTrue(my_session.anonymization_context == my_anonymization_context)
        my_update = self.session.query(schema.Update).one()
        self.assertTrue(my_update.sequence_number == update.sequence_number)
        self.assertTrue(my_update.pcap_received == update.pcap_received)
        self.assertTrue(my_update.pcap_dropped == update.pcap_dropped)
        self.assertTrue(my_update.iface_dropped == update.iface_dropped)
        self.assertTrue(my_update.packet_series_dropped == update.packet_series_dropped)
        self.assertTrue(my_update.flow_table_size == update.flow_table_size)
        self.assertTrue(my_update.flow_table_expired == update.flow_table_expired)
        self.assertTrue(my_update.flow_table_dropped == update.flow_table_dropped)
        self.assertTrue(my_update.dropped_a_records == update.dropped_a_records)
        self.assertTrue(my_update.dropped_cname_records == update.dropped_cname_records)
        self.assertTrue(my_update.session == my_session)

    def test_address_table(self):
        update = AddressUpdate()
        self.importer.import_update(update)

        first_address = self.session.query(schema.LocalAddress).filter_by(remote_id=update.address_table_first_id).one()
        second_address = self.session.query(schema.LocalAddress).filter_by(remote_id=(update.address_table_first_id + 1) % update.address_table_size).one()
        self.assertTrue(first_address.mac_address.data == update.addresses[0].mac_address)
        self.assertTrue(first_address.ip_address.data == update.addresses[0].ip_address)
        self.assertTrue(second_address.mac_address.data == update.addresses[1].mac_address)
        self.assertTrue(second_address.ip_address.data == update.addresses[1].ip_address)

    def test_dns_table(self):
        update = DnsUpdate()
        self.importer.import_update(update)

        first_address = self.session.query(schema.LocalAddress).filter_by(remote_id=update.address_table_first_id).one()
        second_address = self.session.query(schema.LocalAddress).filter_by(remote_id=(update.address_table_first_id + 1) % update.address_table_size).one()
        my_update = self.session.query(schema.Update).one()

        a_records = self.session.query(schema.DnsARecord)
        self.assertTrue(a_records[0].ip_address.data != a_records[1].ip_address.data and update.a_records[0].ip_address != update.a_records[1].ip_address)
        self.assertTrue(a_records[1].ip_address.data != a_records[2].ip_address.data and update.a_records[1].ip_address != update.a_records[2].ip_address)
        for a_record in a_records:
            self.assertTrue(a_record.update == my_update)
            self.assertTrue((a_record.ip_address.data == update.a_records[0].ip_address
                                and a_record.domain_name.data == update.a_records[0].domain
                                and a_record.local_address == first_address)
                    or (a_record.ip_address.data == update.a_records[1].ip_address
                                and a_record.domain_name.data == update.a_records[1].domain
                                and a_record.local_address == first_address)
                    or (a_record.ip_address.data == update.a_records[2].ip_address
                                and a_record.domain_name.data == update.a_records[2].domain
                                and a_record.local_address == second_address))
        cname_record = self.session.query(schema.DnsCnameRecord).one()
        self.assertTrue(cname_record.domain_name.data == update.cname_records[0].domain)
        self.assertTrue(cname_record.cname.data == update.cname_records[0].cname)
        self.assertTrue(cname_record.update == my_update)

    def test_flow_table(self):
        update = FlowUpdate()
        self.importer.import_update(update)

        first_flow = self.session.query(schema.Flow).filter_by(remote_id=update.flow_table[0].flow_id).one()
        second_flow = self.session.query(schema.Flow).filter_by(remote_id=update.flow_table[1].flow_id).one()
        self.assertTrue(first_flow != second_flow)

        self.assertTrue(first_flow.source_ip.data == update.flow_table[0].source_ip)
        self.assertTrue(first_flow.destination_ip.data == update.flow_table[0].destination_ip)
        self.assertTrue(first_flow.transport_protocol == update.flow_table[0].transport_protocol)
        self.assertTrue(first_flow.source_port == update.flow_table[0].source_port)
        self.assertTrue(first_flow.destination_port == update.flow_table[0].destination_port)
        self.assertTrue(first_flow.source_ip.local_addresses[0].mac_address.data == update.addresses[0].mac_address
                            and first_flow.source_ip.local_addresses[0].ip_address.data == update.addresses[0].ip_address)
        self.assertTrue(first_flow.destination_ip.local_addresses == [])

        self.assertTrue(second_flow.source_ip.data == update.flow_table[1].source_ip)
        self.assertTrue(second_flow.destination_ip.data == update.flow_table[1].destination_ip)
        self.assertTrue(second_flow.transport_protocol == update.flow_table[1].transport_protocol)
        self.assertTrue(second_flow.source_port == update.flow_table[1].source_port)
        self.assertTrue(second_flow.destination_port == update.flow_table[1].destination_port)
        self.assertTrue(second_flow.source_ip.local_addresses == [])
        self.assertTrue(second_flow.destination_ip.local_addresses[0].mac_address.data == update.addresses[1].mac_address
                            and second_flow.destination_ip.local_addresses[0].ip_address.data == update.addresses[1].ip_address)

    def test_packet_series(self):
        update = PacketUpdate()
        self.importer.import_update(update)

        packets = [ self.session.query(schema.Packet).filter_by(timestamp=datetime.utcfromtimestamp(update.packet_series[i].timestamp / 1e6)).one() for i in range(3) ]
        for packet, source_packet in zip(packets, update.packet_series):
            self.assertTrue(packet.timestamp == datetime.utcfromtimestamp(source_packet.timestamp / 1e6))
            self.assertTrue(packet.size == source_packet.size)
            self.assertTrue(packet.flow.remote_id == source_packet.flow_id)

    def test_multiple_updates(self):
        first_update = PacketUpdate()
        second_update = SecondUpdate()
        self.importer.import_update(first_update)
        self.importer.import_update(second_update)

        self.assertTrue(self.session.query(schema.Node).one())
        self.assertTrue(self.session.query(schema.AnonymizationContext).one())
        self.assertTrue(self.session.query(schema.Session).one())

        my_first_update = self.session.query(schema.Update).filter_by(sequence_number=first_update.sequence_number).one()
        my_second_update = self.session.query(schema.Update).filter_by(sequence_number=second_update.sequence_number).one()
        self.assertTrue(my_first_update != my_second_update)

        my_address = self.session.query(schema.LocalAddress).filter_by(update=my_second_update).one()
        self.assertTrue(my_address.ip_address.data == second_update.addresses[0].ip_address)

        my_a_record = self.session.query(schema.DnsARecord).filter_by(update=my_second_update).one()
        self.assertTrue(my_a_record.local_address.ip_address.data == first_update.addresses[1].ip_address)
        self.assertTrue(my_a_record.local_address.mac_address.data == first_update.addresses[1].mac_address)

        my_cname_record = self.session.query(schema.DnsCnameRecord).filter_by(update=my_second_update).one()
        self.assertTrue(my_cname_record.local_address.ip_address.data == second_update.addresses[0].ip_address)
        self.assertTrue(my_cname_record.local_address.mac_address.data == second_update.addresses[0].mac_address)
        self.assertTrue(my_cname_record.domain_name.dns_a_records[0].ip_address.data == first_update.a_records[2].ip_address)

        my_flow_record = self.session.query(schema.Flow).filter_by(update=my_second_update).one()
        self.assertTrue(my_flow_record.source_ip.local_addresses[0].mac_address.data == first_update.addresses[0].mac_address)

        my_other_flow = self.session.query(schema.Flow).filter_by(remote_id=first_update.flow_table[0].flow_id).one()
        self.assertTrue(self.session.query(schema.Packet).filter_by(flow=my_other_flow, update=my_second_update).one())
        self.assertTrue(self.session.query(schema.Packet).filter_by(flow=my_flow_record, update=my_second_update).one())

if __name__ == '__main__':
    unittest.main()
