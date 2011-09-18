import importer

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

if __name__ == '__main__':
    unittest.main()
