import parser

import calendar
import unittest

def format_source(source):
    return '\n'.join([w.strip() for w in source.splitlines()])

class TestParser(unittest.TestCase):
    def test_unanonymized_header(self):
        source = """9
                    BUILDID
                    BISMARKID 1234567890 12 98765


                    UNANONYMIZED

                    0 0

                    0 0 0 0

                    0 0


                    0 0"""
        update = parser.PassiveUpdate(format_source(source))
        self.assertTrue(update.file_format_version == 9)
        self.assertTrue(update.build_id == 'BUILDID')
        self.assertTrue(update.bismark_id == 'BISMARKID')
        self.assertTrue(update.creation_time == 1234567890)
        self.assertTrue(update.sequence_number == 12)
        self.assertTrue(calendar.timegm(update.timestamp.timetuple()) == 98765)
        self.assertFalse(update.anonymized)

    def test_anonymized_header(self):
        source = """9
                    BUILDID
                    BISMARKID 1234567890 12 98765


                    KEY

                    0 0

                    0 0 0 0

                    0 0


                    0 0"""
        update = parser.PassiveUpdate(format_source(source))
        self.assertTrue(update.file_format_version == 9)
        self.assertTrue(update.build_id == 'BUILDID')
        self.assertTrue(update.bismark_id == 'BISMARKID')
        self.assertTrue(update.creation_time == 1234567890)
        self.assertTrue(update.sequence_number == 12)
        self.assertTrue(calendar.timegm(update.timestamp.timetuple()) == 98765)
        self.assertTrue(update.anonymized)
        self.assertTrue(update.anonymization_signature == 'KEY')

    def test_header_stats(self):
        source = """0
                    BUILDID
                    BISMARKID 0 0 0
                    12 21 43


                    KEY

                    0 0

                    0 0 0 0

                    0 0


                    0 0"""
        update = parser.PassiveUpdate(format_source(source))
        self.assertTrue(update.pcap_received == 12)
        self.assertTrue(update.pcap_dropped == 21)
        self.assertTrue(update.iface_dropped == 43)
        self.assertTrue(update.anonymized)

    def test_packet_series(self):
        source = """0
                    BUILDID
                    BISMARKID 0 0 0


                    UNANONYMIZED

                    100 123
                    0 15 1
                    10 40 1
                    5 1024 2

                    0 0 0 0

                    0 0


                    0 0"""
        update = parser.PassiveUpdate(format_source(source))
        self.assertTrue(update.packet_series_dropped == 123)
        self.assertTrue(len(update.packet_series) == 3)
        self.assertTrue(update.packet_series[0].timestamp.microsecond == 100)
        self.assertTrue(update.packet_series[0].size == 15)
        self.assertTrue(update.packet_series[0].flow_id == 1)
        self.assertTrue(update.packet_series[1].timestamp.microsecond == 110)
        self.assertTrue(update.packet_series[1].size == 40)
        self.assertTrue(update.packet_series[1].flow_id == 1)
        self.assertTrue(update.packet_series[2].timestamp.microsecond == 115)
        self.assertTrue(update.packet_series[2].size == 1024)
        self.assertTrue(update.packet_series[2].flow_id == 2)

    def test_flow_table(self):
        source = """0
                    BUILDID
                    BISMARKID 0 0 0


                    UNANONYMIZED

                    0 0

                    100 500 12 34
                    29 0 987 0 456 23 45 56
                    32 0 1004 0 433 30 26 31

                    0 0


                    0 0"""
        update = parser.PassiveUpdate(format_source(source))
        self.assertTrue(update.flow_table_baseline == 100)
        self.assertTrue(update.flow_table_size == 500)
        self.assertTrue(update.flow_table_expired == 12)
        self.assertTrue(update.flow_table_dropped == 34)
        self.assertTrue(len(update.flow_table) == 2)
        self.assertTrue(update.flow_table[0].flow_id == 29)
        self.assertTrue(update.flow_table[0].source_ip_anonymized == 0)
        self.assertTrue(update.flow_table[0].source_ip == '987')
        self.assertTrue(update.flow_table[0].destination_ip_anonymized == 0)
        self.assertTrue(update.flow_table[0].destination_ip == '456')
        self.assertTrue(update.flow_table[0].transport_protocol == 23)
        self.assertTrue(update.flow_table[0].source_port == 45)
        self.assertTrue(update.flow_table[0].destination_port == 56)
        self.assertTrue(update.flow_table[1].flow_id == 32)
        self.assertTrue(update.flow_table[1].source_ip == '1004')
        self.assertTrue(update.flow_table[1].destination_ip == '433')
        self.assertTrue(update.flow_table[1].transport_protocol == 30)
        self.assertTrue(update.flow_table[1].source_port == 26)
        self.assertTrue(update.flow_table[1].destination_port == 31)

    def test_dns_table(self):
        source = """0
                    BUILDID
                    BISMARKID 0 0 0


                    UNANONYMIZED

                    0 0

                    0 0 0 0

                    5 6
                    9 12 0 foo.com 123cd 2
                    8 34 1 bar.org ae321 34

                    7 45 1 blah.cn blorg.us 93
                    6 56 0 gorp.com boink.ca 28

                    0 0"""
        update = parser.PassiveUpdate(format_source(source))
        self.assertTrue(update.dropped_a_records == 5)
        self.assertTrue(update.dropped_cname_records == 6)
        self.assertTrue(len(update.a_records) == 2)
        self.assertTrue(update.a_records[0].packet_id == 9)
        self.assertTrue(update.a_records[0].address_id == 12)
        self.assertTrue(update.a_records[0].anonymized == 0)
        self.assertTrue(update.a_records[0].domain == 'foo.com')
        self.assertTrue(update.a_records[0].ip_address == '123cd')
        self.assertTrue(update.a_records[0].ttl.seconds == 2)
        self.assertTrue(update.a_records[1].packet_id == 8)
        self.assertTrue(update.a_records[1].address_id == 34)
        self.assertTrue(update.a_records[1].anonymized == 1)
        self.assertTrue(update.a_records[1].domain == 'bar.org')
        self.assertTrue(update.a_records[1].ip_address == 'ae321')
        self.assertTrue(update.a_records[1].ttl.seconds == 34)
        self.assertTrue(len(update.cname_records) == 2)
        self.assertTrue(update.cname_records[0].packet_id == 7)
        self.assertTrue(update.cname_records[0].address_id == 45)
        self.assertTrue(update.cname_records[0].anonymized == 1)
        self.assertTrue(update.cname_records[0].domain == 'blah.cn')
        self.assertTrue(update.cname_records[0].cname == 'blorg.us')
        self.assertTrue(update.cname_records[0].ttl.seconds == 93)
        self.assertTrue(update.cname_records[1].packet_id == 6)
        self.assertTrue(update.cname_records[1].address_id == 56)
        self.assertTrue(update.cname_records[1].anonymized == 0)
        self.assertTrue(update.cname_records[1].domain == 'gorp.com')
        self.assertTrue(update.cname_records[1].cname == 'boink.ca')
        self.assertTrue(update.cname_records[1].ttl.seconds == 28)

    def test_address_table(self):
        source = """0
                    BUILDID
                    BISMARKID 0 0 0


                    UNANONYMIZED

                    0 0

                    0 0 0 0

                    0 0


                    1 2
                    ABCDEF 1234ab
                    FEDCBA ba4321
                    """
        update = parser.PassiveUpdate(format_source(source))
        self.assertTrue(update.address_table_first_id == 1)
        self.assertTrue(update.address_table_size == 2)
        self.assertTrue(len(update.addresses) == 2)
        self.assertTrue(update.addresses[0].mac_address == 'ABCDEF')
        self.assertTrue(update.addresses[0].ip_address == '1234ab')
        self.assertTrue(update.addresses[1].mac_address == 'FEDCBA')
        self.assertTrue(update.addresses[1].ip_address == 'ba4321')

    def test_whitelist(self):
        source = """0
                    BUILDID
                    BISMARKID 1234567890 12 0

                    foo.com
                    bar.org

                    UNANONYMIZED

                    0 0

                    0 0 0 0

                    0 0


                    0 0"""
        update = parser.PassiveUpdate(format_source(source))
        self.assertTrue(update.whitelist == ['foo.com', 'bar.org'])

if __name__ == '__main__':
    unittest.main()
