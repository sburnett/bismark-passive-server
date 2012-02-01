from collections import defaultdict
from datetime import datetime
from itertools import imap
import locale

import harness
from session_processor import PersistentSessionProcessor

class AggregateStatisticsProcessor(PersistentSessionProcessor):
    def initialize_context(self, context):
        context.number_of_updates = 0
        context.number_of_packets = 0
        context.number_of_bytes = 0
        context.number_of_flows = 0
        context.number_of_dropped_packets = 0
        context.number_of_dropped_packets_sys = 0
        context.number_of_dropped_flows = 0

    def process_update_persistent(self, context, update):
        context.number_of_updates += 1
        context.number_of_packets += len(update.packet_series)
        context.number_of_bytes += \
                sum(imap(lambda p: p.size, update.packet_series))
        context.number_of_flows += len(update.flow_table)
        context.number_of_dropped_packets += update.packet_series_dropped
        context.number_of_dropped_packets_sys = update.pcap_dropped + update.iface_dropped
        context.number_of_dropped_flows = update.flow_table_dropped

    def initialize_global_context(self, global_context):
        global_context.number_of_updates = defaultdict(int)
        global_context.number_of_packets = defaultdict(int)
        global_context.number_of_bytes = defaultdict(int)
        global_context.number_of_flows = defaultdict(int)
        global_context.number_of_dropped_packets = defaultdict(int)
        global_context.number_of_dropped_flows = defaultdict(int)

    def merge_contexts_persistent(self, context, global_context):
        global_context.number_of_updates[context.node_id] \
                += context.number_of_updates
        global_context.number_of_packets[context.node_id] \
                += context.number_of_packets
        global_context.number_of_bytes[context.node_id] \
                += context.number_of_bytes
        global_context.number_of_flows[context.node_id] \
                += context.number_of_flows
        global_context.number_of_dropped_packets[context.node_id] \
                += context.number_of_dropped_packets \
                + context.number_of_dropped_packets_sys
        global_context.number_of_dropped_flows[context.node_id] \
                += context.number_of_dropped_flows

class AggregateStatisticsTableHarness(harness.Harness):
    processors = [AggregateStatisticsProcessor]

    @staticmethod
    def setup_options(parser):
        harness.Harness.setup_options(parser)
        parser.add_option('--output_page', action='store',
                          default='/tmp/statistics_table.html',
                          help='Path of HTML file to produce')

    def process_results(self, global_context):
        locale.setlocale(locale.LC_ALL, '')

        h = open(harness.options.output_page, 'w')
        print >>h, '<html><body>'
        print >>h, '<style type="text/css">'
        print >>h, 'td { padding: 5px; text-align: right; font-family: monospace; border: 1px solid #bbb; }'
        print >>h, 'th { padding: 5px; text-align: right; border: 1px solid #bbb; }'
        print >>h, '</style>'
        print >>h, '<table>'
        print >>h, '<tr>'
        print >>h, '<th></th>'
        print >>h, '<th>Updates</th>'
        print >>h, '<th>Packets</th>'
        print >>h, '<th>Dropped packets</th>'
        print >>h, '<th>Flows</th>'
        print >>h, '<th>Dropped flows</th>'
        print >>h, '<th>Bytes</th>'
        print >>h, '</tr>'
        for node_id in sorted(global_context.number_of_updates.keys()):
            print >>h, '<tr>'
            print >>h, '<td><tt>%s</tt></td>' % node_id
            print >>h, locale.format_string('<td>%d</td>',
                                            global_context.number_of_updates[node_id],
                                            grouping=True)
            print >>h, locale.format_string('<td>%d</td>',
                                            global_context.number_of_packets[node_id],
                                            grouping=True)
            print >>h, locale.format_string('<td>%d</td>',
                                            global_context.number_of_dropped_packets[node_id],
                                            grouping=True)
            print >>h, locale.format_string('<td>%d</td>',
                                            global_context.number_of_flows[node_id],
                                            grouping=True)
            print >>h, locale.format_string('<td>%d</td>',
                                            global_context.number_of_dropped_flows[node_id],
                                            grouping=True)
            print >>h, locale.format_string('<td>%d</td>',
                                            global_context.number_of_bytes[node_id],
                                            grouping=True)
            print >>h, '</tr>'
        print >>h, '<tr>'
        print >>h, '<td><b>Total</b></td>'
        print >>h, locale.format_string('<td><b>%d</b></td>',
                                        sum(global_context.number_of_updates.values()),
                                        grouping=True)
        print >>h, locale.format_string('<td><b>%d</b></td>',
                                        sum(global_context.number_of_packets.values()),
                                        grouping=True)
        print >>h, locale.format_string('<td><b>%d</b></td>',
                                        sum(global_context.number_of_dropped_packets.values()),
                                        grouping=True)
        print >>h, locale.format_string('<td><b>%d</b></td>',
                                        sum(global_context.number_of_flows.values()),
                                        grouping=True)
        print >>h, locale.format_string('<td><b>%d</b></td>',
                                        sum(global_context.number_of_dropped_flows.values()),
                                        grouping=True)
        print >>h, locale.format_string('<td><b>%d</b></td>',
                                        sum(global_context.number_of_bytes.values()),
                                        grouping=True)
        print >>h, '</tr>'
        print >>h, '</table>'
        print >>h, '<small>Generated %s</small>' % datetime.now()
        print >>h, '</body></html>'

if __name__ == '__main__':
    harness.main(AggregateStatisticsTableHarness)
