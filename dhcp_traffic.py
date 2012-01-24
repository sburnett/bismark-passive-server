from collections import defaultdict 
from datetime import date, datetime, time
from itertools import chain
import matplotlib.pyplot as plt
from os.path import join
from socket import IPPROTO_UDP

from correlation_processor import FlowCorrelationSessionProcessor
import harness
from node_plot import NodePlotHarness
from session_processor import PersistentSessionProcessor
from update_statistics_processor import DataAvailabilityProcessor

class DhcpSessionProcessor(PersistentSessionProcessor):
    def initialize_context(self, context):
        context.dhcp_packet_timestamps = list()

    def process_update_persistent(self, context, update):
        for packet in update.packet_series:
            try:
                flow = context.flows[packet.flow_id]
            except:
                continue

            if flow.transport_protocol != IPPROTO_UDP:
                continue

            if (flow.source_port == 67 and flow.destination_port == 68) \
                    or (flow.source_port == 68 and flow.destination_port == 67):
                context.dhcp_packet_timestamps.append(packet.timestamp)

    def initialize_global_context(self, global_context):
        global_context.dhcp_packet_timestamps = defaultdict(list)

    def merge_contexts_persistent(self, context, global_context):
        global_context.dhcp_packet_timestamps[context.node_id].append(
                iter(context.dhcp_packet_timestamps))

class DhcpTrafficHarness(NodePlotHarness):
    processors = [
            DataAvailabilityProcessor,
            FlowCorrelationSessionProcessor,
            DhcpSessionProcessor,
            ]

    def plot_hourly(self, context, node_id, ax, limits):
        buckets = defaultdict(int)
        for timestamp in self.timestamps[node_id]:
            if (limits[0] is None or timestamp >= limits[0]) \
                    and (limits[1] is None or timestamp <= limits[1]):
                buckets[timestamp.replace(minute=0, second=0, microsecond=0)] += 1
        if len(buckets) > 0:
            xs, ys = zip(*sorted(buckets.items()))
            ax.scatter(xs, ys, marker='.')
        ax.set_ylim(bottom=0)

    def plot_daily(self, context, node_id, ax, limits):
        buckets = defaultdict(int)
        for timestamp in self.timestamps[node_id]:
            if (limits[0] is None or timestamp >= limits[0]) \
                    and (limits[1] is None or timestamp <= limits[1]):
                buckets[timestamp.date()] += 1
        if len(buckets) > 0:
            xs, ys = zip(*sorted(buckets.items()))
            ax.scatter(xs, ys, marker='.')
        ax.set_ylim(bottom=0)

    def process_results(self, global_context):
        self.timestamps = {}
        for node_id, timestamp_iters in global_context.dhcp_packet_timestamps.items():
            self.timestamps[node_id] = list(chain.from_iterable(timestamp_iters))
        node_ids = global_context.dhcp_packet_timestamps.keys()
        self.plot(self.plot_hourly, global_context, 'dhcp_packets_hourly.png', node_ids)
        self.plot(self.plot_daily, global_context, 'dhcp_packets_daily.png', node_ids)
        self.plot(self.plot_hourly, global_context, 'dhcp_packets_week.png', node_ids,
                limits=(datetime.combine(date(2011, 12, 25), time.min),
                        datetime.combine(date(2012, 1, 3), time.min)))

if __name__ == '__main__':
    harness.main(DhcpTrafficHarness)
