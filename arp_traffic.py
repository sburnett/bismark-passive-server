from collections import defaultdict
from itertools import chain
import matplotlib.pyplot as plt
from os.path import join

from correlation_processor import FlowCorrelationSessionProcessor
import harness
from node_plot import NodePlotHarness
from session_processor import PersistentSessionProcessor
from update_parser import ReservedFlowIndices
from update_statistics_processor import DataAvailabilityProcessor

class ArpSessionProcessor(PersistentSessionProcessor):
    def initialize_context(self, context):
        context.arp_packet_timestamps = list()

    def process_update_persistent(self, context, update):
        for packet in update.packet_series:
            if packet.flow_id == ReservedFlowIndices.FLOW_ID_ARP:
                context.arp_packet_timestamps.append(packet.timestamp)

    def initialize_global_context(self, global_context):
        global_context.arp_packet_timestamps = defaultdict(list)

    def merge_contexts_persistent(self, context, global_context):
        global_context.arp_packet_timestamps[context.node_id].append(
                iter(context.arp_packet_timestamps))

class ArpTrafficHarness(NodePlotHarness):
    processors = [
            DataAvailabilityProcessor,
            ArpSessionProcessor,
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
        for node_id, timestamp_iters in global_context.arp_packet_timestamps.items():
            self.timestamps[node_id] = list(chain.from_iterable(timestamp_iters))
        node_ids = global_context.arp_packet_timestamps.keys()
        self.plot(self.plot_hourly, global_context, 'arp_packets_hourly.png', node_ids)
        self.plot(self.plot_daily, global_context, 'arp_packets_daily.png', node_ids)

if __name__ == '__main__':
    harness.main(ArpTrafficHarness)