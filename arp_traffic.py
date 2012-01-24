from collections import defaultdict
from itertools import chain
from os.path import join
import matplotlib.pyplot as plt

from correlation_processor import FlowCorrelationSessionProcessor
import harness
from session_processor import PersistentSessionProcessor
from update_parser import ReservedFlowIndices

class ArpSessionProcessor(PersistentSessionProcessor):
    def initialize_context(self, context):
        context.packet_timestamps = list()

    def process_update_persistent(self, context, update):
        for packet in update.packet_series:
            try:
                flow = context.flows[packet.flow_id]
            except KeyError:
                continue

            if flow.transport_protocol == ReservedFlowIndices.FLOW_ID_ARP:
                context.packet_timestamps.append(packet.timestamp)

    def initialize_global_context(self, global_context):
        global_context.packet_timestamps = defaultdict(list)

    def merge_contexts_persistent(self, context, global_context):
        global_context.packet_timestamps[context.node_id].append(
                iter(context.packet_timestamps))

class ArpTrafficHarness(harness.Harness):
    processors = [FlowCorrelationSessionProcessor, ArpSessionProcessor]

    def process_results(self, global_context):
        node_ids = list(global_context.packet_timestamps.keys())
        fig = plt.figure(1, figsize=(15, 20))
        for y, node_id in enumerate(node_ids):
            try:
                ax = plt.subplot(len(node_ids), 1, y, sharex=first_axis)
            except NameError:
                first_axis = ax = plt.subplot(len(node_ids), 1, y)
            ax.set_ylabel(node_id, family='monospace', rotation='horizontal')
            timestamp_iters = global_context.packet_timestamps[node_id]
            timestamps = list(chain.from_iterable(timestamp_iters))
            buckets = defaultdict(int)
            for timestamp in timestamps:
                buckets[timestamp.replace(minute=0, second=0, microsecond=0)] += 1
            xs, ys = zip(*sorted(buckets.items()))
            ax.scatter(xs, ys, marker='.')
            ax.set_ylim(bottom=0)
        fig.autofmt_xdate(bottom=0)
        plt.tight_layout(h_pad=0)
        plt.savefig(join(harness.options.plots_directory, 'arp_packets.png'))

if __name__ == '__main__':
    harness.main(ArpTrafficHarness)
