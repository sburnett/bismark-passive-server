from collections import defaultdict
from datetime import datetime, timedelta
from matplotlib.dates import DateFormatter, MinuteLocator
from matplotlib.ticker import FixedFormatter
import matplotlib.pyplot as plt
from os.path import join

from correlation_processor import MacAddressCorrelationSessionProcessor
import harness
from node_plot import NodePlotHarness
from session_processor import PersistentSessionProcessor
from update_statistics_processor import DataAvailabilityProcessor

focus_ports = [22, 53, 80, 443]
focus_ports_set = set(focus_ports)
#focus_ports = set()
#lower_bound = datetime.min
lower_bound = datetime(2012, 1, 19, 2)
#upper_bound = datetime.max
upper_bound = datetime(2012, 1, 19, 3)
focus_node_id = 'OWC43DC7B0AE78'

class ConcurrentFlowsProcessor(PersistentSessionProcessor):
    def initialize_context(self, context):
        context.flow_time_intervals = defaultdict(list)

    def process_update_persistent(self, context, update):
        for flow in update.flow_table:
            if flow.source_ip in context.ip_to_mac_address_map \
                    and flow.destination_port in focus_ports_set:
                port = flow.destination_port
            elif flow.destination_ip in context.ip_to_mac_address_map \
                    and flow.source_port in focus_ports_set:
                port = flow.source_port
            else:
                port = None
            context.flow_time_intervals[flow.flow_id].append((None, None, port))
        for packet in update.packet_series:
            if packet.flow_id not in context.flow_time_intervals:
                continue
            lower, upper, port = context.flow_time_intervals[packet.flow_id][-1]
            if lower is None:
                lower = packet.timestamp
            upper = packet.timestamp
            context.flow_time_intervals[packet.flow_id][-1] = (lower, upper, port)

    def initialize_global_context(self, global_context):
        global_context.flows_per_minute = defaultdict(int)

    def merge_contexts_persistent(self, context, global_context):
        if context.node_id != focus_node_id:
            return
        for flow_id, intervals in context.flow_time_intervals.items():
            for lower, upper, port in intervals:
                if lower is None:
                    continue
                lower = max(lower, lower_bound)
                upper = min(upper, upper_bound)
                current = lower.replace(microsecond=0, second=0, minute=lower.minute - (lower.minute%5))
                while current < upper:
                    global_context.flows_per_minute[context.node_id, port, current] += 1
                    current += timedelta(minutes=5)

class ConcurrentFlowsHarness(NodePlotHarness):
    processors = [
            DataAvailabilityProcessor,
            MacAddressCorrelationSessionProcessor,
            ConcurrentFlowsProcessor,
            ]

    def plot_hourly(self, context, node_id, ax, limits):
        bottoms = defaultdict(int)
        hatches = ['/', None, '\\', None, 'x']
        colors = ['w', 'w', 'w', 'k', 'w']
        labels = {None: 'etc',
                  80: 'HTTP(80)',
                  443: 'HTTPS(443)',
                  53: 'DNS',
                  22: 'SSH'}
        for port, hatch, color in zip(focus_ports + [None], hatches, colors):
            points = self.concurrent_flows[node_id][port]
            if points != []:
                xs, ys = zip(*sorted(points))
                ax.bar(xs, ys, width=1/700.,
                        label=labels[port],
                        align='center',
                        color=color,
                        bottom=map(lambda x: bottoms[x], xs),
                        hatch=hatch)
                for timestamp, count in points:
                    bottoms[timestamp] += count
        ax.xaxis.set_major_locator(MinuteLocator(byminute=range(0, 60, 5)))
        ax.xaxis.set_major_formatter(FixedFormatter(map(lambda n: '%d-%d' % (n, n+5), range(0, 60, 5))))
        plt.setp(ax.get_xticklabels(), fontsize=10)
        plt.xlabel('Minutes (from 21:00 to 22:00) on Jan 18, 2012')
        plt.ylabel('Number of flows')
        ax.legend(prop=dict(size=12))
        ax.set_ylim(bottom=0)

    def process_results(self, global_context):
        self.concurrent_flows = defaultdict(lambda: defaultdict(list))
        for (node_id, port, timestamp), count in global_context.flows_per_minute.items():
            self.concurrent_flows[node_id][port].append((timestamp, count))
        node_ids = self.concurrent_flows.keys()
        self.plot(self.plot_hourly, global_context, 'concurrent_flows.eps', node_ids,
                figsize=(8,6),
                limits=(lower_bound - timedelta(minutes=4), upper_bound - timedelta(minutes=1)),
                title=False,
                availability=False,
                timestamp=False,
                autoformat=False)

if __name__ == '__main__':
    harness.main(ConcurrentFlowsHarness)
