from datetime import datetime, timedelta
from matplotlib.dates import AutoDateFormatter, AutoDateLocator
import matplotlib.pyplot as plt
import pytz

from byte_count_processor import BytesPerHourSessionProcessor, BytesPerDaySessionProcessor, BytesPerPortPerHourSessionProcessor
from correlation_processor import FlowCorrelationSessionProcessor
import harness
from node_plot import PlotPerNodeHarness
from update_statistics_processor import DataAvailabilityProcessor

class TrafficPlotHarness(PlotPerNodeHarness):
    processors = [
            DataAvailabilityProcessor,
            FlowCorrelationSessionProcessor,
            BytesPerHourSessionProcessor,
            BytesPerDaySessionProcessor,
            BytesPerPortPerHourSessionProcessor,
            ]

    def plot_daily(self, context, node_id, limits):
        plt.yscale('log', basey=1000, subsy=range(100, 1000, 100))
        plt.yticks([10**0, 10**3, 10**6, 10**9, 10**12, 10**15, 10**18],
                   ['1 B', '1 KB', '1 MB', '1 GB', '1 TB', '1 PB', '1 EB'])
        loc = AutoDateLocator(tz=pytz.timezone('US/Eastern'))
        plt.gca().xaxis.set_major_locator(loc)
        plt.gca().xaxis.set_major_formatter(AutoDateFormatter(loc))
        plt.bar(*zip(*context.bytes_per_day[node_id].items()),
                color='#50a050',
                linewidth=0,
                width=1)
        plt.xlabel('Time in EST/EDT')
        plt.ylabel('Bytes transferred per day')
        plt.ylim(bottom=1)

    def plot_hourly(self, context, node_id, limits):
        plt.yscale('log', basey=1000, subsy=range(100, 1000, 100))
        plt.yticks([10**0, 10**3, 10**6, 10**9, 10**12, 10**15, 10**18],
                   ['1 B', '1 KB', '1 MB', '1 GB', '1 TB', '1 PB', '1 EB'])
        loc = AutoDateLocator(tz=pytz.timezone('US/Eastern'))
        plt.gca().xaxis.set_major_locator(loc)
        plt.gca().xaxis.set_major_formatter(AutoDateFormatter(loc))
        plt.grid(b=True, axis='x')
        plt.grid(b=True, axis='y', which='both')
        plt.bar(*zip(*context.bytes_per_hour[node_id].items()),
                color='#50a050',
                linewidth=0,
                width=1/24.,
                label='total')
        if 80 in context.bytes_per_port_per_hour[node_id]:
            plt.bar(*zip(*context.bytes_per_port_per_hour[node_id][80].items()),
                    color='#5050a0',
                    linewidth=0,
                    width=1/24.,
                    label='HTTP')
        plt.xlabel('Time in EST/EDT')
        plt.ylabel('Bytes transferred per hour')
        plt.legend(prop=dict(size=10))
        plt.ylim(bottom=1)

    def process_results(self, global_context):
        node_ids = global_context.bytes_per_day.keys()
        self.plot(self.plot_daily, global_context, 'overall', node_ids)

        eastern = pytz.timezone('US/Eastern')
        start_time = eastern.localize(datetime(2011, 10, 3))
        end_time = eastern.localize(datetime(2012, 1, 26))

        current_time = start_time
        while current_time < end_time:
            node_ids = global_context.bytes_per_hour.keys()
            self.plot(self.plot_hourly,
                    global_context,
                    current_time.strftime('weekly_%Y-%m-%d'),
                    node_ids,
                    limits=(current_time, current_time + timedelta(weeks=1)))
            current_time += timedelta(weeks=1)

if __name__ == '__main__':
    harness.main(TrafficPlotHarness)
