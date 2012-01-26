from collections import defaultdict
import datetime
from itertools import chain
from matplotlib.dates import AutoDateFormatter, AutoDateLocator, date2num
import matplotlib.pyplot as plt
from os.path import join

from harness import Harness, main
import harness
from update_statistics_processor import DataAvailabilityProcessor

OUTAGE_TIMEOUT = datetime.timedelta(seconds=60)

class PlotUpdateStatisticsHarness(Harness):
    processors = [DataAvailabilityProcessor]

    def plot_updates(self, availability_intervals, filename, max_age=None):
        current_time = datetime.datetime.utcnow()
        if max_age is None:
            oldest = datetime.datetime.min
        else:
            oldest = current_time - max_age
        
        outages = dict()
        for node_id, intervals in availability_intervals.items():
            outages[node_id] = map(lambda (lower, upper): upper, intervals)

        node_ids = sorted(availability_intervals.keys())
        indices = {}
        for node_id in node_ids:
            indices[node_id] = node_ids.index(node_id)

        availability_lines = []
        for node_id, intervals in availability_intervals.items():
            for lower, upper in intervals:
                availability_lines.append((indices[node_id], lower, upper))
        outage_points = []
        for node_id, outage_times in outages.items():
            for beginning in outage_times:
                outage_points.append((beginning,
                                      indices[node_id] - 0.3,
                                      indices[node_id] + 0.3))

        fig = plt.figure(figsize=(10,0.3*len(node_ids)))
        ax = fig.add_subplot(111)
        plt.yticks(range(len(node_ids)), node_ids, family='monospace')
        if len(availability_lines) > 0:
            ax.hlines(*zip(*availability_lines), lw=2)
        if len(outage_points) > 0:
            ax.vlines(*zip(*outage_points), lw=1.5, color='r')
        ax.set_xlabel('Date')
        ax.set_ylabel('Bismark Node ID')
        loc = AutoDateLocator()
        ax.xaxis.set_major_locator(loc)
        ax.xaxis.set_major_formatter(AutoDateFormatter(loc))
        if max_age is not None:
            plt.xlim(xmin=date2num(oldest))
        plt.xlim(xmax=date2num(current_time))
        plt.ylim(-1, len(node_ids))
        fig.autofmt_xdate()
        plt.text(1, 0, 'Generated at %s' % current_time,
                 fontsize=8,
                 color='gray',
                 transform=fig.transFigure,
                 verticalalignment='bottom',
                 horizontalalignment='right')
        if max_age is None:
            plt.title('Passive data availability for all time')
        else:
            plt.title('Passive data availability for the past %s' % max_age)
        plt.tight_layout()
        plt.savefig(filename)

    def process_results(self, global_context):
        print 'Plotting daily status'
        self.plot_updates(global_context.availability_intervals,
                          join(harness.options.plots_directory, 'status_daily.png'),
                          datetime.timedelta(days=1))
        print 'Plotting weekly status'
        self.plot_updates(global_context.availability_intervals,
                          join(harness.options.plots_directory, 'status_weekly.png'),
                          datetime.timedelta(days=7))
        print 'Plotting overall status'
        self.plot_updates(global_context.availability_intervals,
                          join(harness.options.plots_directory, 'status.png'))

if __name__ == '__main__':
    main(PlotUpdateStatisticsHarness)
