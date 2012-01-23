from collections import defaultdict
import datetime
from itertools import chain
from matplotlib.dates import AutoDateFormatter, AutoDateLocator, date2num
import matplotlib.pyplot as plt
from os.path import join

from harness import Harness, main
import harness
from update_statistics_processor import UpdateStatisticsSessionProcessor

OUTAGE_TIMEOUT = datetime.timedelta(seconds=60)

class PlotUpdateStatisticsHarness(Harness):
    processors = [UpdateStatisticsSessionProcessor]

    def __init__(self):
        super(PlotUpdateStatisticsHarness, self).__init__()

    def plot_updates(self, updates, filename, max_age=None):
        current_time = datetime.datetime.utcnow()
        if max_age is None:
            oldest = datetime.datetime.min
        else:
            oldest = current_time - max_age
        
        node_ids = set()
        for update in updates:
            node_ids.add(update.node_id)
        node_ids = list(node_ids)

        availabilities = defaultdict(list)
        outages = defaultdict(list)
        current_node_id = None
        for update in updates:
            if update.eventstamp <= oldest:
                continue
            if update.node_id != current_node_id:
                if current_node_id is not None:
                    availabilities[current_node_id].append((lower, upper))
                current_node_id = update.node_id
                lower = update.eventstamp
                upper = update.eventstamp
            if update.eventstamp - upper >= OUTAGE_TIMEOUT:
                print 'Outage:', update.node_id, update.eventstamp - upper
                availabilities[update.node_id].append((lower, upper))
                outages[update.node_id].append(upper)
                lower = update.eventstamp
            upper = update.eventstamp
        if current_node_id is not None:
            availabilities[update.node_id].append((lower, upper))

        indices = {}
        for node_id in node_ids:
            indices[node_id] = node_ids.index(node_id)

        availability_intervals = []
        for node_id, intervals in availabilities.items():
            for lower, upper in intervals:
                availability_intervals.append((indices[node_id], lower, upper))
        outage_points = []
        for node_id, outage_times in outages.items():
            for beginning in outage_times:
                outage_points.append((beginning,
                                      indices[node_id] - 0.2,
                                      indices[node_id] + 0.2))

        fig = plt.figure()
        ax = fig.add_subplot(111)
        plt.yticks(range(len(node_ids)), node_ids, family='monospace')
        if len(availability_intervals) > 0:
            ax.hlines(*zip(*availability_intervals), lw=2)
        if len(outage_points) > 0:
            ax.vlines(*zip(*outage_points), lw=2, color='r')
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
        updates = list(chain.from_iterable(global_context.update_statistics))
        updates.sort(key=lambda u: (u.node_id, u.eventstamp))
        print 'Plotting daily status'
        self.plot_updates(updates,
                          join(harness.options.plots_directory, 'status_daily.png'),
                          datetime.timedelta(days=1))
        print 'Plotting weekly status'
        self.plot_updates(updates,
                          join(harness.options.plots_directory, 'status_weekly.png'),
                          datetime.timedelta(days=7))
        print 'Plotting overall status'
        self.plot_updates(updates, join(harness.options.plots_directory, 'status.png'))

if __name__ == '__main__':
    main(PlotUpdateStatisticsHarness)
