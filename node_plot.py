from datetime import datetime
import matplotlib.pyplot as plt
from os.path import join

import harness

class NodePlotHarness(harness.Harness):
    def plot(self, plotter, context, filename, node_ids, figsize=(15,20), limits=(None, None)):
        node_ids.sort()
        fig = plt.figure(1, figsize=figsize)
        fig.clear()
        plt.text(1, 0, 'Generated %s' % datetime.now(),
                 fontsize=8,
                 color='gray',
                 transform=fig.transFigure,
                 verticalalignment='bottom',
                 horizontalalignment='right')
        for row, node_id in enumerate(node_ids):
            try:
                ax = plt.subplot(len(node_ids), 1, row, sharex=first_axis)
            except NameError:
                first_axis = ax = plt.subplot(len(node_ids), 1, row)
            left, right = limits
            if left is not None:
                ax.set_xlim(left=left)
            if right is not None:
                ax.set_xlim(right=right)
            plt.title(node_id)
            for lower, upper in context.availability_intervals[node_id]:
                ax.axvspan(lower, upper, facecolor='y', alpha=0.3, linewidth=0.0)
            plotter(context, node_id, ax, limits)
        fig.autofmt_xdate(bottom=0)
        plt.tight_layout()
        plt.savefig(join(harness.options.plots_directory, filename))
