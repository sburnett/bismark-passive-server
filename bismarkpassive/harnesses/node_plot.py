from datetime import datetime
from errno import EEXIST
import matplotlib.pyplot as plt
from os import makedirs
from os.path import join

import bismarkpassive.harness as harness

class NodePlotHarness(harness.Harness):
    def plot(self, plotter, context, filename, node_ids,
                figsize=(15,20),
                limits=(None, None),
                title=True,
                availability=True,
                timestamp=True,
                autoformat=True,
                tight_layout=True):
        node_ids.sort()
        fig = plt.figure(1, figsize=figsize)
        fig.clear()
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
            if title:
                plt.title(node_id)
            if availability:
                for lower, upper in context.availability_intervals[node_id]:
                    ax.axvspan(lower, upper, facecolor='y', alpha=0.3, linewidth=0.0)
            plotter(context, node_id, ax, limits)
        if autoformat:
            fig.autofmt_xdate(bottom=0)
        if timestamp:
            plt.text(1, 0, 'Generated %s' % datetime.now(),
                     fontsize=8,
                     color='gray',
                     transform=fig.transFigure,
                     verticalalignment='bottom',
                     horizontalalignment='right')
        if tight_layout:
            plt.tight_layout()
        plt.savefig(join(harness.options.plots_directory, filename))

class PlotPerNodeHarness(harness.Harness):
    def plot(self, plotter, context, directory, node_ids,
                extension='png',
                figsize=(11, 8.5),
                limits=(None, None),
                title=True,
                availability=True,
                timestamp=True,
                autoformat=True,
                tight_layout=True):
        try:
            makedirs(join(harness.options.plots_directory, directory))
        except OSError, e:
            if e.errno != EEXIST:
                raise
        for node_id in node_ids:
            fig = plt.figure(1, figsize=figsize)
            fig.clear()
            left, right = limits
            if left is not None:
                plt.xlim(xmin=left)
            if right is not None:
                plt.xlim(xmax=right)
            if title:
                plt.title(node_id)
            if availability:
                for lower, upper in context.availability_intervals[node_id]:
                    plt.axvspan(lower, upper, facecolor='0.9', linewidth=0.0)
            plotter(context, node_id, limits)
            if autoformat:
                fig.autofmt_xdate(bottom=0)
            if timestamp:
                plt.text(1, 0, 'Generated %s' % datetime.now(),
                         fontsize=8,
                         color='gray',
                         transform=fig.transFigure,
                         verticalalignment='bottom',
                         horizontalalignment='right')
            if tight_layout:
                plt.tight_layout()
            plt.savefig(join(harness.options.plots_directory,
                             directory,
                             '%s.%s' % (node_id, extension)))
