"""
This module is a simple example of how to process data using Bismark Passive. It
sums the sizes of all the packets sent through each Bismark router and presents
the results to the user in a table on the console.

To run it:

    python example.py sqlite /data/users/sburnett/index.sqlite /data/users/$USER/passive-pickles

Running it the first time will take a few minutes. After the first run
is complete, run it again to verify that the results were cached.

"""

from collections import defaultdict

import bismarkpassive

class SimpleByteCountProcessor(bismarkpassive.PersistentSessionProcessor):
    def initialize_context(self, context):
        context.number_of_bytes_this_session = 0

    def process_update_persistent(self, context, update):
        for packet in update.packet_series:
            context.number_of_bytes_this_session += packet.size

    def initialize_global_context(self, global_context):
        global_context.number_of_bytes_per_node = defaultdict(int)

    def merge_contexts_persistent(self, context, global_context):
        global_context.number_of_bytes_per_node[context.node_id] += context.number_of_bytes_this_session

class PrintByteCountsHarness(bismarkpassive.Harness):
    @property
    def processors(self):
        return [SimpleByteCountProcessor]

    def process_results(self, global_context):
        print 'Bytes transferred through each router:'
        for node_id, count in global_context.number_of_bytes_per_node.items():
            print node_id, count

if __name__ == '__main__':
    bismarkpassive.main(PrintByteCountsHarness)
