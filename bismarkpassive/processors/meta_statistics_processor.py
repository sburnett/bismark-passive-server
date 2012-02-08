from collections import defaultdict
from datetime import datetime

from bismarkpassive import SessionProcessor

def max_date():
    return datetime.max
def min_date():
    return datetime.min

class MetaStatisticsProcessor(SessionProcessor):
    def initialize_ephemeral_context(self, context):
        context.oldest_timestamp = datetime.max
        context.newest_timestamp = datetime.min

    def process_update(self, persistent_context, ephemeral_context, update):
        ephemeral_context.oldest_timestamp = \
                min(ephemeral_context.oldest_timestamp, update.timestamp)
        ephemeral_context.newest_timestamp = \
                min(ephemeral_context.newest_timestamp, update.timestamp)

    def initialize_global_context(self, global_context):
        global_context.oldest_timestamp = datetime.max
        global_context.oldest_timestamp_per_node = \
                defaultdict(max_date)
        global_context.oldest_timestamp_per_anonymization_context = \
                defaultdict(max_date)
        global_context.newest_timestamp = datetime.min
        global_context.newest_timestamp_per_node = \
                defaultdict(min_date)
        global_context.newest_timestamp_per_anyonymization_context = \
                defaultdict(min_date)

    def merge_contexts(self, persistent_context, ephemeral_context, global_context):
        global_context.oldest_timestamp = \
                min(global_context.oldest_timestamp,
                    ephemeral_context.oldest_timestamp)
        global_context.newest_timestamp = \
                max(global_context.newest_timestamp,
                    ephemeral_context.newest_timestamp)
        key = ephemeral_context.node_id 
        global_context.oldest_timestamp_per_node[key] = \
                min(global_context.oldest_timestamp_per_node[key],
                    ephemeral_context.oldest_timestamp)
        global_context.newest_timestamp_per_node[key] = \
                max(global_context.newest_timestamp_per_node[key],
                    ephemeral_context.newest_timestamp)
        key = (ephemeral_context.node_id,
               ephemeral_context.anonymization_context)
        global_context.oldest_timestamp_per_anonymization_context[key] = \
                min(global_context.oldest_timestamp_per_anonymization_context[key],
                    ephemeral_context.oldest_timestamp)
        global_context.newest_timestamp_per_anyonymization_context[key] = \
                max(global_context.newest_timestamp_per_anyonymization_context[key],
                    ephemeral_context.newest_timestamp)
