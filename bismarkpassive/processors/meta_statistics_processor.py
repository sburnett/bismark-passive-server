from collections import defaultdict
from datetime import datetime

from bismarkpassive.session_processor import EphemeralSessionProcessor

class MetaStatisticsProcessor(EphemeralSessionProcessor):
    def initialize_ephemeral_context(self, context):
        context.oldest_timestamp = datetime.max
        context.newest_timestamp = datetime.min

    def process_update_ephemeral(self, ephemeral_context, update):
        ephemeral_context.oldest_timestamp = \
                min(ephemeral_context.oldest_timestamp, update.timestamp)
        ephemeral_context.newest_timestamp = \
                min(ephemeral_context.newest_timestamp, update.timestamp)

    def initialize_global_context(self, global_context):
        global_context.oldest_timestamp = datetime.max
        global_context.oldest_timestamp_per_node = \
                defaultdict(lambda: datetime.max)
        global_context.oldest_timestamp_per_anyonymization_context = \
                defaultdict(lambda: datetime.max)
        global_context.newest_timestamp = datetime.min
        global_context.newest_timestamp_per_node = \
                defaultdict(lambda: datetime.min)
        global_context.newest_timestamp_per_anyonymization_context = \
                defaultdict(lambda: datetime.min)

    def merge_contexts_ephemeral(self, ephemeral_context, global_context):
        global_context.oldest_timestamp = \
                min(global_context.oldest_timestamp,
                    ephemeral_context.oldest_timestamp)
        global_context.newest_timestamp = \
                max(global_context.newest_timestamp,
                    ephemeral_context.newest_timestamp)
        with ephemeral_context.node_id as key:
            global_context.oldest_timestamp_per_node[key] = \
                    min(global_context.oldest_timestamp_per_node[key],
                        ephemeral_context.oldest_timestamp)
            global_context.newest_timestamp_per_node[key] = \
                    max(global_context.newest_timestamp_per_node[key],
                        ephemeral_context.newest_timestamp)
        with (ephemeral_context.node_id,
              ephemeral_context.anonymization_context) as key:
            global_context.oldest_timestamp_per_anyonymization_context[key] = \
                    min(global_context.oldest_timestamp_per_node[key],
                        ephemeral_context.oldest_timestamp)
            global_context.newest_timestamp_per_anyonymization_context[key] = \
                    max(global_context.newest_timestamp_per_node[key],
                        ephemeral_context.newest_timestamp)
