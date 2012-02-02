from collections import defaultdict, namedtuple
import datetime

from bismarkpassive.session_processor import PersistentSessionProcessor

UpdateStatistics = namedtuple('UpdateStatistics',
                              ['node_id',
                               'eventstamp',
                               'file_format_version',
                               'pcap_dropped',
                               'iface_dropped',
                               'packet_series_dropped',
                               'flow_table_dropped',
                               'dropped_a_records',
                               'dropped_cname_records',
                               'packet_series_size',
                               'flow_table_size',
                               'a_records_size',
                               'cname_records_size'])

OUTAGE_TIMEOUT = datetime.timedelta(seconds=35)

class UpdateStatisticsSessionProcessor(PersistentSessionProcessor):
    def initialize_context(self, context):
        context.update_statistics = []

    def process_update_persistent(self, persistent_context, update):
        persistent_context.update_statistics.append(UpdateStatistics(
                        node_id=persistent_context.node_id,
                        eventstamp=update.timestamp,
                        file_format_version=update.file_format_version,
                        pcap_dropped=update.pcap_dropped,
                        iface_dropped=update.iface_dropped,
                        packet_series_dropped=update.packet_series_dropped,
                        flow_table_dropped=update.flow_table_dropped,
                        dropped_a_records=update.dropped_a_records,
                        dropped_cname_records=update.dropped_cname_records,
                        packet_series_size=len(update.packet_series),
                        flow_table_size=update.flow_table_size,
                        a_records_size=len(update.a_records),
                        cname_records_size=len(update.cname_records)))

    def initialize_global_context(self, global_context):
        global_context.update_statistics = []

    def merge_contexts_persistent(self, persistent_context, global_context):
        global_context.update_statistics.append(
                iter(persistent_context.update_statistics))

class DataAvailabilityProcessor(PersistentSessionProcessor):
    def initialize_context(self, context):
        context.availability_lower_bound = None
        context.availability_upper_bound = None

    def process_update_persistent(self, context, update):
        if update.sequence_number == 0:
            try:
                context.availability_lower_bound = update.packet_series[0].timestamp
            except IndexError:
                context.availability_lower_bound = update.timestamp
        context.availability_upper_bound = update.timestamp

    def initialize_global_context(self, global_context):
        global_context.availability_intervals = defaultdict(list)

    def merge_contexts_persistent(self, context, global_context):
        new_lower, new_upper = (context.availability_lower_bound,
                                context.availability_upper_bound)
        if new_lower is None and new_upper is None:
            return
        obsolete_indices = []
        for idx, (lower, upper) in \
                enumerate(global_context.availability_intervals[context.node_id]):
            if new_upper <= lower - OUTAGE_TIMEOUT:
                continue
            if new_lower >= upper + OUTAGE_TIMEOUT:
                continue
            new_lower = min(new_lower, lower)
            new_upper = max(new_upper, upper)
            obsolete_indices.append(idx)
        for index in sorted(obsolete_indices, reverse=True):
            del global_context.availability_intervals[context.node_id][index]
        global_context.availability_intervals[context.node_id].append(
                (new_lower, new_upper))
