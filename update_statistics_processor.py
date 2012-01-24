from collections import namedtuple

from session_processor import PersistentSessionProcessor

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
