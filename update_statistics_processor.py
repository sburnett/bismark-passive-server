from collections import namedtuple

from sqlite_session_processor import SqliteProcessorCoordinator
from session_processor import SessionProcessor
import utils

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

class UpdateStatisticsSessionProcessor(SessionProcessor):
    def __init__(self):
        super(UpdateStatisticsSessionProcessor, self).__init__()

    def process_update(self, context, update):
        context.update_statistics.append(UpdateStatistics(
                        node_id=context.node_id,
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
        context.update_statistics_oldest_timestamps[context.node_id] \
                = min(context.update_statistics_oldest_timestamps[context.node_id],
                      update.timestamp)

def add_iter(first, second):
    second.append(iter(first))
    return second

class UpdateStatisticsProcessorCoordinator(SqliteProcessorCoordinator):
    persistent_state = dict(update_statistics=(list, add_iter))

    def __init__(self, options):
        super(UpdateStatisticsProcessorCoordinator, self).__init__(options)
        if options.db_rebuild:
            timestamp_init = utils.initialize_min_timestamp_dict
        else:
            timestamp_init = utils.initialize_max_timestamp_dict
        self.ephemeral_state = dict(
                update_statistics_oldest_timestamps=\
                        (timestamp_init, utils.min_dicts)
                )

    def create_processor(self, session):
        return UpdateStatisticsSessionProcessor()

    def write_to_database(self, database, global_context):
        database.import_update_statistics(
                global_context.update_statistics,
                global_context.update_statistics_oldest_timestamps)
