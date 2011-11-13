from collections import defaultdict, namedtuple

import database
from database_session_processor import DatabaseSessionProcessor
import utils

UpdateStatistics = namedtuple('UpdateStatistics',
                              ['pcap_dropped',
                               'iface_dropped',
                               'packet_series_dropped',
                               'flow_table_dropped',
                               'dropped_a_records',
                               'dropped_cname_records',
                               'packet_series_size',
                               'flow_table_size',
                               'a_records_size',
                               'cname_records_size'])

class UpdateStatisticsSessionProcessor(DatabaseSessionProcessor):
    states = dict(update_statistics=(dict, utils.update_dict))

    def __init__(self, username, database, rebuild=False):
        super(UpdateStatisticsSessionProcessor, self).__init__(username, database)
        if rebuild:
            self._oldest_timestamps = defaultdict(utils.initialize_min_timestamp)
        else:
            self._oldest_timestamps = defaultdict(utils.initialize_max_timestamp)

    def process_update(self, context, update):
        context.update_statistics[context.node_id, update.timestamp] \
                = UpdateStatistics(
                        pcap_dropped=update.pcap_dropped,
                        iface_dropped=update.iface_dropped,
                        packet_series_dropped=update.packet_series_dropped,
                        flow_table_dropped=update.flow_table_dropped,
                        dropped_a_records=update.dropped_a_records,
                        dropped_cname_records=update.dropped_cname_records,
                        packet_series_size=len(update.packet_series),
                        flow_table_size=update.flow_table_size,
                        a_records_size=len(update.a_records),
                        cname_records_size=len(update.cname_records))
        self._oldest_timestamps[context.node_id] \
                = min(self._oldest_timestamps[context.node_id],
                      update.timestamp)

    def write_to_database(self, database, global_context):
        database.import_update_statistics(global_context.update_statistics,
                                          self._oldest_timestamps)
