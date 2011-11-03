from collections import defaultdict, namedtuple
from datetime import datetime

import db
from session_computations import \
        SessionProcessor, SessionAggregator, merge_timeseries

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

class UpdateStatisticsSessionProcessor(SessionProcessor):
    def __init__(self):
        super(UpdateStatisticsSessionProcessor, self).__init__()
        self._update_statistics = {}

    def process_update(self, update):
        self._update_statistics[update.timestamp] = UpdateStatistics(
                pcap_dropped = update.pcap_dropped,
                iface_dropped = update.iface_dropped,
                packet_series_dropped = update.packet_series_dropped,
                flow_table_dropped = update.flow_table_dropped,
                dropped_a_records = update.dropped_a_records,
                dropped_cname_records = update.dropped_cname_records,
                packet_series_size = len(update.packet_series),
                flow_table_size = update.flow_table_size,
                a_records_size = len(update.a_records),
                cname_records_size = len(update.cname_records))
        return update.timestamp

    @property
    def results(self):
        return { 'update_statistics': self._update_statistics }

    def augment_session_result(self, session_result, update_result):
        if session_result is None:
            return update_result
        else:
            return min(update_result, session_result)

class UpdateStatisticsSessionAggregator(SessionAggregator):
    def __init__(self, username, database, rebuild):
        super(UpdateStatisticsSessionAggregator, self).__init__()

        self._username = username
        self._database = database
        self._rebuild = rebuild

        self._update_statistics = defaultdict(lambda: defaultdict(int))
        self._nodes_updated = set()
        self._oldest_timestamps = defaultdict(lambda: datetime.max)

    def augment_results(self,
                        node_id,
                        anonymization_id,
                        session_id,
                        results,
                        updated,
                        process_result):
        self._update_statistics[node_id].update(
                results['update_statistics'])
        if updated:
            self._nodes_updated.add(node_id)
            if process_result is not None:
                self._oldest_timestamps[node_id] \
                        = min(self._oldest_timestamps[node_id], process_result)

    def store_results(self):
        database = db.BismarkPassiveDatabase(self._username, self._database)
        print 'Writing update statistics'
        for node_id, statistics in self._update_statistics.items():
            print ' ', node_id
            if node_id in self._nodes_updated or self._rebuild:
                if self._rebuild:
                    oldest_timestamp = datetime.min
                else:
                    oldest_timestamp = self._oldest_timestamps.get(node_id)
                database.import_update_statistics(node_id,
                                                  oldest_timestamp,
                                                  statistics)
