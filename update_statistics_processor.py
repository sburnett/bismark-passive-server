from collections import defaultdict, namedtuple
from datetime import datetime
import multiprocessing

import database
from database_session_processor import DatabaseProcessorCoordinator
from session_processor import SessionProcessor
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

class UpdateStatisticsSessionProcessor(SessionProcessor):
    def __init__(self, fingerprint, timestamp_queue):
        super(UpdateStatisticsSessionProcessor, self).__init__()
        self._oldest_timestamp = datetime.max
        self._fingerprint = fingerprint
        self._timestamp_queue = timestamp_queue

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
        self._oldest_timestamp = min(self._oldest_timestamp, update.timestamp)

    def finished_session(self):
        self._timestamp_queue.put((self._fingerprint, self._oldest_timestamp))

class UpdateStatisticsProcessorCoordinator(DatabaseProcessorCoordinator):
    states = dict(update_statistics=(dict, utils.update_dict))

    def __init__(self, username, database, rebuild=False):
        super(UpdateStatisticsProcessorCoordinator, self).__init__(username, database)
        self._rebuild = rebuild
        if rebuild:
            self._oldest_timestamps = defaultdict(lambda: datetime.min)
        else:
            self._oldest_timestamps = defaultdict(lambda: datetime.max)
        manager = multiprocessing.Manager()
        self._timestamp_queue = manager.Queue()
        self._num_processors = 0

    def create_processor(self, session):
        self._num_processors += 1
        return UpdateStatisticsSessionProcessor(
                (session.node_id, session.anonymization_context),
                self._timestamp_queue)

    def finished_processing(self, global_context):
        while self._num_processors > 0:
            (node_id, anonymization_id), oldest_timestamp \
                    = self._timestamp_queue.get()
            self._num_processors -= 1
            self._oldest_timestamps[node_id] \
                    = min(self._oldest_timestamps[node_id], oldest_timestamp)
        super(UpdateStatisticsProcessorCoordinator, self)\
                .finished_processing(global_context)

    def write_to_database(self, database, global_context):
        database.import_update_statistics(global_context.update_statistics,
                                          self._oldest_timestamps)
