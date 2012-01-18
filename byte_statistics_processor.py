from flow_properties_processor import FlowPropertiesSessionProcessor
from sqlite_session_processor import SqliteProcessorCoordinator
import utils

class ByteStatisticsSessionProcessor(FlowPropertiesSessionProcessor):
    def __init__(self):
        super(ByteStatisticsSessionProcessor, self).__init__()
    
    def process_packet(self, context, packet, port, device_names, domains):
        rounded_timestamp = packet.timestamp.replace(second=0, microsecond=0)

        context_key = context.node_id, context.anonymization_id
        context.byte_statistics_oldest_timestamps[context_key] \
                = min(context.byte_statistics_oldest_timestamps[context_key],
                      rounded_timestamp)
        for domain in domains:
            for device_name in device_names:
                context.byte_statistics[context.node_id,
                                        context.anonymization_id,
                                        rounded_timestamp,
                                        device_name,
                                        port,
                                        domain] += packet.size

class ByteStatisticsProcessorCoordinator(SqliteProcessorCoordinator):
    persistent_state = dict(
            byte_statistics=(utils.initialize_int_dict, utils.sum_dicts),
            )

    def __init__(self, options):
        super(ByteStatisticsProcessorCoordinator, self).__init__(options)
        if options.db_rebuild:
            timestamp_init = utils.initialize_min_timestamp_dict
        else:
            timestamp_init = utils.initialize_max_timestamp_dict
        self.ephemeral_state = dict(
                byte_statistics_oldest_timestamps=\
                        (timestamp_init, utils.min_dicts))

    def create_processor(self, session):
        return ByteStatisticsSessionProcessor()
    
    def write_to_database(self, database, global_context):
        print 'Oldest timestamps:', global_context.byte_statistics_oldest_timestamps
        database.import_byte_statistics(
                global_context.byte_statistics,
                global_context.byte_statistics_oldest_timestamps)
