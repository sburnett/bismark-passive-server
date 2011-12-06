from postgres_session_processor import PostgresProcessorCoordinator
from flow_properties_processor import FlowPropertiesSessionProcessor
import utils

class ByteCountSessionProcessor(FlowPropertiesSessionProcessor):
    def __init__(self):
        super(ByteCountSessionProcessor, self).__init__()
    
    def process_packet(self, context, packet, port, device_names, domains):
        rounded_timestamp = packet.timestamp.replace(second=0, microsecond=0)

        context.byte_count_oldest_timestamps[context.node_id] \
                = min(context.byte_count_oldest_timestamps[context.node_id],
                      rounded_timestamp)
        context_key = context.node_id, context.anonymization_id
        context.byte_count_oldest_timestamps[context_key] \
                = min(context.byte_count_oldest_timestamps[context_key],
                      rounded_timestamp)

        context.bytes_per_minute[context.node_id, rounded_timestamp] \
                += packet.size
        context.bytes_per_port_per_minute[
                context.node_id, rounded_timestamp, port] += packet.size
        for domain in domains:
            context.bytes_per_domain_per_minute[context.node_id,
                                                rounded_timestamp,
                                                domain] += packet.size
        for device_name in device_names:
            context.bytes_per_device_per_minute[context.node_id,
                                                context.anonymization_id,
                                                rounded_timestamp,
                                                device_name] += packet.size
            context.bytes_per_device_per_port_per_minute[
                    context.node_id,
                    context.anonymization_id,
                    rounded_timestamp,
                    device_name,
                    port] += packet.size
            for domain in domains:
                context.bytes_per_device_per_domain_per_minute[
                        context.node_id,
                        context.anonymization_id,
                        rounded_timestamp,
                        device_name,
                        domain] += packet.size

class ByteCountProcessorCoordinator(PostgresProcessorCoordinator):
    persistent_state = dict(
            bytes_per_minute=\
                    (utils.initialize_int_dict, utils.sum_dicts),
            bytes_per_port_per_minute=\
                    (utils.initialize_int_dict, utils.sum_dicts),
            bytes_per_domain_per_minute=\
                    (utils.initialize_int_dict, utils.sum_dicts),
            bytes_per_device_per_minute=\
                    (utils.initialize_int_dict, utils.sum_dicts),
            bytes_per_device_per_port_per_minute=\
                    (utils.initialize_int_dict, utils.sum_dicts),
            bytes_per_device_per_domain_per_minute=\
                    (utils.initialize_int_dict, utils.sum_dicts),
            )

    def __init__(self, options):
        super(ByteCountProcessorCoordinator, self).__init__(options)
        if options.db_rebuild:
            timestamp_init = utils.initialize_min_timestamp_dict
        else:
            timestamp_init = utils.initialize_max_timestamp_dict
        self.ephemeral_state = dict(
                byte_count_oldest_timestamps=(timestamp_init, utils.min_dicts))

    def create_processor(self, session):
        return ByteCountSessionProcessor()
    
    def write_to_database(self, database, global_context):
        print 'Oldest timestamps:', global_context.byte_count_oldest_timestamps
        database.import_bytes_per_minute(
                global_context.bytes_per_minute,
                global_context.byte_count_oldest_timestamps)
        database.import_bytes_per_port_per_minute(
                global_context.bytes_per_port_per_minute,
                global_context.byte_count_oldest_timestamps)
        database.import_bytes_per_domain_per_minute(
                global_context.bytes_per_domain_per_minute,
                global_context.byte_count_oldest_timestamps)
        database.import_bytes_per_device_per_minute(
                global_context.bytes_per_device_per_minute,
                global_context.byte_count_oldest_timestamps)
        database.import_bytes_per_device_per_port_per_minute(
                global_context.bytes_per_device_per_port_per_minute,
                global_context.byte_count_oldest_timestamps)
        database.import_bytes_per_device_per_domain_per_minute(
                global_context.bytes_per_device_per_domain_per_minute,
                global_context.byte_count_oldest_timestamps)
        database.refresh_matviews(global_context.byte_count_oldest_timestamps)
