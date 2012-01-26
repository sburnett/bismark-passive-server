from collections import defaultdict
#from flow_properties_processor import FlowPropertiesSessionProcessor
#from postgres_session_processor import PostgresProcessorCoordinator
from session_processor import PersistentSessionProcessor
import utils

class BytesPerMinuteSessionProcessor(PersistentSessionProcessor):
    def initialize_context(self, context):
        context.bytes_per_minute = defaultdict(int)

    def process_update_persistent(self, context, update):
        for packet in update.packet_series:
            context.bytes_per_minute[packet.timestamp.replace(microsecond=0, second=0)] += packet.size

    def initialize_global_context(self, context):
        context.bytes_per_minute = defaultdict(lambda: defaultdict(int))

    def merge_contexts_persistent(self, context, global_context):
        for timestamp, count in context.bytes_per_minute.iteritems():
            global_context.bytes_per_minute[context.node_id][timestamp] += count

class BytesPerHourSessionProcessor(PersistentSessionProcessor):
    def initialize_context(self, context):
        context.bytes_per_hour = defaultdict(int)

    def process_update_persistent(self, context, update):
        for packet in update.packet_series:
            context.bytes_per_hour[packet.timestamp.replace(microsecond=0, second=0, minute=0)] += packet.size

    def initialize_global_context(self, context):
        context.bytes_per_hour = defaultdict(lambda: defaultdict(int))

    def merge_contexts_persistent(self, context, global_context):
        for timestamp, count in context.bytes_per_hour.iteritems():
            global_context.bytes_per_hour[context.node_id][timestamp] += count

class BytesPerDaySessionProcessor(PersistentSessionProcessor):
    def initialize_context(self, context):
        context.bytes_per_day = defaultdict(int)

    def process_update_persistent(self, context, update):
        for packet in update.packet_series:
            context.bytes_per_day[packet.timestamp.replace(microsecond=0, second=0, minute=0, hour=0)] += packet.size

    def initialize_global_context(self, context):
        context.bytes_per_day = defaultdict(lambda: defaultdict(int))

    def merge_contexts_persistent(self, context, global_context):
        for timestamp, count in context.bytes_per_day.iteritems():
            global_context.bytes_per_day[context.node_id][timestamp] += count

def create_dict_int():
    return defaultdict(int)

class BytesPerPortPerHourSessionProcessor(PersistentSessionProcessor):
    def __init__(self, ports=None):
        if ports is not None:
            self._ports = set(ports)
        else:
            self._ports = set([80])

    def initialize_context(self, context):
        context.bytes_per_port_per_hour = defaultdict(create_dict_int)

    def process_update_persistent(self, context, update):
        for packet in update.packet_series:
            try:
                flow = context.flows[packet.flow_id]
            except KeyError:
                continue
            if flow.source_port in self._ports:
                port = flow.source_port
            elif flow.destination_port in self._ports:
                port = flow.destination_port
            else:
                continue
            timestamp = packet.timestamp.replace(microsecond=0, second=0, minute=0)
            context.bytes_per_port_per_hour[port][timestamp] += packet.size

    def initialize_global_context(self, context):
        context.bytes_per_port_per_hour = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    def merge_contexts_persistent(self, context, global_context):
        for port, bytes_per_hour in context.bytes_per_port_per_hour.iteritems():
            result_dict = global_context.bytes_per_port_per_hour[context.node_id][port]
            for timestamp, count in bytes_per_hour.iteritems():
                result_dict[timestamp] += count


#class ByteCountSessionProcessor(FlowPropertiesSessionProcessor):
#    def process_packet(self, context, packet, transport_protocol, port, device_names, domains):
#        rounded_timestamp = packet.timestamp.replace(second=0, microsecond=0)
#
#        context.byte_count_oldest_timestamps[context.node_id] \
#                = min(context.byte_count_oldest_timestamps[context.node_id],
#                      rounded_timestamp)
#        context_key = context.node_id, context.anonymization_id
#        context.byte_count_oldest_timestamps[context_key] \
#                = min(context.byte_count_oldest_timestamps[context_key],
#                      rounded_timestamp)
#
#        context.bytes_per_minute[context.node_id, rounded_timestamp] \
#                += packet.size
#        context.bytes_per_port_per_minute[
#                context.node_id, rounded_timestamp, port] += packet.size
#        for domain in domains:
#            context.bytes_per_domain_per_minute[context.node_id,
#                                                rounded_timestamp,
#                                                domain] += packet.size
#        for device_name in device_names:
#            context.bytes_per_device_per_minute[context.node_id,
#                                                context.anonymization_id,
#                                                rounded_timestamp,
#                                                device_name] += packet.size
#            context.bytes_per_device_per_port_per_minute[
#                    context.node_id,
#                    context.anonymization_id,
#                    rounded_timestamp,
#                    device_name,
#                    port] += packet.size
#            for domain in domains:
#                context.bytes_per_device_per_domain_per_minute[
#                        context.node_id,
#                        context.anonymization_id,
#                        rounded_timestamp,
#                        device_name,
#                        domain] += packet.size
#
#class ByteCountProcessorCoordinator(PostgresProcessorCoordinator):
#    persistent_state = dict(
#            bytes_per_minute=\
#                    (utils.initialize_int_dict, utils.sum_dicts),
#            bytes_per_port_per_minute=\
#                    (utils.initialize_int_dict, utils.sum_dicts),
#            bytes_per_domain_per_minute=\
#                    (utils.initialize_int_dict, utils.sum_dicts),
#            bytes_per_device_per_minute=\
#                    (utils.initialize_int_dict, utils.sum_dicts),
#            bytes_per_device_per_port_per_minute=\
#                    (utils.initialize_int_dict, utils.sum_dicts),
#            bytes_per_device_per_domain_per_minute=\
#                    (utils.initialize_int_dict, utils.sum_dicts),
#            )
#
#    def __init__(self, options):
#        super(ByteCountProcessorCoordinator, self).__init__(options)
#        if options.db_rebuild:
#            timestamp_init = utils.initialize_min_timestamp_dict
#        else:
#            timestamp_init = utils.initialize_max_timestamp_dict
#        self.ephemeral_state = dict(
#                byte_count_oldest_timestamps=(timestamp_init, utils.min_dicts))
#
#    def create_processor(self, session):
#        return ByteCountSessionProcessor()
#    
#    def write_to_database(self, database, global_context):
#        print 'Oldest timestamps:', global_context.byte_count_oldest_timestamps
#        database.import_bytes_per_minute(
#                global_context.bytes_per_minute,
#                global_context.byte_count_oldest_timestamps)
#        database.import_bytes_per_port_per_minute(
#                global_context.bytes_per_port_per_minute,
#                global_context.byte_count_oldest_timestamps)
#        database.import_bytes_per_domain_per_minute(
#                global_context.bytes_per_domain_per_minute,
#                global_context.byte_count_oldest_timestamps)
#        database.import_bytes_per_device_per_minute(
#                global_context.bytes_per_device_per_minute,
#                global_context.byte_count_oldest_timestamps)
#        database.import_bytes_per_device_per_port_per_minute(
#                global_context.bytes_per_device_per_port_per_minute,
#                global_context.byte_count_oldest_timestamps)
#        database.import_bytes_per_device_per_domain_per_minute(
#                global_context.bytes_per_device_per_domain_per_minute,
#                global_context.byte_count_oldest_timestamps)
#        database.refresh_matviews(global_context.byte_count_oldest_timestamps)
