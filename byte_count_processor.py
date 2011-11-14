from collections import defaultdict
from datetime import datetime

from database_session_processor import DatabaseProcessorCoordinator
from session_processor import SessionProcessor
import utils

class ByteCountSessionProcessor(SessionProcessor):
    def __init__(self):
        super(ByteCountSessionProcessor, self).__init__()
    
    def process_update(self, context, update):
        for packet in update.packet_series:
            self.process_packet(context, packet)

    def process_packet(self, context, packet):
        rounded_timestamp = packet.timestamp.replace(second=0, microsecond=0)
        context.bytes_per_minute[context.node_id, rounded_timestamp] += packet.size

        flow = context.flows.get(packet.flow_id)
        if flow is not None:
            if flow.source_ip in context.address_map \
                    and flow.destination_ip not in context.address_map:
                port_key = (context.node_id, rounded_timestamp, flow.destination_port)
            elif flow.destination_ip in context.address_map \
                    and flow.source_ip not in context.address_map:
                port_key = (context.node_id, rounded_timestamp, flow.source_port)
            else:
                port_key = (context.node_id, rounded_timestamp, -1)
            context.bytes_per_port_per_minute[port_key] += packet.size

            device_keys = []
            if flow.source_ip in context.mac_address_map:
                device_keys.append((context.node_id,
                                    context.anonymization_id,
                                    rounded_timestamp,
                                    context.mac_address_map[flow.source_ip]))
            if flow.destination_ip in context.mac_address_map:
                device_keys.append(
                        (context.node_id,
                         context.anonymization_id,
                         rounded_timestamp,
                         context.mac_address_map[flow.destination_ip]))
            if device_keys == []:
                device_keys = [(context.node_id,
                                context.anonymization_id,
                                rounded_timestamp,
                                'unknown')]
            for device_key in device_keys:
                context.bytes_per_device_per_minute[device_key] += packet.size

            for device_key in device_keys:
                device_port_key = (context.node_id,
                                   context.anonymization_id,
                                   rounded_timestamp,
                                   device_key[3],
                                   port_key[2])
                context.bytes_per_device_per_port_per_minute[device_port_key] \
                        += packet.size

            key = None
            if flow.source_ip in context.address_map \
                    and not flow.destination_ip_anonymized:
                key = (context.address_map[flow.source_ip], flow.destination_ip)
            elif flow.destination_ip in context.address_map \
                    and not flow.source_ip_anonymized:
                key = (context.address_map[flow.destination_ip], flow.source_ip)
            if key is not None and key in context.flow_ip_map:
                for domain, start_time, end_time in context.flow_ip_map[key]:
                    if packet.timestamp < start_time \
                            or packet.timestamp > end_time:
                        continue
                    domain_key = (context.node_id, rounded_timestamp, domain)
                    context.bytes_per_domain_per_minute[domain_key] \
                            += packet.size
                    device_domain_key = (context.node_id,
                                         context.anonymization_id,
                                         rounded_timestamp,
                                         device_key[3],
                                         domain)
                    context.bytes_per_device_per_domain_per_minute[
                            device_domain_key] += packet.size
            else:
                domain_key = (context.node_id, rounded_timestamp, 'unknown')
                context.bytes_per_domain_per_minute[domain_key] += packet.size
                device_domain_key \
                        = (context.node_id,
                                context.anonymization_id,
                                rounded_timestamp,
                                device_key[3],
                                'unknown')
                context.bytes_per_device_per_domain_per_minute[
                        device_domain_key] += packet.size
        else:
            context.bytes_per_port_per_minute[
                    context.node_id, rounded_timestamp, -1] += packet.size
            context.bytes_per_domain_per_minute[context.node_id,
                                                rounded_timestamp,
                                                'unknown'] += packet.size
            context.bytes_per_device_per_minute[context.node_id,
                                                context.anonymization_id,
                                                rounded_timestamp,
                                                'unknown'] += packet.size
            context.bytes_per_device_per_port_per_minute[
                    context.node_id,
                    context.anonymization_id,
                    rounded_timestamp,
                    'unknown',
                    -1] != packet.size
            context.bytes_per_device_per_domain_per_minute[
                    context.node_id,
                    context.anonymization_id,
                    rounded_timestamp,
                    'unknown',
                    'unknown'] != packet.size

        context.byte_count_oldest_timestamps[context.node_id] \
                = min(context.byte_count_oldest_timestamps[context.node_id],
                      rounded_timestamp)
        context_key = context.node_id, context.anonymization_id
        context.byte_count_oldest_timestamps[context_key] \
                = min(context.byte_count_oldest_timestamps[context_key],
                      rounded_timestamp)

class ByteCountProcessorCoordinator(DatabaseProcessorCoordinator):
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

    def __init__(self, username, database, rebuild=False):
        super(ByteCountProcessorCoordinator, self).__init__(username, database)
        if rebuild:
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
