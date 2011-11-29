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
        context.byte_count_oldest_timestamps[context.node_id] \
                = min(context.byte_count_oldest_timestamps[context.node_id],
                      rounded_timestamp)
        context_key = context.node_id, context.anonymization_id
        context.byte_count_oldest_timestamps[context_key] \
                = min(context.byte_count_oldest_timestamps[context_key],
                      rounded_timestamp)

        context.bytes_per_minute[context.node_id, rounded_timestamp] \
                += packet.size

        try:
            flow, flow_data = context.flows[packet.flow_id]
        except KeyError:
            flow = flow_data = None
        if flow is not None:
            if flow.source_ip in context.address_map \
                    and flow.destination_ip not in context.address_map:
                port = flow.destination_port
            elif flow.destination_ip in context.address_map \
                    and flow.source_ip not in context.address_map:
                port = flow.source_port
            else:
                port = -1
            context.bytes_per_port_per_minute[
                    context.node_id, rounded_timestamp, port] += packet.size

            device_names = []
            if flow.source_ip in context.mac_address_map:
                device_names.append(context.mac_address_map[flow.source_ip])
            if flow.destination_ip in context.mac_address_map:
                device_names.append(
                        context.mac_address_map[flow.destination_ip])
            if device_names == []:
                device_names = ['unknown']
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

            if 'domains' not in flow_data:
                flow_data['domains'] = set()
                if flow.source_ip in context.address_map \
                        and not flow.destination_ip_anonymized:
                    key = (context.address_map[flow.source_ip],
                           flow.destination_ip)
                elif flow.destination_ip in context.address_map \
                        and not flow.source_ip_anonymized:
                    key = (context.address_map[flow.destination_ip],
                           flow.source_ip)
                else:
                    key = None
                if key is not None and key in context.dns_ip_map:
                    for domain, start_time, end_time in context.dns_ip_map[key]:
                        if packet.timestamp >= start_time \
                                and packet.timestamp <= end_time:
                            flow_data['domains'].add(domain)
                else:
                    flow_data['domains'] = ['unknown']
            for domain in flow_data['domains']:
                context.bytes_per_domain_per_minute[context.node_id,
                                                    rounded_timestamp,
                                                    domain] += packet.size
                for device_name in device_names:
                    context.bytes_per_device_per_domain_per_minute[
                            context.node_id,
                            context.anonymization_id,
                            rounded_timestamp,
                            device_name,
                            domain] += packet.size
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
                    -1] += packet.size
            context.bytes_per_device_per_domain_per_minute[
                    context.node_id,
                    context.anonymization_id,
                    rounded_timestamp,
                    'unknown',
                    'unknown'] += packet.size

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
