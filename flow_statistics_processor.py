from database_session_processor import DatabaseProcessorCoordinator
from flow_properties_processor import FlowPropertiesSessionProcessor
from session_processor import SessionProcessor
import utils

class FlowStatisticsSessionProcessor(FlowPropertiesSessionProcessor):
    def __init__(self):
        super(FlowStatisticsSessionProcessor, self).__init__()
    
    def process_packet(self, context, packet, port, device_names, domains):
        try:
            flow, flow_data = context.flows[packet.flow_id]
        except KeyError:
            return
        if 'start_time' not in flow_data \
                or packet.timestamp < flow_data['start_time']:
            flow_data['start_time'] = packet.timestamp
        flow_key = (context.node_id,
                    context.anonymization_id,
                    context.session_id,
                    packet.flow_id)
        if 'flow_started' not in flow_data:
            flow_data['flow_started'] = True
            if flow_key in context.auxiliary_per_flow:
                port, domains, device_names \
                        = context.auxiliary_per_flow[flow_key]
                byte_count = context.bytes_per_flow_accumulator[flow_key]
                packet_count = context.packets_per_flow_accumulator[flow_key]
                second_count = context.seconds_per_flow_accumulator[flow_key]

                context.bytes_per_flow[context.node_id, byte_count] += 1
                context.packets_per_flow[context.node_id, packet_count] += 1
                context.seconds_per_flow[context.node_id, second_count] += 1

                context.bytes_per_port_per_flow[
                        context.node_id, port, byte_count] += 1
                context.packets_per_port_per_flow[
                        context.node_id, port, packet_count] += 1
                context.seconds_per_port_per_flow[
                        context.node_id, port, second_count] += 1

                for domain in domains:
                    context.bytes_per_domain_per_flow[
                            context.node_id, domain, byte_count] += 1
                    context.packets_per_domain_per_flow[
                            context.node_id, domain, packet_count] += 1
                    context.seconds_per_domain_per_flow[
                            context.node_id, domain, second_count] += 1
                for device_name in device_names:
                    context.bytes_per_device_per_flow[
                            context.node_id, device_name, byte_count] += 1
                    context.packets_per_device_per_flow[
                            context.node_id, device_name, packet_count] += 1
                    context.seconds_per_device_per_flow[
                            context.node_id, device_name, second_count] += 1

                del context.auxiliary_per_flow[flow_key]
                del context.bytes_per_flow_accumulator[flow_key]
                del context.packets_per_flow_accumulator[flow_key]
                del context.seconds_per_flow_accumulator[flow_key]

        context.auxiliary_per_flow[flow_key] = (port, domains, device_names)
        context.bytes_per_flow_accumulator[flow_key] += packet.size
        context.packets_per_flow_accumulator[flow_key] += 1
        time_elapsed = packet.timestamp - flow_data['start_time']
        context.seconds_per_flow_accumulator[flow_key] \
                = time_elapsed.seconds + time_elapsed.days * 86400

class FlowStatisticsProcessorCoordinator(DatabaseProcessorCoordinator):
    persistent_state = dict(
            bytes_per_flow=\
                    (utils.initialize_int_dict, utils.sum_dicts),
            packets_per_flow=\
                    (utils.initialize_int_dict, utils.sum_dicts),
            seconds_per_flow=\
                    (utils.initialize_int_dict, utils.sum_dicts),
            bytes_per_port_per_flow=\
                    (utils.initialize_int_dict, utils.sum_dicts),
            packets_per_port_per_flow=\
                    (utils.initialize_int_dict, utils.sum_dicts),
            seconds_per_port_per_flow=\
                    (utils.initialize_int_dict, utils.sum_dicts),
            bytes_per_domain_per_flow=\
                    (utils.initialize_int_dict, utils.sum_dicts),
            packets_per_domain_per_flow=\
                    (utils.initialize_int_dict, utils.sum_dicts),
            seconds_per_domain_per_flow=\
                    (utils.initialize_int_dict, utils.sum_dicts),
            bytes_per_device_per_flow=\
                    (utils.initialize_int_dict, utils.sum_dicts),
            packets_per_device_per_flow=\
                    (utils.initialize_int_dict, utils.sum_dicts),
            seconds_per_device_per_flow=\
                    (utils.initialize_int_dict, utils.sum_dicts),
            )
    ephemeral_state = dict(
            auxiliary_per_flow=\
                    (dict, utils.merge_disjoint_dicts),
            bytes_per_flow_accumulator=\
                    (utils.initialize_int_dict, utils.merge_disjoint_dicts),
            packets_per_flow_accumulator=\
                    (utils.initialize_int_dict, utils.merge_disjoint_dicts),
            seconds_per_flow_accumulator=\
                    (utils.initialize_int_dict, utils.merge_disjoint_dicts),
            )

    def __init__(self, options):
        super(FlowStatisticsProcessorCoordinator, self).__init__(options)

    def create_processor(self, session):
        return FlowStatisticsSessionProcessor()
    
    def finished_processing(self, global_context):
        for flow_key, (port, domains, device_names) \
                in global_context.auxiliary_per_flow.iteritems():
            node_id = flow_key[0]

            byte_count \
                    = global_context.bytes_per_flow_accumulator[flow_key]
            packet_count \
                    = global_context.packets_per_flow_accumulator[flow_key]
            second_count \
                    = global_context.seconds_per_flow_accumulator[flow_key]

            global_context.bytes_per_flow[node_id, byte_count] += 1
            global_context.packets_per_flow[node_id, packet_count] += 1
            global_context.seconds_per_flow[node_id, second_count] += 1

            global_context.bytes_per_port_per_flow[
                    node_id, port, byte_count] += 1
            global_context.packets_per_port_per_flow[
                    node_id, port, packet_count] += 1
            global_context.seconds_per_port_per_flow[
                    node_id, port, second_count] += 1

            for domain in domains:
                global_context.bytes_per_domain_per_flow[
                        node_id, domain, byte_count] += 1
                global_context.packets_per_domain_per_flow[
                            node_id, domain, packet_count] += 1
                global_context.seconds_per_domain_per_flow[
                            node_id, domain, second_count] += 1

            for device_name in device_names:
                global_context.bytes_per_device_per_flow[
                        node_id, device_name, byte_count] += 1
                global_context.packets_per_device_per_flow[
                        node_id, device_name, packet_count] += 1
                global_context.seconds_per_device_per_flow[
                        node_id, device_name, second_count] += 1

        super(FlowStatisticsProcessorCoordinator, self).finished_processing(
                global_context)

    def write_to_database(self, database, global_context):
        database.import_bytes_per_flow(global_context.bytes_per_flow)
        database.import_packets_per_flow(global_context.packets_per_flow)
        database.import_seconds_per_flow(global_context.seconds_per_flow)
        database.import_bytes_per_port_per_flow(
                global_context.bytes_per_port_per_flow)
        database.import_packets_per_port_per_flow(
                global_context.packets_per_port_per_flow)
        database.import_seconds_per_port_per_flow(
                global_context.seconds_per_port_per_flow)
        database.import_bytes_per_domain_per_flow(
                global_context.bytes_per_domain_per_flow)
        database.import_packets_per_domain_per_flow(
                global_context.packets_per_domain_per_flow)
        database.import_seconds_per_domain_per_flow(
                global_context.seconds_per_domain_per_flow)
        database.import_bytes_per_device_per_flow(
                global_context.bytes_per_device_per_flow)
        database.import_packets_per_device_per_flow(
                global_context.packets_per_device_per_flow)
        database.import_seconds_per_device_per_flow(
                global_context.seconds_per_device_per_flow)
