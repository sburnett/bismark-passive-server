from sqlite_session_processor import SqliteProcessorCoordinator
from flow_properties_processor import FlowPropertiesSessionProcessor
import utils

class FlowStatisticsSessionProcessor(FlowPropertiesSessionProcessor):
    def __init__(self):
        super(FlowStatisticsSessionProcessor, self).__init__()
    
    def process_packet(self, context, packet, transport_protocol, port, device_names, domains):
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
                transport_protocol, port, domains, device_names \
                        = context.auxiliary_per_flow[flow_key]
                byte_count = context.bytes_per_flow_accumulator[flow_key]
                packet_count = context.packets_per_flow_accumulator[flow_key]
                start_time, end_time = context.seconds_per_flow_accumulator[flow_key]

                for domain in domains:
                    for device_name in device_names:
                        context.flow_statistics.append(
                                [context.node_id,
                                 context.anonymization_id,
                                 str(start_time),
                                 str(end_time),
                                 transport_protocol,
                                 port,
                                 domain,
                                 device_name,
                                 byte_count,
                                 packet_count])

                del context.auxiliary_per_flow[flow_key]
                del context.bytes_per_flow_accumulator[flow_key]
                del context.packets_per_flow_accumulator[flow_key]
                del context.seconds_per_flow_accumulator[flow_key]

        context.auxiliary_per_flow[flow_key] \
                = (transport_protocol, port, domains, device_names)
        context.bytes_per_flow_accumulator[flow_key] += packet.size
        context.packets_per_flow_accumulator[flow_key] += 1
        context.seconds_per_flow_accumulator[flow_key] \
                = (flow_data['start_time'], packet.timestamp)

class FlowStatisticsProcessorCoordinator(SqliteProcessorCoordinator):
    persistent_state = dict(
            flow_statistics=\
                    (list, utils.append_lists),
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
        for flow_key, (transport_protocol, port, domains, device_names) \
                in global_context.auxiliary_per_flow.iteritems():
            (node_id, anonymization_id, _, _) = flow_key

            byte_count = global_context.bytes_per_flow_accumulator[flow_key]
            packet_count = global_context.packets_per_flow_accumulator[flow_key]
            start_time, end_time = global_context.seconds_per_flow_accumulator[flow_key]

            for domain in domains:
                for device_name in device_names:
                    global_context.flow_statistics.append(
                            [node_id,
                             anonymization_id,
                             str(start_time),
                             str(end_time),
                             transport_protocol,
                             port,
                             domain,
                             device_name,
                             byte_count,
                             packet_count])

        super(FlowStatisticsProcessorCoordinator, self).finished_processing(
                global_context)

    def write_to_database(self, database, global_context):
        database.import_flow_statistics(global_context.flow_statistics)
