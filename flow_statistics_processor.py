from flow_properties_processor import FlowPropertiesSessionProcessor
import utils

class FlowStatisticsSessionProcessor(FlowPropertiesSessionProcessor):
    persistent_state = dict(
            flow_statistics=(list, utils.append_lists),
            )
    ephemeral_state = dict(
            auxiliary_per_flow=(dict, None),
            bytes_per_flow_accumulator=(utils.initialize_int_dict, None),
            packets_per_flow_accumulator=(utils.initialize_int_dict, None),
            seconds_per_flow_accumulator=(utils.initialize_int_dict, None),
            )

    def __init__(self):
        super(FlowStatisticsSessionProcessor, self).__init__()
    
    def add_flow_statistics(context, flow_id):
        transport_protocol, port, domains, device_names \
                = context.auxiliary_per_flow[packet.flow_id]
        byte_count = context.bytes_per_flow_accumulator[flow_id]
        packet_count = context.packets_per_flow_accumulator[flow_id]
        start_time, end_time = context.seconds_per_flow_accumulator[flow_id]

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

    def process_packet(self, context, packet, transport_protocol, port, device_names, domains):
        try:
            flow, flow_data = context.flows[packet.flow_id]
        except KeyError:
            return
        if 'start_time' not in flow_data \
                or packet.timestamp < flow_data['start_time']:
            flow_data['start_time'] = packet.timestamp
        if 'flow_started' not in flow_data:
            flow_data['flow_started'] = True
            if packet.flow_id in context.auxiliary_per_flow:
                self.add_flow_statistics(context, packet.flow_id)
                del context.auxiliary_per_flow[packet.flow_id]
                del context.bytes_per_flow_accumulator[packet.flow_id]
                del context.packets_per_flow_accumulator[packet.flow_id]
                del context.seconds_per_flow_accumulator[packet.flow_id]

        context.auxiliary_per_flow[packet.flow_id] \
                = (transport_protocol, port, domains, device_names)
        context.bytes_per_flow_accumulator[packet.flow_id] += packet.size
        context.packets_per_flow_accumulator[packet.flow_id] += 1
        context.seconds_per_flow_accumulator[packet.flow_id] \
                = (flow_data['start_time'], packet.timestamp)

    def complete_session(self, context):
        for flow_id in context.auxiliary_per_flow.iterkeys():
            self.add_flow_statistics(context, flow_id)
