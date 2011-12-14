from postgres_session_processor import PostgresProcessorCoordinator
from session_processor import SessionProcessor
import utils

from socket import IPPROTO_TCP, IPPROTO_UDP

class PacketSizeSessionProcessor(SessionProcessor):
    def __init__(self):
        super(PacketSizeSessionProcessor, self).__init__()

    def process_update(self, context, update):
        for packet in update.packet_series:
            self.process_packet(context, packet)

    def process_packet(self, context, packet):
        try:
            flow, flow_data = context.flows[packet.flow_id]
        except KeyError:
            flow = flow_data = None
        if flow is not None:
            if flow.destination_ip in context.address_map \
                    and flow.source_ip not in context.address_map:
                if flow.transport_protocol == IPPROTO_TCP:
                    context.packet_size_per_port_tcp[context.node_id,
                                                     flow.source_port,
                                                     packet.size][0] += 1
                elif flow.transport_protocol == IPPROTO_UDP:
                    context.packet_size_per_port_udp[context.node_id,
                                                     flow.source_port,
                                                     packet.size][0] += 1
            elif flow.source_ip in context.address_map \
                    and flow.destination_ip not in context.address_map:
                if flow.transport_protocol == IPPROTO_TCP:
                    context.packet_size_per_port_tcp[context.node_id,
                                                     flow.destination_port,
                                                     packet.size][1] += 1
                elif flow.transport_protocol == IPPROTO_UDP:
                    context.packet_size_per_port_udp[context.node_id,
                                                     flow.destination_port,
                                                     packet.size][1] += 1
            else:
                context.packet_size_per_port_unmatched[context.node_id,
                                                       packet.size] += 1
        else:
            context.packet_size_per_port_unmatched[context.node_id,
                                                   packet.size] += 1

class PacketSizeProcessorCoordinator(PostgresProcessorCoordinator):
    persistent_state = dict(
            packet_size_per_port_tcp=\
                    (utils.initialize_int_pair_dict, utils.sum_pair_dicts),
            packet_size_per_port_udp=\
                    (utils.initialize_int_pair_dict, utils.sum_pair_dicts),
            packet_size_per_port_unmatched=\
                    (utils.initialize_int_dict, utils.sum_dicts)
            )
    ephemeral_state = dict()

    def __init__(self, options):
        super(PacketSizeProcessorCoordinator, self).__init__(options)

    def create_processor(self, session):
        return PacketSizeSessionProcessor()

    def write_to_database(self, database, global_context):
        database.import_size_statistics(
                global_context.packet_size_per_port_tcp,
                global_context.packet_size_per_port_udp)
