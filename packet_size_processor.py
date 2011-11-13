from database_session_processor import DatabaseSessionProcessor
import utils

class PacketSizeProcessor(DatabaseSessionProcessor):
    states = dict(
            packet_size_per_port=\
                    (utils.initialize_int_dict, utils.sum_dicts)
            )

    def __init__(self, username, database, rebuild=False):
        super(PacketSizeProcessor, self).__init__(username, database)

    def process_update(self, context, update):
        for packet in update.packet_series:
            self.process_packet(context, packet)

    def write_to_database(self, database, global_context):
        database.import_size_statistics(global_context.packet_size_per_port)

    def process_packet(self, context, packet):
        rounded_timestamp = packet.timestamp.replace(second=0, microsecond=0)
        flow = context.flows.get(packet.flow_id)
        if flow is not None:
            if flow.source_ip in context.address_map \
                    and flow.destination_ip not in context.address_map:
                context.packet_size_per_port[context.node_id,
                                             flow.destination_port,
                                             packet.size] += 1
            elif flow.destination_ip in context.address_map \
                    and flow.source_ip not in context.address_map:
                context.packet_size_per_port[context.node_id,
                                             flow.source_port,
                                             packet.size] += 1
