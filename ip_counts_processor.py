from database_session_processor import DatabaseProcessorCoordinator
from session_processor import SessionProcessor
import utils

class IpCountsSessionProcessor(SessionProcessor):
    def __init__(self):
        super(IpCountsSessionProcessor, self).__init__()
    
    def process_update(self, context, update):
        for packet in update.packet_series:
            self.process_packet(context, packet)

    def process_packet(self, context, packet):
        try:
            flow, flow_data = context.flows[packet.flow_id]
        except KeyError:
            flow = flow_data = None
        if flow is not None:
            if flow.source_ip in context.address_map \
                    and flow.destination_ip not in context.address_map:
                ip = flow.destination_ip
            elif flow.destination_ip in context.address_map \
                    and flow.source_ip not in context.address_map:
                ip = flow.source_ip
            else:
                ip = 'unknown'
            context.bytes_per_ip[context.node_id,
                                 context.anonymization_id,
                                 ip] += packet.size
            context.packets_per_ip[context.node_id,
                                   context.anonymization_id,
                                   ip] += 1

class IpCountsProcessorCoordinator(DatabaseProcessorCoordinator):
    persistent_state = dict(
            bytes_per_ip=\
                    (utils.initialize_int_dict, utils.sum_dicts),
            packets_per_ip=\
                    (utils.initialize_int_dict, utils.sum_dicts),
            )
    ephemeral_state = dict()

    def __init__(self, options):
        super(IpCountsProcessorCoordinator, self).__init__(options)

    def create_processor(self, session):
        return IpCountsSessionProcessor()
    
    def write_to_database(self, database, global_context):
        database.import_bytes_per_ip(global_context.bytes_per_ip)
        database.import_packets_per_ip(global_context.packets_per_ip)
