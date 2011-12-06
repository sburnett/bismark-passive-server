import re

from session_processor import ProcessorCoordinator, SessionProcessor
import utils

class DomainsPerFlowSessionProcessor(SessionProcessor):
    def __init__(self):
        super(DomainsPerFlowSessionProcessor, self).__init__()

    def process_update(self, context, update):
        for packet in update.packet_series:
            try:
                flow, flow_data = context.flows[packet.flow_id]
            except KeyError:
                continue
            if 'domains' in flow_data:
                continue
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

class DomainsPerFlowProcessorCoordinator(ProcessorCoordinator):
    persistent_state = dict()
    ephemeral_state = dict()

    def __init__(self, options):
        super(DomainsPerFlowProcessorCoordinator, self).__init__(options)

    def create_processor(self, session):
        return DomainsPerFlowSessionProcessor()
