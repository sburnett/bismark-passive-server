from abc import abstractmethod

from session_processor import PersistentSessionProcessor

class FlowPropertiesSessionProcessor(PersistentSessionProcessor):
    @abstractmethod
    def process_packet(self,
                       context,
                       packet,
                       transport_protocol,
                       port,
                       device_names,
                       domains):
        pass

    def process_update_persistent(self, context, update):
        for packet in update.packet_series:
            try:
                flow, flow_data = context.flows[packet.flow_id]
            except KeyError:
                flow = flow_data = None

            if flow is not None:
                transport_protocol = flow.transport_protocol
            else:
                transport_protocol = -1

            if flow is not None \
                    and flow.source_ip in context.address_map \
                    and flow.destination_ip not in context.address_map:
                port = flow.destination_port
            elif flow is not None \
                    and flow.destination_ip in context.address_map \
                    and flow.source_ip not in context.address_map:
                port = flow.source_port
            else:
                port = -1

            device_names = []
            if flow is not None and flow.source_ip in context.mac_address_map:
                device_names.append(context.mac_address_map[flow.source_ip])
            if flow is not None \
                    and flow.destination_ip in context.mac_address_map:
                device_names.append(
                        context.mac_address_map[flow.destination_ip])
            if device_names == []:
                device_names = ['unknown']

            if flow_data is not None:
                domains = flow_data['domains']
            else:
                domains = ['unknown']

            self.process_packet(context,
                                packet,
                                transport_protocol,
                                port,
                                device_names,
                                domains)
