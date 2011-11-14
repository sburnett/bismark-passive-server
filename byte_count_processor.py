from collections import defaultdict
from datetime import datetime
import multiprocessing

from database_session_processor import DatabaseProcessorCoordinator
from session_processor import SessionProcessor
import utils

class ByteCountSessionProcessor(SessionProcessor):
    def __init__(self, fingerprint, timestamp_queue):
        super(ByteCountSessionProcessor, self).__init__()
        self._oldest_timestamp = datetime.max
        self._fingerprint = fingerprint
        self._timestamp_queue = timestamp_queue
    
    def process_update(self, context, update):
        for packet in update.packet_series:
            self.process_packet(context, packet)

    def finished_session(self):
        self._timestamp_queue.put((self._fingerprint, self._oldest_timestamp))

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

        self._oldest_timestamp = min(self._oldest_timestamp, rounded_timestamp)

class ByteCountProcessorCoordinator(DatabaseProcessorCoordinator):
    states = dict(
            bytes_per_minute=(utils.initialize_int_dict, utils.sum_dicts),
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
        self._rebuild = rebuild
        if rebuild:
            self._oldest_timestamps = defaultdict(lambda: datetime.min)
        else:
            self._oldest_timestamps = defaultdict(lambda: datetime.max)
        manager = multiprocessing.Manager()
        self._timestamp_queue = manager.Queue()
        self._num_processors = 0

    def create_processor(self, session):
        self._num_processors += 1
        return ByteCountSessionProcessor((session.node_id,
                                          session.anonymization_context),
                                         self._timestamp_queue)
    
    def finished_processing(self, global_context):
        while self._num_processors > 0:
            (node_id, anonymization_id), oldest_timestamp \
                    = self._timestamp_queue.get()
            self._num_processors -= 1

            self._oldest_timestamps[node_id] \
                    = min(self._oldest_timestamps[node_id], oldest_timestamp)
            context_key = node_id, anonymization_id
            self._oldest_timestamps[context_key] \
                    = min(self._oldest_timestamps[context_key],
                          oldest_timestamp)
        super(ByteCountProcessorCoordinator, self)\
                .finished_processing(global_context)

    def write_to_database(self, database, global_context):
        print 'Oldest timestamps:', self._oldest_timestamps
        database.import_bytes_per_minute(
                global_context.bytes_per_minute,
                self._oldest_timestamps)
        database.import_bytes_per_port_per_minute(
                global_context.bytes_per_port_per_minute,
                self._oldest_timestamps)
        database.import_bytes_per_domain_per_minute(
                global_context.bytes_per_domain_per_minute,
                self._oldest_timestamps)
        database.import_bytes_per_device_per_minute(
                global_context.bytes_per_device_per_minute,
                self._oldest_timestamps)
        database.import_bytes_per_device_per_port_per_minute(
                global_context.bytes_per_device_per_port_per_minute,
                self._oldest_timestamps)
        database.import_bytes_per_device_per_domain_per_minute(
                global_context.bytes_per_device_per_domain_per_minute,
                self._oldest_timestamps)
        database.refresh_matviews(self._oldest_timestamps)
