import re

from session_processor import SessionProcessor
from postgres_session_processor import PostgresProcessorCoordinator 
import utils
import inspect

class DomainSessionProcessor(SessionProcessor):
    def __init__(self):
        super(DomainSessionProcessor, self).__init__()
    
    def process_update(self, context, update):
        for offset, address in enumerate(update.addresses):
            index = (update.address_table_first_id + offset) \
                    % update.address_table_size
            context.address_id_map[index] = address.mac_address

        for a_record in update.a_records:
            self.process_a_record(context, update.bismark_id,\
                a_record)
        for cname_record in update.cname_records:
            self.process_cname_record(context, update.bismark_id,\
                cname_record)
    
    def process_a_record(self, context, bismark_id, a_record):
        if a_record.anonymized:
            return
        mac_address = context.address_id_map[a_record.address_id]
        device_key = bismark_id, mac_address
        domain = a_record.domain
        context.domains_accessed[device_key].add(domain)
    
    def process_cname_record(self, context, bismark_id, cname_record):
        if cname_record.anonymized:
            return
        mac_address = context.address_id_map[cname_record.address_id]
        device_key = bismark_id, mac_address
        domain = cname_record.domain
        context.domains_accessed[device_key].add(domain)
                
class DomainProcessorCoordinator(PostgresProcessorCoordinator):
    persistent_state = dict(
            address_id_map = (dict, None),
            domains_accessed = (utils.initialize_set_dict, utils.update_set),
            )
    ephemeral_state = dict()
    
    def __init__(self, options):
        super(DomainProcessorCoordinator, self).__init__(options)
    
    def create_processor(self, session):
        return DomainSessionProcessor()

    def write_to_database(self, database, global_context):
        database.import_domains_statistics(global_context.domains_accessed)
