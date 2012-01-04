import re

from session_processor import SessionProcessor
from postgres_session_processor import PostgresProcessorCoordinator 
import utils
from datetime import datetime

class DomainSessionProcessor(SessionProcessor):
    def __init__(self):
        super(DomainSessionProcessor, self).__init__()
    
    def process_update(self, context, update):
        update_date = datetime.date(update.timestamp)
        update_hour = update.timestamp.replace(minute = 0, second = 0,\
            mirosecond = 0)
        for offset, address in enumerate(update.addresses):
            index = (update.address_table_first_id + offset) \
                    % update.address_table_size
            context.address_id_map[index] = address.mac_address
            context.device_visibility[address.mac_address,\
                update_date].add(update_hour)

        for a_record in update.a_records:
            self.process_a_record(context, update.bismark_id,\
                a_record, update.timestamp)
        for cname_record in update.cname_records:
            self.process_cname_record(context, update.bismark_id,\
                cname_record, update.timestamp)
    
    def process_a_record(self, context, bismark_id, a_record, timestamp):
        if a_record.anonymized:
            return
        mac_address = context.address_id_map[a_record.address_id]
        domain = a_record.domain
        record_key = bismark_id, mac_address, domain
        context.domains_accessed[record_key].append(\
            datetime.date(timestamp.replace(hour = 0, minute = 0,\
            second = 0, microsecond = 0)))
    
    def process_cname_record(self, context, bismark_id, c_record, timestamp):
        if c_record.anonymized:
            return
        mac_address = context.address_id_map[c_record.address_id]
        domain = c_record.domain
        record_key = bismark_id, mac_address, domain
        context.domains_accessed[record_key].append(\
            datetime.date(timestamp.replace(hour = 0, minute = 0,\
            second = 0, microsecond = 0)))
                
class DomainProcessorCoordinator(PostgresProcessorCoordinator):
    '''
        address_id_map: 
            keeps a map from address_id to mac address to look up the mac
            address when we see a certain address_id
        domains_accessed: 
            for each device, domain, store the dates on which it was accessed
        device_visibility: 
            for each device and date stores the hours on which
            the device was visible. Can generate a score of 0-24 to see the 
            number of hours in a day that the device was visible
    '''
    persistent_state = dict(
            address_id_map = (dict, None),
            domains_accessed = (utils.initialize_list_dict,\
                utils.sum_dicts),
            device_visibility = (utils.initialize_set_dict,\
                utils.sum_dicts),
            )
    ephemeral_state = dict()
    
    def __init__(self, options):
        super(DomainProcessorCoordinator, self).__init__(options)
    
    def create_processor(self, session):
        return DomainSessionProcessor()

    def write_to_database(self, database, global_context):
        database.import_domains_statistics(global_context.domains_accessed)
        database.import_device_visiblity(global_context.device_visibility)
