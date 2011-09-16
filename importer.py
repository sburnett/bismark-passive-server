from datetime import datetime
from sqlalchemy.sql.expression import desc 

import schema

class PassiveUpdateImporter(object):
    def __init__(self, session):
        self._session = session

    def _insert_mac_address(self, session, data):
        mac_address = self._session.query(schema.MacAddress).\
                filter_by(session=session, data=data).\
                first()
        if not mac_address:
            mac_address = schema.MacAddress(session=session, data=data)
            self._session.add(mac_address)
        #self._session.flush()
        return mac_address

    def _insert_ip_address(self, session, data):
        ip_address = self._session.query(schema.IpAddress).\
                filter_by(session=session, data=data).\
                first()
        if not ip_address:
            ip_address = schema.IpAddress(session=session, data=data)
            self._session.add(ip_address)
        #self._session.flush()
        return ip_address

    def _insert_domain_name(self, session, data):
        domain_name = self._session.query(schema.DomainName).\
                filter_by(session=session, data=data).\
                first()
        if not domain_name:
            domain_name = schema.DomainName(session=session, data=data)
            self._session.add(domain_name)
        #self._session.flush()
        return domain_name

    def _insert_local_address(
            self, update, remote_address_id, mac_address, ip_address):
        local_address = self._session.query(schema.LocalAddress).\
                filter_by(update=update,
                          mac_address=mac_address,
                          ip_address=ip_address).\
                first()
        if not local_address:
            local_address = schema.LocalAddress(
                    update=update,
                    remote_address_id=remote_address_id,
                    mac_address=mac_address,
                    ip_address=ip_address)
            self._session.add(local_address)
        local_address.remote_address_id = remote_address_id
        self._session.merge(local_address)
        #self._session.flush()
        return local_address

    def _insert_dns_mapping(self, update, local_address, domain_name, ip_address):
        dns_mapping = self._session.query(schema.DnsMapping).\
                filter_by(update=update,
                          local_address=local_address,
                          domain_name=domain_name,
                          ip_address=ip_address).\
                first()
        if not dns_mapping:
            dns_mapping = schema.DnsMapping(
                    update=update,
                    local_address=local_address,
                    domain_name=domain_name,
                    ip_address=ip_address)
            self._session.add(dns_mapping)
        #self._session.flush()
        return dns_mapping

    def import_update(self, parsed_update):
        print 'HEADER'

        node = self._session.query(schema.Node).\
                filter_by(name=parsed_update.bismark_id).\
                first()
        if not node:
            node = schema.Node(name=parsed_update.bismark_id)
            self._session.add(node)

        if parsed_update.anonymized:
            signature = parsed_update.anonymization_signature
        else:
            signature = ''
        anonymization_context = self._session.query(schema.AnonymizationContext).\
                filter_by(node=node, signature=signature).\
                first()
        if not anonymization_context:
            anonymization_context = schema.AnonymizationContext(
                    node=node,
                    signature=signature)
            self._session.add(anonymization_context)

        session = self._session.query(schema.Session).\
                filter_by(anonymization_context=anonymization_context,
                          key=parsed_update.creation_time).\
                first()
        if not session:
            session = schema.Session(anonymization_context=anonymization_context,
                                     key=parsed_update.creation_time)
            self._session.add(session)

        update = self._session.query(schema.Update).\
                filter_by(session=session,
                          sequence_number=parsed_update.sequence_number).\
                first()
        if not update:
            update = schema.Update(
                    session=session,
                    sequence_number=parsed_update.sequence_number)
            self._session.add(update)
        try:
            update.pcap_received = parsed_update.pcap_received
            update.pcap_dropped = parsed_update.pcap_dropped
            update.iface_dropped = parsed_update.iface_dropped
            self._session.merge(update)
        except:
            pass

        print 'LOCAL ADDRESSES'

        for idx, entry in enumerate(parsed_update.addresses):
            mac_address = self._insert_mac_address(session, entry.mac_address)
            ip_address = self._insert_ip_address(session, entry.ip_address)
            remote_id = (parsed_update.address_table_first_id + idx) \
                    % parsed_update.address_table_size
            local_address = self._insert_local_address(
                    update, remote_id, mac_address, ip_address)

        print 'A RECORDS'

        local_addresses = self._session.\
                query(schema.LocalAddress).join(schema.Update).\
                filter(schema.Update.session == session).\
                order_by(desc(schema.Update.sequence_number)).\
                distinct(schema.LocalAddress.remote_address_id)
        local_address_map = dict(map(lambda la: (la.remote_address_id, la),
                                     local_addresses))

        for entry in parsed_update.a_records:
            local_address = local_address_map[entry.address_id]
            domain_name = self._insert_domain_name(session, entry.domain)
            ip_address = self._insert_ip_address(session, entry.ip_address)
            dns_mapping = self._insert_dns_mapping(update,
                                                   local_address,
                                                   domain_name,
                                                   ip_address)

        print 'CNAME RECORDS'

        for entry in parsed_update.cname_records:
            local_address = local_address_map[entry.address_id]
            dns_mappings = self._session.\
                    query(schema.DnsMapping).join(schema.DomainName).\
                    filter(schema.DnsMapping.local_address == local_address).\
                    filter(schema.DomainName.data == entry.cname)
            domain_name = self._insert_domain_name(session, entry.domain)
            for dns_mapping in dns_mappings:
                self._insert_dns_mapping(update,
                                         dns_mapping.local_address,
                                         domain_name,
                                         dns_mapping.ip_address)

        print 'FLOW TABLE'

        for entry in parsed_update.flow_table:
            source_ip = self._insert_ip_address(session, entry.source_ip)
            destination_ip = self._insert_ip_address(session,
                                                     entry.destination_ip)
            flow = schema.Flow(remote_flow_id=entry.flow_id,
                               update=update,
                               source_ip=source_ip,
                               destination_ip=destination_ip,
                               transport=entry.transport_protocol,
                               source_port=entry.source_port,
                               destination_port=entry.destination_port)
            self._session.add(flow)

        print 'PACKET SERIES'

        flow_ids = map(lambda entry: entry.flow_id, parsed_update.packet_series)

        flows = self._session.\
                query(schema.Flow).\
                join(schema.Update).\
                filter(schema.Update.session == session).\
                order_by(desc(schema.Update.sequence_number)).\
                distinct(schema.Flow.remote_flow_id)
        flowmap = dict(map(lambda f: (f.remote_flow_id, f), flows))

        for entry in parsed_update.packet_series:
            flow = flowmap.get(entry.flow_id)
            timestamp = datetime.utcfromtimestamp(entry.timestamp / 1e6)
            packet = schema.Packet(timestamp=timestamp,
                                   size=entry.size,
                                   flow=flow)
            self._session.add(packet)

        self._session.commit()
