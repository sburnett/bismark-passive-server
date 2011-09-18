from datetime import datetime
from sqlalchemy.sql.expression import desc

import schema

class PassiveUpdateImporter(object):
    def __init__(self, session):
        self._session = session

    def _insert_entry(self, Cl, **kwargs):
        ob = self._session.query(Cl).filter_by(**kwargs).first()
        if not ob:
            ob = Cl(**kwargs)
            self._session.add(ob)
        return ob

    def _insert_mac_address(self, anonymization_context, data):
        return self._insert_entry(schema.MacAddress,
                                  anonymization_context=anonymization_context,
                                  data=data)

    def _insert_ip_address(self, anonymization_context, data):
        return self._insert_entry(schema.IpAddress,
                                  anonymization_context=anonymization_context,
                                  data=data)

    def _insert_domain_name(self, anonymization_context, data):
        return self._insert_entry(schema.DomainName,
                                  anonymization_context=anonymization_context,
                                  data=data)

    def _insert_local_address(
            self, remote_id, update, mac_address, ip_address):
        local_address = self._insert_entry(
                schema.LocalAddress,
                update=update,
                mac_address=mac_address,
                ip_address=ip_address)
        local_address.remote_id = remote_id
        self._session.merge(local_address)
        return local_address

    def _insert_dns_a_record(self, update, local_address, domain_name, ip_address):
        return self._insert_entry(schema.DnsARecord,
                                  update=update,
                                  local_address=local_address,
                                  domain_name=domain_name,
                                  ip_address=ip_address)

    def _insert_dns_cname_record(self, update, local_address, domain_name, cname):
        return self._insert_entry(schema.DnsCnameRecord,
                                  update=update,
                                  local_address=local_address,
                                  domain_name=domain_name,
                                  cname=cname)

    def _lookup_local_addresses_for_session(self, session):
        local_addresses = self._session.\
                query(schema.LocalAddress).join(schema.Update).\
                filter(schema.Update.session == session).\
                order_by(desc(schema.Update.sequence_number)).\
                distinct(schema.LocalAddress.remote_id)
        return dict(map(lambda la: (la.remote_id, la), local_addresses))

    def _lookup_flows_for_session(self, session):
        flows = self._session.\
                query(schema.Flow).\
                join(schema.Update).\
                filter(schema.Update.session == session).\
                order_by(desc(schema.Update.sequence_number)).\
                distinct(schema.Flow.remote_id)
        return dict(map(lambda f: (f.remote_id, f), flows))

    def import_update(self, parsed_update):
        node = self._insert_entry(schema.Node, name=parsed_update.bismark_id)
        if parsed_update.anonymized:
            signature = parsed_update.anonymization_signature
        else:
            signature = ''
        anonymization_context = self._insert_entry(schema.AnonymizationContext,
                                                   node=node,
                                                   signature=signature)
        session = self._insert_entry(
                schema.Session,
                anonymization_context=anonymization_context,
                key=parsed_update.creation_time)
        update = self._insert_entry(
                schema.Update,
                session=session,
                sequence_number=parsed_update.sequence_number)
        try:
            update.pcap_received = parsed_update.pcap_received
            update.pcap_dropped = parsed_update.pcap_dropped
            update.iface_dropped = parsed_update.iface_dropped
            self._session.merge(update)
        except:
            pass

        for idx, entry in enumerate(parsed_update.addresses):
            mac_address = self._insert_mac_address(anonymization_context,
                                                   entry.mac_address)
            ip_address = self._insert_ip_address(anonymization_context,
                                                 entry.ip_address)
            remote_id = (parsed_update.address_table_first_id + idx) \
                    % parsed_update.address_table_size
            local_address = self._insert_local_address(
                    remote_id, update, mac_address, ip_address)

        local_addresses = self._lookup_local_addresses_for_session(session)
        for entry in parsed_update.a_records:
            local_address = local_addresses[entry.address_id]
            domain_name = self._insert_domain_name(anonymization_context,
                                                   entry.domain)
            ip_address = self._insert_ip_address(anonymization_context,
                                                 entry.ip_address)
            self._insert_dns_a_record(update,
                                      local_address,
                                      domain_name,
                                      ip_address)
        for entry in parsed_update.cname_records:
            local_address = local_addresses[entry.address_id]
            domain_name = self._insert_domain_name(anonymization_context,
                                                   entry.domain)
            cname = self._insert_domain_name(anonymization_context,
                                             entry.cname)
            self._insert_dns_cname_record(update,
                                          local_address,
                                          domain_name,
                                          cname)

        for entry in parsed_update.flow_table:
            source_ip = self._insert_ip_address(anonymization_context,
                                                entry.source_ip)
            destination_ip = self._insert_ip_address(anonymization_context,
                                                     entry.destination_ip)
            flow = schema.Flow(remote_id=entry.flow_id,
                               update=update,
                               source_ip=source_ip,
                               destination_ip=destination_ip,
                               transport_protocol=entry.transport_protocol,
                               source_port=entry.source_port,
                               destination_port=entry.destination_port)
            self._session.add(flow)

        flows = self._lookup_flows_for_session(session)
        for entry in parsed_update.packet_series:
            flow = flows.get(entry.flow_id)
            timestamp = datetime.utcfromtimestamp(entry.timestamp / 1e6)
            packet = schema.Packet(timestamp=timestamp,
                                   size=entry.size,
                                   flow=flow)
            self._session.add(packet)

        self._session.commit()
