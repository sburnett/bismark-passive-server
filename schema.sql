CREATE SCHEMA passive;
SET search_path TO passive;

CREATE TABLE nodes (name varchar PRIMARY KEY);

CREATE TABLE anonymization_contexts (
    id SERIAL PRIMARY KEY,
    node_id varchar REFERENCES nodes (name) NOT NULL,
    signature varchar NOT NULL,
    UNIQUE (node_id, signature)
);

CREATE TABLE sessions (
    id SERIAL PRIMARY KEY,
    anonymization_context_id integer REFERENCES anonymization_contexts (id) NOT NULL,
    key varchar NOT NULL,
    UNIQUE (anonymization_context_id, key)
);

CREATE TABLE whitelisted_domains (
    session_id integer REFERENCES sessions (id) NOT NULL,
    domain varchar NOT NULL,
    PRIMARY KEY (session_id, domain)
);

CREATE TABLE updates (
    id SERIAL PRIMARY KEY,
    session_id integer REFERENCES sessions (id) NOT NULL,
    sequence_number integer NOT NULL,
    pcap_received integer,
    pcap_dropped integer,
    iface_dropped integer,
    packet_series_dropped integer,
    flow_table_size integer,
    flow_table_expired integer,
    flow_table_dropped integer,
    dropped_a_records integer,
    dropped_cname_records integer,
    UNIQUE (session_id, sequence_number)
);

CREATE TABLE mac_addresses (
    id SERIAL PRIMARY KEY,
    anonymization_context_id integer REFERENCES anonymization_contexts (id) NOT NULL,
    data varchar NOT NULL,
    UNIQUE (anonymization_context_id, data)
);

CREATE TABLE ip_addresses (
    id SERIAL PRIMARY KEY,
    anonymization_context_id integer REFERENCES anonymization_contexts (id) NOT NULL,
    data varchar NOT NULL,
    UNIQUE (anonymization_context_id, data)
);

CREATE TABLE domain_names (
    id SERIAL PRIMARY KEY,
    anonymization_context_id integer REFERENCES anonymization_contexts (id) NOT NULL,
    data varchar NOT NULL,
    UNIQUE (anonymization_context_id, data)
);

CREATE TABLE local_addresses (
    id SERIAL PRIMARY KEY,
    remote_id integer NOT NULL,
    update_id integer REFERENCES updates (id) NOT NULL,
    mac_address_id integer REFERENCES mac_addresses (id) NOT NULL,
    ip_address_id integer REFERENCES ip_addresses (id) NOT NULL,
    UNIQUE (update_id, mac_address_id, ip_address_id)
);

CREATE TABLE dns_a_records (
    id SERIAL PRIMARY KEY,
    update_id integer REFERENCES updates (id) NOT NULL,
    local_address_id integer REFERENCES local_addresses (id) NOT NULL,
    domain_name_id integer REFERENCES domain_names (id) NOT NULL,
    ip_address_id integer REFERENCES ip_addresses (id) NOT NULL,
    UNIQUE (update_id, local_address_id, domain_name_id, ip_address_id)
);

CREATE TABLE dns_cname_records (
    id SERIAL PRIMARY KEY,
    update_id integer REFERENCES updates (id) NOT NULL,
    local_address_id integer REFERENCES local_addresses (id) NOT NULL,
    domain_name_id integer REFERENCES domain_names (id) NOT NULL,
    cname_id integer REFERENCES domain_names (id) NOT NULL,
    UNIQUE (update_id, local_address_id, domain_name_id, cname_id)
);

CREATE TABLE flows (
    id SERIAL PRIMARY KEY,
    remote_id integer NOT NULL,
    update_id integer REFERENCES updates (id) NOT NULL,
    source_ip_id integer REFERENCES ip_addresses (id) NOT NULL,
    destination_ip_id integer REFERENCES ip_addresses (id) NOT NULL,
    transport_protocol integer NOT NULL,
    source_port integer NOT NULL,
    destination_port integer NOT NULL,
    UNIQUE (remote_id, update_id)
);

CREATE TABLE packets (
    id SERIAL PRIMARY KEY,
    update_id integer REFERENCES updates (id) NOT NULL,
    flow_id integer REFERENCES flows (id) NOT NULL,
    timestamp timestamp with time zone NOT NULL,
    size integer NOT NULL
);

CREATE FUNCTION merge_node(v_name varchar)
RETURNS varchar AS $$
BEGIN
     INSERT INTO nodes (name) SELECT v_name WHERE NOT EXISTS
     (SELECT 0 FROM nodes WHERE name = v_name);
     RETURN v_name;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION
merge_whitelisted_domain(v_session_id integer, v_domain varchar)
RETURNS void AS $$
BEGIN
     INSERT INTO whitelisted_domains (session_id, domain) SELECT v_session_id, v_domain WHERE NOT EXISTS
     (SELECT 0 FROM whitelisted_domains
        WHERE session_id = v_session_id
        AND domain = v_domain);
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION
merge_anonymization_context(v_node_id varchar, v_signature varchar)
RETURNS int AS $$
DECLARE
     v_id int;
BEGIN
     SELECT id INTO v_id FROM anonymization_contexts
     WHERE node_id = v_node_id AND signature = v_signature;
     IF NOT found THEN
         INSERT INTO anonymization_contexts (node_id, signature)
         VALUES (v_node_id, v_signature)
         RETURNING id INTO v_id;
     END IF;
     RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION
merge_session(v_anonymization_context_id int, v_key varchar)
RETURNS int AS $$
DECLARE
     v_id int;
BEGIN
    SELECT id INTO v_id FROM sessions
    WHERE anonymization_context_id = v_anonymization_context_id
    AND key = v_key;
    IF NOT found THEN
        INSERT INTO sessions (anonymization_context_id, key)
        VALUES (v_anonymization_context_id, v_key)
        RETURNING id INTO v_id;
    END IF;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION
merge_update(v_session_id int,
             v_sequence_number int,
             v_packet_series_dropped int,
             v_flow_table_size int,
             v_flow_table_expired int,
             v_flow_table_dropped int,
             v_dropped_a_records int,
             v_dropped_cname_records int,
             v_pcap_received int,
             v_pcap_dropped int,
             v_iface_dropped int
             )
RETURNS int AS $$
DECLARE
    v_id int;
BEGIN
    BEGIN
        INSERT INTO updates
        (session_id,
         sequence_number,
         packet_series_dropped,
         flow_table_size,
         flow_table_expired,
         flow_table_dropped,
         dropped_a_records,
         dropped_cname_records,
         pcap_received,
         pcap_dropped,
         iface_dropped)
        VALUES
        (v_session_id,
         v_sequence_number,
         v_packet_series_dropped,
         v_flow_table_size,
         v_flow_table_expired,
         v_flow_table_dropped,
         v_dropped_a_records,
         v_dropped_cname_records,
         v_pcap_received,
         v_pcap_dropped,
         v_iface_dropped)
        RETURNING id INTO v_id;
    EXCEPTION WHEN unique_violation THEN
        UPDATE updates SET
         packet_series_dropped = v_packet_series_dropped,
         flow_table_size = v_flow_table_size,
         flow_table_expired = v_flow_table_expired,
         flow_table_dropped = v_flow_table_dropped,
         dropped_a_records = v_dropped_a_records,
         dropped_cname_records = v_dropped_cname_records,
         pcap_received = v_pcap_received,
         pcap_dropped = v_pcap_dropped,
         iface_dropped = v_iface_dropped
        WHERE session_id = v_session_id
        AND sequence_number = v_sequence_number
        RETURNING id INTO v_id;
    END;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION
merge_mac_address(v_anonymization_context_id int, v_data varchar)
RETURNS int AS $$
DECLARE
    v_id int;
BEGIN
    SELECT id INTO v_id FROM mac_addresses
    WHERE anonymization_context_id = v_anonymization_context_id
    AND data = v_data;
    IF NOT found THEN
        INSERT INTO mac_addresses (anonymization_context_id, data)
        VALUES (v_anonymization_context_id, v_data)
        RETURNING id INTO v_id;
    END IF;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION
merge_ip_address(v_anonymization_context_id int, v_data varchar)
RETURNS int AS $$
DECLARE
    v_id int;
BEGIN
    SELECT id INTO v_id FROM ip_addresses
    WHERE anonymization_context_id = v_anonymization_context_id
    AND data = v_data;
    IF NOT found THEN
        INSERT INTO ip_addresses (anonymization_context_id, data)
        VALUES (v_anonymization_context_id, v_data)
        RETURNING id INTO v_id;
    END IF;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION
merge_domain_name(v_anonymization_context_id int, v_data varchar)
RETURNS int AS $$
DECLARE
    v_id int;
BEGIN
    SELECT id INTO v_id FROM domain_names
    WHERE anonymization_context_id = v_anonymization_context_id
    AND data = v_data;
    IF NOT found THEN
        INSERT INTO domain_names (anonymization_context_id, data)
        VALUES (v_anonymization_context_id, v_data)
        RETURNING id INTO v_id;
    END IF;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION
merge_local_address(v_remote_id int,
                    v_update_id int,
                    v_mac_address_id int,
                    v_ip_address_id int)
RETURNS int AS $$
DECLARE
    v_id int;
BEGIN
    UPDATE local_addresses SET remote_id = v_remote_id
    WHERE update_id = v_update_id
    AND mac_address_id = v_mac_address_id
    AND ip_address_id = v_ip_address_id
    RETURNING id INTO v_id;
    IF NOT found THEN
        INSERT INTO local_addresses
        (remote_id,
         update_id,
         mac_address_id,
         ip_address_id)
        VALUES (v_remote_id,
                v_update_id,
                v_mac_address_id,
                v_ip_address_id)
        RETURNING id INTO v_id;
    END IF;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION
merge_dns_a_record(v_update_id int,
                   v_local_address_id int,
                   v_domain_name_id int,
                   v_ip_address_id int)
RETURNS int AS $$
DECLARE
    v_id int;
BEGIN
    SELECT id INTO v_id FROM dns_a_records
    WHERE update_id = v_update_id
    AND local_address_id = v_local_address_id
    AND domain_name_id = v_domain_name_id
    AND ip_address_id = v_ip_address_id;
    IF NOT found THEN
        INSERT INTO dns_a_records
        (update_id,
         local_address_id,
         domain_name_id,
         ip_address_id)
        VALUES (v_update_id,
                v_local_address_id,
                v_domain_name_id,
                v_ip_address_id)
        RETURNING id INTO v_id;
    END IF;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION
merge_dns_cname_record(v_update_id int,
                       v_local_address_id int,
                       v_domain_name_id int,
                       v_cname_id int)
RETURNS int AS $$
DECLARE
    v_id int;
BEGIN
    SELECT id INTO v_id FROM dns_cname_records
    WHERE update_id = v_update_id
    AND local_address_id = v_local_address_id
    AND domain_name_id = v_domain_name_id
    AND cname_id = v_cname_id;
    IF NOT found THEN
        INSERT INTO dns_cname_records
        (update_id,
         local_address_id,
         domain_name_id,
         cname_id)
        VALUES (v_update_id,
                v_local_address_id,
                v_domain_name_id,
                v_cname_id)
        RETURNING id INTO v_id;
    END IF;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION
merge_flow(v_remote_id int,
           v_update_id int,
           v_source_ip_id int,
           v_destination_ip_id int,
           v_transport_protocol int,
           v_source_port int,
           v_destination_port int)
RETURNS int AS $$
DECLARE
    v_id int;
BEGIN
    UPDATE flows SET
    source_ip_id = v_source_ip_id,
    destination_ip_id = v_destination_ip_id,
    transport_protocol = v_transport_protocol,
    source_port = v_source_port,
    destination_port = v_destination_port
    WHERE remote_id = v_remote_id
    AND update_id = v_update_id
    RETURNING id INTO v_id;
    IF NOT found THEN
        INSERT INTO flows
        (remote_id,
         update_id,
         source_ip_id,
         destination_ip_id,
         transport_protocol,
         source_port,
         destination_port)
        VALUES (v_remote_id,
                v_update_id,
                v_source_ip_id,
                v_destination_ip_id,
                v_transport_protocol,
                v_source_port,
                v_destination_port)
        RETURNING id INTO v_id;
    END IF;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION
merge_packet(v_update_id int,
             v_flow_id int,
             v_timestamp timestamp with time zone,
             v_size int)
RETURNS int AS $$
DECLARE
    v_id int;
BEGIN
    INSERT INTO packets (update_id, flow_id, timestamp, size)
    VALUES (v_update_id, v_flow_id, v_timestamp, v_size)
    RETURNING id INTO v_id;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION
lookup_local_address_for_session(v_session_id int, v_remote_id int)
RETURNS int AS $$
DECLARE
    v_id int;
BEGIN
    SELECT local_addresses.id INTO v_id
    FROM local_addresses, updates
    WHERE remote_id = v_remote_id
    AND session_id = v_session_id
    AND update_id = updates.id
    ORDER BY sequence_number DESC
    LIMIT 1;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION
lookup_flow_for_session(v_session_id int, v_remote_id int)
RETURNS int AS $$
DECLARE
    v_id int;
BEGIN
    SELECT flows.id INTO v_id
    FROM flows, updates
    WHERE remote_id = v_remote_id
    AND session_id = v_session_id
    AND update_id = updates.id
    ORDER BY sequence_number DESC
    LIMIT 1;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION
count_out_of_order(v_session_id int, v_sequence_number int)
RETURNS int AS $$
DECLARE
    v_count int;
BEGIN
    SELECT COUNT(id) INTO v_count
    FROM updates
    WHERE session_id = v_session_id
    AND sequence_number >= v_sequence_number;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE VIEW packets_by_minute
(packet_id, rounded_timestamp) AS
SELECT id, date_trunc('minute', timestamp) FROM packets;

CREATE OR REPLACE VIEW packets_by_hour
(packet_id, rounded_timestamp) AS
SELECT id, date_trunc('hour', timestamp) FROM packets;

CREATE OR REPLACE VIEW packets_by_day
(packet_id, rounded_timestamp) AS
SELECT id, date_trunc('day', timestamp) FROM packets;

CREATE OR REPLACE VIEW bytes_per_minute
(node_id, timestamp, bytes_transferred) AS
SELECT node_id, rounded_timestamp, sum(size)
FROM packets, packets_by_minute, updates, sessions, anonymization_contexts, nodes
WHERE packets.id = packets_by_minute.packet_id
AND packets.update_id = updates.id
AND updates.session_id = sessions.id
AND sessions.anonymization_context_id = anonymization_contexts.id
GROUP BY rounded_timestamp, anonymization_contexts.node_id;

CREATE OR REPLACE VIEW bytes_per_hour
(node_id, timestamp, bytes_transferred) AS
SELECT node_id, rounded_timestamp, sum(size)
FROM packets, packets_by_hour, updates, sessions, anonymization_contexts, nodes
WHERE packets.id = packets_by_hour.packet_id
AND packets.update_id = updates.id
AND updates.session_id = sessions.id
AND sessions.anonymization_context_id = anonymization_contexts.id
GROUP BY rounded_timestamp, anonymization_contexts.node_id;

CREATE OR REPLACE VIEW bytes_per_day
(node_id, timestamp, bytes_transferred) AS
SELECT node_id, rounded_timestamp, sum(size)
FROM packets, packets_by_day, updates, sessions, anonymization_contexts, nodes
WHERE packets.id = packets_by_day.packet_id
AND packets.update_id = updates.id
AND updates.session_id = sessions.id
AND sessions.anonymization_context_id = anonymization_contexts.id
GROUP BY rounded_timestamp, anonymization_contexts.node_id;

--CREATE OR REPLACE VIEW bytes_per_hour
--(node_id, timestamp, bytes_transferred) AS
--SELECT node_id, date_trunc('hour', timestamp) AS rounded_timestamp, sum(size)
--FROM packets, updates, sessions, anonymization_contexts, nodes
--WHERE packets.update_id = updates.id
--AND updates.session_id = sessions.id
--AND sessions.anonymization_context_id = anonymization_contexts.id
--GROUP BY rounded_timestamp, anonymization_contexts.node_id;

CREATE OR REPLACE VIEW bytes_per_day
(node_id, timestamp, bytes_transferred) AS
SELECT node_id, date_trunc('day', timestamp) AS rounded_timestamp, sum(size)
FROM packets, updates, sessions, anonymization_contexts, nodes
WHERE packets.update_id = updates.id
AND updates.session_id = sessions.id
AND sessions.anonymization_context_id = anonymization_contexts.id
GROUP BY rounded_timestamp, anonymization_contexts.node_id;

CREATE OR REPLACE VIEW domains_for_ip_address
(ip_address_id, domain_name) AS
SELECT ip_address_id, domain_names.data
FROM dns_a_records, dns_cname_records, domain_names
WHERE dns_a_records.domain_name_id = domain_names.id
OR (dns_a_records.domain_name_id = dns_cname_records.cname_id
    AND dns_cname_records.domain_name_id = domain_names.id);

CREATE OR REPLACE VIEW whitelisted_domain_flows
(flow_id, domain) AS
SELECT DISTINCT flows.id, whitelisted_domains.domain
FROM flows, domains_for_ip_address, updates, sessions, whitelisted_domains
WHERE (flows.source_ip_id = domains_for_ip_address.ip_address_id
    OR flows.destination_ip_id = domains_for_ip_address.ip_address_id)
AND (domains_for_ip_address.domain_name LIKE '%.' || whitelisted_domains.domain
    OR domains_for_ip_address.domain_name = whitelisted_domains.domain)
AND flows.update_id = updates.id
AND updates.session_id = sessions.id
AND sessions.id = whitelisted_domains.session_id;

CREATE OR REPLACE VIEW bytes_per_domain_per_minute
(node_id, timestamp, domain, bytes_transferred) AS
SELECT anonymization_contexts.node_id, date_trunc('minute', timestamp) AS rounded_timestamp, whitelisted_domain_flows.domain, sum(packets.size)
FROM packets, whitelisted_domain_flows, updates, sessions, anonymization_contexts
WHERE packets.flow_id = whitelisted_domain_flows.flow_id
AND packets.update_id = updates.id
AND updates.session_id = sessions.id
AND sessions.anonymization_context_id = anonymization_contexts.id
GROUP BY rounded_timestamp, anonymization_contexts.node_id, whitelisted_domain_flows.domain;

CREATE OR REPLACE VIEW bytes_per_domain_per_hour
(node_id, timestamp, domain, bytes_transferred) AS
SELECT anonymization_contexts.node_id, date_trunc('hour', timestamp) AS rounded_timestamp, whitelisted_domain_flows.domain, sum(packets.size)
FROM packets, whitelisted_domain_flows, updates, sessions, anonymization_contexts
WHERE packets.flow_id = whitelisted_domain_flows.flow_id
AND packets.update_id = updates.id
AND updates.session_id = sessions.id
AND sessions.anonymization_context_id = anonymization_contexts.id
GROUP BY rounded_timestamp, anonymization_contexts.node_id, whitelisted_domain_flows.domain;

CREATE OR REPLACE VIEW bytes_per_domain_per_day
(node_id, timestamp, domain, bytes_transferred) AS
SELECT anonymization_contexts.node_id, date_trunc('day', timestamp) AS rounded_timestamp, whitelisted_domain_flows.domain, sum(packets.size)
FROM packets, whitelisted_domain_flows, updates, sessions, anonymization_contexts
WHERE packets.flow_id = whitelisted_domain_flows.flow_id
AND packets.update_id = updates.id
AND updates.session_id = sessions.id
AND sessions.anonymization_context_id = anonymization_contexts.id
GROUP BY rounded_timestamp, anonymization_contexts.node_id, whitelisted_domain_flows.domain;
