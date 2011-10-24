SET search_path TO bismark_passive;

CREATE TABLE bytes_per_minute (
    id SERIAL PRIMARY KEY,
    node_id varchar NOT NULL,
    eventstamp timestamp with time zone NOT NULL,
    bytes_transferred integer NOT NULL,
    UNIQUE (node_id, eventstamp)
);

CREATE OR REPLACE VIEW bytes_per_hour
(node_id, eventstamp, bytes_transferred) AS
SELECT node_id, date_trunc('hour', eventstamp) AS rounded_eventstamp, sum(bytes_transferred)
FROM bytes_per_minute
GROUP BY rounded_eventstamp, node_id;

CREATE OR REPLACE VIEW bytes_per_day
(node_id, eventstamp, bytes_transferred) AS
SELECT node_id, date_trunc('day', eventstamp) AS rounded_eventstamp, sum(bytes_transferred)
FROM bytes_per_minute
GROUP BY rounded_eventstamp, node_id;

CREATE OR REPLACE VIEW bytes_total
(node_id, bytes_transferred) AS
SELECT node_id, sum(bytes_transferred)
FROM bytes_per_minute
GROUP BY node_id;

CREATE TABLE bytes_per_port_per_minute (
    id SERIAL PRIMARY KEY,
    node_id varchar NOT NULL,
    eventstamp timestamp with time zone NOT NULL,
    port integer NOT NULL,
    bytes_transferred integer NOT NULL,
    UNIQUE (node_id, port, eventstamp)
);

CREATE OR REPLACE VIEW bytes_per_port_per_hour
(node_id, eventstamp, port, bytes_transferred) AS
SELECT node_id, date_trunc('hour', eventstamp) AS rounded_eventstamp, port, sum(bytes_transferred)
FROM bytes_per_port_per_minute
GROUP BY rounded_eventstamp, node_id, port;

CREATE OR REPLACE VIEW bytes_per_port_per_day
(node_id, eventstamp, port, bytes_transferred) AS
SELECT node_id, date_trunc('day', eventstamp) AS rounded_eventstamp, port, sum(bytes_transferred)
FROM bytes_per_port_per_minute
GROUP BY rounded_eventstamp, node_id, port;

CREATE OR REPLACE VIEW bytes_per_port_total
(node_id, port, bytes_transferred) AS
SELECT node_id, port, sum(bytes_transferred)
FROM bytes_per_port_per_minute
GROUP BY node_id, port;

CREATE TABLE bytes_per_domain_per_minute (
    id SERIAL PRIMARY KEY,
    node_id varchar NOT NULL,
    eventstamp timestamp with time zone NOT NULL,
    domain varchar NOT NULL,
    bytes_transferred integer NOT NULL,
    UNIQUE (node_id, domain, eventstamp)
);

CREATE OR REPLACE VIEW bytes_per_domain_per_hour
(node_id, eventstamp, domain, bytes_transferred) AS
SELECT node_id, date_trunc('hour', eventstamp) AS rounded_eventstamp, domain, sum(bytes_transferred)
FROM bytes_per_domain_per_minute
GROUP BY rounded_eventstamp, node_id, domain;

CREATE OR REPLACE VIEW bytes_per_domain_per_day
(node_id, eventstamp, domain, bytes_transferred) AS
SELECT node_id, date_trunc('day', eventstamp) AS rounded_eventstamp, domain, sum(bytes_transferred)
FROM bytes_per_domain_per_minute
GROUP BY rounded_eventstamp, node_id, domain;

CREATE OR REPLACE VIEW bytes_per_domain_total
(node_id, domain, bytes_transferred) AS
SELECT node_id, domain, sum(bytes_transferred)
FROM bytes_per_domain_per_minute
GROUP BY node_id, domain;

CREATE TABLE bytes_per_device_per_minute (
    id SERIAL PRIMARY KEY,
    node_id varchar NOT NULL,
    anonymization_context varchar NOT NULL,
    eventstamp timestamp with time zone NOT NULL,
    mac_address varchar NOT NULL,
    bytes_transferred integer NOT NULL,
    UNIQUE (node_id, anonymization_context, mac_address, eventstamp)
);

CREATE OR REPLACE VIEW bytes_per_device_per_hour
(node_id, anonymization_context, eventstamp, mac_address, bytes_transferred) AS
SELECT node_id, anonymization_context, date_trunc('hour', eventstamp) AS rounded_eventstamp, mac_address, sum(bytes_transferred)
FROM bytes_per_device_per_minute
GROUP BY rounded_eventstamp, node_id, anonymization_context, mac_address;

CREATE OR REPLACE VIEW bytes_per_device_per_day
(node_id, anonymization_context, eventstamp, mac_address, bytes_transferred) AS
SELECT node_id, anonymization_context, date_trunc('day', eventstamp) AS rounded_eventstamp, mac_address, sum(bytes_transferred)
FROM bytes_per_device_per_minute
GROUP BY rounded_eventstamp, node_id, anonymization_context, mac_address;

CREATE OR REPLACE VIEW bytes_per_device_total
(node_id, anonymization_context, mac_address, bytes_transferred) AS
SELECT node_id, anonymization_context, mac_address, sum(bytes_transferred)
FROM bytes_per_device_per_minute
GROUP BY node_id, anonymization_context, mac_address;

CREATE TABLE bytes_per_device_per_port_per_minute (
    id SERIAL PRIMARY KEY,
    node_id varchar NOT NULL,
    anonymization_context varchar NOT NULL,
    eventstamp timestamp with time zone NOT NULL,
    mac_address varchar NOT NULL,
    port integer NOT NULL,
    bytes_transferred integer NOT NULL,
    UNIQUE (node_id, anonymization_context, mac_address, port, eventstamp)
);

CREATE OR REPLACE VIEW bytes_per_device_per_port_per_hour
(node_id, anonymization_context, eventstamp, mac_address, port, bytes_transferred) AS
SELECT node_id, anonymization_context, date_trunc('hour', eventstamp) AS rounded_eventstamp, mac_address, port, sum(bytes_transferred)
FROM bytes_per_device_per_port_per_minute
GROUP BY rounded_eventstamp, node_id, anonymization_context, mac_address, port;

CREATE OR REPLACE VIEW bytes_per_device_per_port_per_day
(node_id, anonymization_context, eventstamp, mac_address, port, bytes_transferred) AS
SELECT node_id, anonymization_context, date_trunc('day', eventstamp) AS rounded_eventstamp, mac_address, port, sum(bytes_transferred)
FROM bytes_per_device_per_port_per_minute
GROUP BY rounded_eventstamp, node_id, anonymization_context, mac_address, port;

CREATE OR REPLACE VIEW bytes_per_device_per_port_total
(node_id, anonymization_context, mac_address, port, bytes_transferred) AS
SELECT node_id, anonymization_context, mac_address, port, sum(bytes_transferred)
FROM bytes_per_device_per_port_per_minute
GROUP BY node_id, anonymization_context, mac_address, port;

CREATE TABLE bytes_per_device_per_domain_per_minute (
    id SERIAL PRIMARY KEY,
    node_id varchar NOT NULL,
    anonymization_context varchar NOT NULL,
    eventstamp timestamp with time zone NOT NULL,
    mac_address varchar NOT NULL,
    domain varchar NOT NULL,
    bytes_transferred integer NOT NULL,
    UNIQUE (node_id, anonymization_context, mac_address, domain, eventstamp)
);

CREATE OR REPLACE VIEW bytes_per_device_per_domain_per_hour
(node_id, anonymization_context, eventstamp, mac_address, domain, bytes_transferred) AS
SELECT node_id, anonymization_context, date_trunc('hour', eventstamp) AS rounded_eventstamp, mac_address, domain, sum(bytes_transferred)
FROM bytes_per_device_per_domain_per_minute
GROUP BY rounded_eventstamp, node_id, anonymization_context, mac_address, domain;

CREATE OR REPLACE VIEW bytes_per_device_per_domain_per_day
(node_id, anonymization_context, eventstamp, mac_address, domain, bytes_transferred) AS
SELECT node_id, anonymization_context, date_trunc('day', eventstamp) AS rounded_eventstamp, mac_address, domain, sum(bytes_transferred)
FROM bytes_per_device_per_domain_per_minute
GROUP BY rounded_eventstamp, node_id, anonymization_context, mac_address, domain;

CREATE OR REPLACE VIEW bytes_per_device_per_domain_total
(node_id, anonymization_context, mac_address, domain, bytes_transferred) AS
SELECT node_id, anonymization_context, mac_address, domain, sum(bytes_transferred)
FROM bytes_per_device_per_domain_per_minute
GROUP BY node_id, anonymization_context, mac_address, domain;

CREATE TABLE update_statistics (
    id SERIAL PRIMARY KEY,
    node_id varchar NOT NULL,
    eventstamp timestamp with time zone NOT NULL,
    pcap_dropped integer NOT NULL,
    iface_dropped integer NOT NULL,
    packet_series_dropped integer NOT NULL,
    flow_table_dropped integer NOT NULL,
    dropped_a_records integer NOT NULL,
    dropped_cname_records integer NOT NULL,
    packet_series_size integer NOT NULL,
    flow_table_size integer NOT NULL,
    a_records_size integer NOT NULL,
    cname_records_size integer NOT NULL,
    UNIQUE (node_id, eventstamp)
);

CREATE OR REPLACE FUNCTION execute(text) returns void as $BODY$BEGIN execute $1; END;$BODY$ language plpgsql;
SELECT execute('GRANT SELECT ON bismark_passive.'||tablename||' to abhishek;')
FROM pg_tables WHERE schemaname = 'bismark_passive';
SELECT execute('GRANT SELECT ON bismark_passive.'||viewname||' to abhishek;')
FROM pg_views WHERE schemaname = 'bismark_passive';

\i materialized_views.sql
SELECT execute('SELECT create_matview('''||viewname||'_memoized'', '''||viewname||''');')
FROM pg_views WHERE schemaname = 'bismark_passive';
