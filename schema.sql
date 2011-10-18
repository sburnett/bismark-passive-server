SET search_path TO bismark_passive;

CREATE TABLE bytes_per_minute (
    id SERIAL PRIMARY KEY,
    node_id varchar NOT NULL,
    timestamp timestamp with time zone NOT NULL,
    bytes_transferred integer NOT NULL,
    UNIQUE (node_id, timestamp)
);

CREATE OR REPLACE VIEW bytes_per_hour
(node_id, timestamp, bytes_transferred) AS
SELECT node_id, date_trunc('hour', timestamp) AS rounded_timestamp, sum(bytes_transferred)
FROM bytes_per_minute
GROUP BY rounded_timestamp, node_id;

CREATE OR REPLACE VIEW bytes_per_day
(node_id, timestamp, bytes_transferred) AS
SELECT node_id, date_trunc('day', timestamp) AS rounded_timestamp, sum(bytes_transferred)
FROM bytes_per_minute
GROUP BY rounded_timestamp, node_id;

CREATE OR REPLACE VIEW bytes_total
(node_id, bytes_transferred) AS
SELECT node_id, sum(bytes_transferred)
FROM bytes_per_minute
GROUP BY node_id;

CREATE TABLE bytes_per_port_per_minute (
    id SERIAL PRIMARY KEY,
    node_id varchar NOT NULL,
    timestamp timestamp with time zone NOT NULL,
    port integer NOT NULL,
    bytes_transferred integer NOT NULL,
    UNIQUE (node_id, port, timestamp)
);

CREATE OR REPLACE VIEW bytes_per_port_per_hour
(node_id, timestamp, port, bytes_transferred) AS
SELECT node_id, date_trunc('hour', timestamp) AS rounded_timestamp, port, sum(bytes_transferred)
FROM bytes_per_port_per_minute
GROUP BY rounded_timestamp, node_id, port;

CREATE OR REPLACE VIEW bytes_per_port_per_day
(node_id, timestamp, port, bytes_transferred) AS
SELECT node_id, date_trunc('day', timestamp) AS rounded_timestamp, port, sum(bytes_transferred)
FROM bytes_per_port_per_minute
GROUP BY rounded_timestamp, node_id, port;

CREATE OR REPLACE VIEW bytes_per_port_total
(node_id, port, bytes_transferred) AS
SELECT node_id, port, sum(bytes_transferred)
FROM bytes_per_port_per_minute
GROUP BY node_id, port;

CREATE TABLE bytes_per_domain_per_minute (
    id SERIAL PRIMARY KEY,
    node_id varchar NOT NULL,
    timestamp timestamp with time zone NOT NULL,
    domain varchar NOT NULL,
    bytes_transferred integer NOT NULL,
    UNIQUE (node_id, domain, timestamp)
);

CREATE OR REPLACE VIEW bytes_per_domain_per_hour
(node_id, timestamp, domain, bytes_transferred) AS
SELECT node_id, date_trunc('hour', timestamp) AS rounded_timestamp, domain, sum(bytes_transferred)
FROM bytes_per_domain_per_minute
GROUP BY rounded_timestamp, node_id, domain;

CREATE OR REPLACE VIEW bytes_per_domain_per_day
(node_id, timestamp, domain, bytes_transferred) AS
SELECT node_id, date_trunc('day', timestamp) AS rounded_timestamp, domain, sum(bytes_transferred)
FROM bytes_per_domain_per_minute
GROUP BY rounded_timestamp, node_id, domain;

CREATE OR REPLACE VIEW bytes_per_domain_total
(node_id, domain, bytes_transferred) AS
SELECT node_id, domain, sum(bytes_transferred)
FROM bytes_per_domain_per_minute
GROUP BY node_id, domain;

CREATE TABLE update_statistics (
    id SERIAL PRIMARY KEY,
    node_id varchar NOT NULL,
    timestamp timestamp with time zone NOT NULL,
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
    UNIQUE (node_id, timestamp)
);

