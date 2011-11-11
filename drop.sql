SET search_path TO bismark_passive;

DROP TABLE bytes_per_minute CASCADE;
DROP TABLE bytes_per_port_per_minute CASCADE;
DROP TABLE bytes_per_domain_per_minute CASCADE;
DROP TABLE bytes_per_device_per_minute CASCADE;
DROP TABLE bytes_per_device_per_port_per_minute CASCADE;
DROP TABLE bytes_per_device_per_domain_per_minute CASCADE;
DROP TABLE update_statistics CASCADE;
DROP TABLE packet_sizes_per_port CASCADE;

SELECT execute('SELECT drop_matview('''||mv_name||''');') FROM matviews;
DROP TABLE matviews;

DROP FUNCTION merge_bytes_per_minute(
    varchar, timestamp with time zone, int);
DROP FUNCTION merge_bytes_per_port_per_minute(
    varchar, timestamp with time zone, integer, integer);
DROP FUNCTION merge_bytes_per_domain_per_minute(
    varchar, timestamp with time zone, varchar, integer);
DROP FUNCTION merge_bytes_per_device_per_minute(
    varchar, varchar, timestamp with time zone, varchar, integer);
DROP FUNCTION merge_bytes_per_device_per_port_per_minute(
    varchar, varchar, timestamp with time zone, varchar, integer, integer);
DROP FUNCTION merge_bytes_per_device_per_domain_per_minute(
    varchar, varchar, timestamp with time zone, varchar, varchar, integer);
DROP FUNCTION merge_update_statistics(
    varchar, timestamp with time zone, integer, integer, integer, integer,
    integer, integer, integer, integer, integer, integer);
DROP FUNCTION merge_packet_size_per_port(
    varchar, integer, integer, integer);
DROP FUNCTION refresh_matviews_node_latest(varchar, timestamp with time zone);
DROP FUNCTION refresh_matviews_context_latest(
    varchar, varchar, timestamp with time zone);

DROP FUNCTION refresh_matview_node_latest(
    name, varchar, timestamp with time zone, varchar);
DROP FUNCTION refresh_matview_context_latest(
    name, varchar, varchar, timestamp with time zone, varchar);
