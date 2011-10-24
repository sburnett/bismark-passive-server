SET search_path TO bismark_passive;

DROP TABLE bytes_per_minute CASCADE;
DROP TABLE bytes_per_port_per_minute CASCADE;
DROP TABLE bytes_per_domain_per_minute CASCADE;
DROP TABLE bytes_per_device_per_minute CASCADE;
DROP TABLE bytes_per_device_per_port_per_minute CASCADE;
DROP TABLE bytes_per_device_per_domain_per_minute CASCADE;
DROP TABLE update_statistics CASCADE;

SELECT execute('SELECT drop_matview('''||mv_name||''');') FROM matviews;
DROP TABLE matviews;
