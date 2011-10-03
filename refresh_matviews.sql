SET search_path TO bismark_passive;

SELECT refresh_matview('mv_bytes_per_minute');
SELECT refresh_matview('mv_unanonymized_domain_names');
SELECT refresh_matview('mv_first_packet_in_flow');
SELECT refresh_matview('mv_time_window_for_dns_a_records');
SELECT refresh_matview('mv_time_window_for_dns_cname_records');
SELECT refresh_matview('mv_domains_for_flow');
SELECT refresh_matview('mv_bytes_per_domain_per_minute');
SELECT refresh_matview('mv_bytes_per_port_per_minute');
