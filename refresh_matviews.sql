SET search_path TO bismark_passive;

SELECT execute('SELECT refresh_matview('''||mv_name||''');') FROM matviews;
