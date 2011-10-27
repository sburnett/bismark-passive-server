from optparse import OptionParser
import psycopg2

def parse_args():
    usage = 'usage: %prog [options]'
    parser = OptionParser(usage=usage)
    parser.add_option('-u', '--username', action='store', dest='db_user',
                      default='sburnett', help='Database username')
    parser.add_option('-d', '--database', action='store', dest='db_name',
                      default='bismark_openwrt_live_v0_1',
                      help='Database name')
    options, args = parser.parse_args()
    return options, args

def connect(user, database):
    conn = psycopg2.connect(user=user, database=database)
    cur = conn.cursor()
    cur.execute('SET search_path TO bismark_passive')
    cur.close()
    conn.commit()
    return conn

def main():
    options, args = parse_args()
    conn = connect(options.db_user, options.db_name)
    cur = conn.cursor()

    view_results = []
    memoized_results = []

    cur.execute('SELECT * FROM bytes_per_hour ORDER BY node_id, eventstamp')
    view_results.append(cur.fetchall())
    cur.execute('SELECT * FROM bytes_per_hour_memoized ORDER BY node_id, eventstamp')
    memoized_results.append(cur.fetchall())
    cur.execute('SELECT * FROM bytes_per_day ORDER BY node_id, eventstamp')
    view_results.append(cur.fetchall())
    cur.execute('SELECT * FROM bytes_per_day_memoized ORDER BY node_id, eventstamp')
    memoized_results.append(cur.fetchall())

    cur.execute('SELECT * FROM bytes_per_port_per_hour ORDER BY node_id, eventstamp, port')
    view_results.append(cur.fetchall())
    cur.execute('SELECT * FROM bytes_per_port_per_hour_memoized ORDER BY node_id, eventstamp, port')
    memoized_results.append(cur.fetchall())
    cur.execute('SELECT * FROM bytes_per_port_per_day ORDER BY node_id, eventstamp, port')
    view_results.append(cur.fetchall())
    cur.execute('SELECT * FROM bytes_per_port_per_day_memoized ORDER BY node_id, eventstamp, port')
    memoized_results.append(cur.fetchall())

    cur.execute('SELECT * FROM bytes_per_domain_per_hour ORDER BY node_id, eventstamp, domain')
    view_results.append(cur.fetchall())
    cur.execute('SELECT * FROM bytes_per_domain_per_hour_memoized ORDER BY node_id, eventstamp, domain')
    memoized_results.append(cur.fetchall())
    cur.execute('SELECT * FROM bytes_per_domain_per_day ORDER BY node_id, eventstamp, domain')
    view_results.append(cur.fetchall())
    cur.execute('SELECT * FROM bytes_per_domain_per_day_memoized ORDER BY node_id, eventstamp, domain')
    memoized_results.append(cur.fetchall())

    cur.execute('SELECT * FROM bytes_per_device_per_hour ORDER BY node_id, anonymization_context, eventstamp, mac_address')
    view_results.append(cur.fetchall())
    cur.execute('SELECT * FROM bytes_per_device_per_hour_memoized ORDER BY node_id, anonymization_context, eventstamp, mac_address')
    memoized_results.append(cur.fetchall())
    cur.execute('SELECT * FROM bytes_per_device_per_day ORDER BY node_id, anonymization_context, eventstamp, mac_address')
    view_results.append(cur.fetchall())
    cur.execute('SELECT * FROM bytes_per_device_per_day_memoized ORDER BY node_id, anonymization_context, eventstamp, mac_address')
    memoized_results.append(cur.fetchall())

    cur.execute('SELECT * FROM bytes_per_device_per_port_per_hour ORDER BY node_id, eventstamp, anonymization_context, mac_address, port')
    view_results.append(cur.fetchall())
    cur.execute('SELECT * FROM bytes_per_device_per_port_per_hour_memoized ORDER BY node_id, anonymization_context, eventstamp, mac_address, port')
    memoized_results.append(cur.fetchall())
    cur.execute('SELECT * FROM bytes_per_device_per_port_per_day ORDER BY node_id, anonymization_context, eventstamp, mac_address, port')
    view_results.append(cur.fetchall())
    cur.execute('SELECT * FROM bytes_per_device_per_port_per_day_memoized ORDER BY node_id, anonymization_context, eventstamp, mac_address, port')
    memoized_results.append(cur.fetchall())

    cur.execute('SELECT * FROM bytes_per_device_per_domain_per_hour ORDER BY node_id, anonymization_context, eventstamp, mac_address, domain')
    view_results.append(cur.fetchall())
    cur.execute('SELECT * FROM bytes_per_device_per_domain_per_hour_memoized ORDER BY node_id, anonymization_context, eventstamp, mac_address, domain')
    memoized_results.append(cur.fetchall())
    cur.execute('SELECT * FROM bytes_per_device_per_domain_per_day ORDER BY node_id, anonymization_context, eventstamp, mac_address, domain')
    view_results.append(cur.fetchall())
    cur.execute('SELECT * FROM bytes_per_device_per_domain_per_day_memoized ORDER BY node_id, anonymization_context, eventstamp, mac_address, domain')
    memoized_results.append(cur.fetchall())

    for (view_result, memoized_result) in zip(view_results, memoized_results):
        assert(len(view_result) != 0)
        assert(view_result == memoized_result)
        print view_result == memoized_result

if __name__ == '__main__':
    main()
