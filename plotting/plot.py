#!/usr/bin/env python

from collections import defaultdict
import datetime
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.dates import DayLocator, HourLocator, DateFormatter
import matplotlib.cm as cm
import numpy
import psycopg2

def connect(user='sburnett', database='bismark_openwrt_live_v0_1'):
    conn = psycopg2.connect(user=user, database=database)
    cur = conn.cursor()
    cur.execute('SET search_path TO bismark_passive')
    cur.close()
    conn.commit()
    return conn

def plot_daily_traffic(conn, node):
    cur = conn.cursor()
    cur.execute('''SELECT eventstamp,bytes_transferred
                   FROM bytes_per_day_memoized WHERE node_id = %s''', (node,))
    points = []
    for row in cur:
        points.append((row[0].date(), float(row[1])/2**20))
    points.sort()

    fig = plt.figure()
    fig.suptitle('Daily traffic for %s' % node)
    ax = fig.add_subplot(111)
    xs, ys = zip(*points)
    ax.bar(xs, ys, color=cm.Paired(0))
    ax.set_xlabel('Time (days)')
    ax.set_ylabel('Megabytes')
    ax.xaxis.set_major_formatter(DateFormatter('%m-%d'))
    ax.xaxis.set_major_locator(DayLocator())
    fig.autofmt_xdate()
    plt.savefig('%s_daily_traffic.pdf' % node)

def plot_hourly_traffic(conn, node):
    cur = conn.cursor()
    cur.execute('''SELECT eventstamp,bytes_transferred
                   FROM bytes_per_hour_memoized WHERE node_id = %s''', (node,))
    points = []
    for row in cur:
        points.append((row[0], float(row[1])/2**20))
    points.sort()

    fig = plt.figure()
    fig.suptitle('Hourly traffic for %s' % node)
    ax = fig.add_subplot(111)
    xs, ys = zip(*points)
    ax.bar(xs, ys, width=1/24.)
    ax.set_xlabel('Time')
    ax.set_ylabel('Megabytes')
    ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_minor_locator(HourLocator())
    ax.xaxis.set_major_locator(DayLocator())
    fig.autofmt_xdate()
    plt.savefig('%s_hourly_traffic.pdf' % node)

def plot_minutely_traffic(conn, node):
    cur = conn.cursor()
    cur.execute('''SELECT timestamp,bytes_transferred FROM
                   mv_bytes_per_minute WHERE node_id = %s''', (node,))
    points = []
    for row in cur:
        points.append((row[0], float(row[1])/2**20))
    points.sort()

    fig = plt.figure()
    fig.suptitle('Minute-granularity traffic totals for %s' % node)
    ax = fig.add_subplot(111)
    xs, ys = zip(*points)
    ax.bar(xs, ys, width=1/1440.)
    ax.set_xlabel('Time')
    ax.set_ylabel('Megabytes')
    ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_minor_locator(HourLocator())
    ax.xaxis.set_major_locator(DayLocator())
    fig.autofmt_xdate()
    plt.savefig('%s_minutely_traffic.pdf' % node)

def plot_hourly_traffic_ports(conn, node):
    cur = conn.cursor()
    cur.execute('''SELECT timestamp,port,bytes_transferred
                   FROM bytes_per_port_per_hour WHERE node_id = %s
                   ORDER BY bytes_transferred DESC''', (node,))

    dates = {}
    for row in cur:
        dates.setdefault(row[0], [])
        if len(dates[row[0]]) < 3:
            dates[row[0]].append((row[1], float(row[2])/2**20))

    lines = {}
    for timestamp, elements in dates.items():
        for port, bytes in elements:
            lines.setdefault(port, [])
            lines[port].append((timestamp, bytes))
    for port, line in lines.items():
        line.sort()
        current_timestamp = line[0][0]
        current_idx = 0
        while current_timestamp <= line[-1][0]:
            if current_timestamp < line[current_idx][0]:
                line[current_idx:current_idx] = [(current_timestamp, 0)]
            current_idx += 1
            current_timestamp += datetime.timedelta(hours=1)

    fig = plt.figure()
    fig.suptitle('Hourly per-port traffic for %s' % node)
    ax = fig.add_subplot(111)
    for port, line in lines.items():
        xs, ys = zip(*line)
        ax.plot(xs, ys, label='Port %d' % port)
    ax.legend(loc='upper left')
    ax.set_xlabel('Time')
    ax.set_ylabel('Megabytes')
    ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_minor_locator(HourLocator())
    ax.xaxis.set_major_locator(DayLocator())
    fig.autofmt_xdate()
    plt.savefig('%s_port_hourly_traffic.pdf' % node)

def plot_daily_traffic_ports_ssl(conn, node):
    cur = conn.cursor()
    cur.execute('''SELECT eventstamp,port,bytes_transferred
                   FROM bytes_per_port_per_day_memoized
                   WHERE node_id = %s
                   AND (port = 443
                        OR port = 993
                        OR port = 465
                        OR port = 995
                        OR port = 636
                        OR port = 5223)
                   ORDER BY bytes_transferred DESC''', (node,))

    lines = defaultdict(list)
    for row in cur:
        lines[row[1]].append((row[0], float(row[2])/2**20))

    fig = plt.figure()
    fig.suptitle('Daily SSL traffic for %s' % node)
    ax = fig.add_subplot(111)
    for idx, (port, points) in enumerate(lines.items()):
        xs, ys = zip(*points)
        xxs = numpy.array(xs)
        ax.bar(xxs + datetime.timedelta(hours=24./(len(lines) + 1))*idx, ys, label='Port %d' % port, color=cm.Paired(idx*100), width=1./(len(lines)+1))
    ax.legend()
    ax.set_xlabel('Time (days)')
    ax.set_ylabel('Megabytes')
    ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_minor_locator(DayLocator())
    ax.xaxis.set_major_locator(DayLocator(interval=1))
    fig.autofmt_xdate()
    plt.savefig('%s_port_daily_traffic_ssl.pdf' % node)

def plot_daily_traffic_devices_ssl(conn, node):
    cur = conn.cursor()
    cur.execute('''SELECT eventstamp,mac_address,sum(bytes_transferred)
                   FROM bytes_per_device_per_port_per_day_memoized
                   WHERE node_id = %s
                   AND (port = 443
                        OR port = 993
                        OR port = 465
                        OR port = 995
                        OR port = 636
                        OR port = 5223)
                   GROUP BY eventstamp,mac_address''', (node,))

    lines = defaultdict(list)
    for row in cur:
        lines[row[1]].append((row[0], float(row[2])/2**20))

    print len(lines)

    fig = plt.figure()
    fig.suptitle('Daily per-device SSL traffic for router %s' % node)
    ax = fig.add_subplot(111)
    for idx, (mac_address, points) in enumerate(lines.items()):
        xs, ys = zip(*points)
        xxs = numpy.array(xs)
        ax.bar(xxs + datetime.timedelta(minutes=1440./(len(lines)+1))*idx, ys, label=mac_address, color=cm.Paired(idx*20), width=1.0/(len(lines)+1))
    ax.legend(loc='upper right', prop={'size':10})
    ax.set_xlabel('Time (days)')
    ax.set_ylabel('Megabytes')
    ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_minor_locator(DayLocator())
    ax.xaxis.set_major_locator(DayLocator(interval=1))
    fig.autofmt_xdate()
    plt.savefig('%s_port_daily_device_traffic_ssl.pdf' % node)

def plot_daily_traffic_devices(conn, node):
    cur = conn.cursor()
    cur.execute('''SELECT eventstamp,mac_address,sum(bytes_transferred)
                   FROM bytes_per_device_per_day_memoized
                   WHERE node_id = %s
                   GROUP BY eventstamp,mac_address''', (node,))

    lines = defaultdict(list)
    for row in cur:
        lines[row[1]].append((row[0], float(row[2])/2**20))

    print len(lines)

    fig = plt.figure()
    fig.suptitle('Daily per-device traffic for router %s' % node)
    ax = fig.add_subplot(111)
    for idx, (mac_address, points) in enumerate(lines.items()):
        xs, ys = zip(*points)
        xxs = numpy.array(xs)
        ax.bar(xxs + datetime.timedelta(minutes=1440./(len(lines)+1))*idx, ys, label=mac_address, color=cm.Paired(idx*20), width=1.0/(len(lines)+1))
    ax.legend(loc='upper right', prop={'size':8})
    ax.set_xlabel('Time (days)')
    ax.set_ylabel('Megabytes')
    ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_minor_locator(DayLocator())
    ax.xaxis.set_major_locator(DayLocator(interval=1))
    fig.autofmt_xdate()
    plt.savefig('%s_daily_device_traffic.pdf' % node)

#def plot_hourly_traffic_devices(conn, node):
#    cur = conn.cursor()
#    cur.execute('''SELECT timestamp,mac_address,bytes_transferred
#                   FROM bytes_per_device_per_hour WHERE node_id = %s''',
#                (node,))
#
#    dates = {}
#    for row in cur:
#        dates.setdefault(row[0], [])
#        if len(dates[row[0]]) < 3:
#            dates[row[0]].append((row[1], float(row[2])/2**20))
#
#    lines = {}
#    for timestamp, elements in dates.items():
#        for mac_address, bytes in elements:
#            lines.setdefault(mac_address, [])
#            lines[mac_address].append((timestamp, bytes))
#    for mac_address, line in lines.items():
#        line.sort()
#        current_timestamp = line[0][0]
#        current_idx = 0
#        while current_timestamp <= line[-1][0]:
#            if current_timestamp < line[current_idx][0]:
#                line[current_idx:current_idx] = [(current_timestamp, 0)]
#            current_idx += 1
#            current_timestamp += datetime.timedelta(hours=1)
#
#    fig = plt.figure()
#    fig.suptitle('Hourly per-device traffic for %s' % node)
#    ax = fig.add_subplot(111)
#    for mac_address, line in lines.items():
#        xs, ys = zip(*line)
#        ax.plot(xs, ys, label=mac_address)
#    ax.legend(loc='upper left')
#    ax.set_xlabel('Time')
#    ax.set_ylabel('Megabytes')
#    ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
#    ax.xaxis.set_minor_locator(HourLocator())
#    ax.xaxis.set_major_locator(DayLocator())
#    fig.autofmt_xdate()
#    plt.savefig('%s_device_hourly_traffic.pdf' % node)

def plot_packet_size_cdf_ssl(conn, node):
    cur = conn.cursor()
    cur.execute('''SELECT port,packet_size,count
                   FROM packet_sizes_per_port
                   WHERE node_id = %s
                   AND (port = 443
                        OR port = 993
                        OR port = 465
                        OR port = 995
                        OR port = 636
                        OR port = 5223)''',
                (node,))
    port_data = defaultdict(dict)
    for row in cur:
        port_data[row[0]][row[1]] = row[2]
    xs = range(1500)

    fig = plt.figure()
    fig.suptitle('CDF of SSL packet sizes for router %s' % node)
    ax = fig.add_subplot(111)
    for seq, (port, samples) in enumerate(port_data.items()):
        ys = []
        total = 0
        for idx in range(1500):
            if idx in samples:
                total += samples[idx]
            ys.append(total)
        ys = map(lambda y: y/float(total), ys)
        plt.plot(xs, ys, label='Port %d' % port, color=cm.Paired(seq*100))
    ax.legend(loc='lower right', prop={'size':10})
    ax.set_xlabel('Packet size')
    ax.set_ylabel('CDF')
    plt.savefig('%s_packet_size_cdf.pdf' % node)

def plot_daily_traffic_ports_dhcp(conn, node):
    cur = conn.cursor()
    cur.execute('''SELECT eventstamp,port,bytes_transferred
                   FROM bytes_per_port_per_day_memoized
                   WHERE node_id = %s
                   AND (port = 67 OR port = 68)
                   ORDER BY bytes_transferred DESC''', (node,))

    lines = defaultdict(list)
    for row in cur:
        lines[row[1]].append((row[0], float(row[2])/2**20))

    fig = plt.figure()
    fig.suptitle('Daily DHCP traffic for %s' % node)
    ax = fig.add_subplot(111)
    for idx, (port, points) in enumerate(lines.items()):
        xs, ys = zip(*points)
        xxs = numpy.array(xs)
        ax.bar(xxs + datetime.timedelta(hours=24./(len(lines) + 1))*idx, ys, label='Port %d' % port, color=cm.Paired(idx*100), width=1./(len(lines)+1))
    ax.legend()
    ax.set_xlabel('Time (days)')
    ax.set_ylabel('Megabytes')
    ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_minor_locator(DayLocator())
    ax.xaxis.set_major_locator(DayLocator(interval=1))
    fig.autofmt_xdate()
    plt.savefig('%s_port_daily_traffic_dhcp.pdf' % node)

def plot_updates(conn, max_age=None, filename='plots/updates.pdf'):
    if max_age is None:
        oldest = datetime.datetime.min
    else:
        oldest = datetime.datetime.utcnow() - max_age
    
    availabilities = defaultdict(list)
    outages = defaultdict(list)

    lower = defaultdict(lambda: None)
    upper = defaultdict(lambda: None)
    current_node_id = None
    timeout = datetime.timedelta(seconds=60)

    cur = conn.cursor()
    cur.execute('SELECT DISTINCT node_id FROM update_statistics')
    node_ids = map(lambda row: row[0], cur)

    cur = conn.cursor()
    cur.execute('''SELECT node_id, eventstamp
                   FROM update_statistics
                   WHERE eventstamp > ?
                   ORDER BY node_id, eventstamp''',
                   (oldest,))
    for row in cur:
        node_id = row[0]
        eventstamp = datetime.datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')
        if node_id != current_node_id:
            if current_node_id is not None:
                availabilities[current_node_id].append((lower, upper))
            current_node_id = node_id
            lower = eventstamp
            upper = eventstamp
        if eventstamp - upper >= timeout:
            print 'Outage:', node_id, eventstamp - upper
            availabilities[node_id].append((lower, upper))
            outages[node_id].append(upper)
            lower = eventstamp
        upper = eventstamp
    if current_node_id is not None:
        availabilities[node_id].append((lower, upper))

    indices = dict(map(lambda nid: (nid, node_ids.index(nid)), node_ids))

    apoints = []
    for node_id, intervals in availabilities.items():
        for lower, upper in intervals:
            apoints.append((indices[node_id], lower, upper))
    opoints = []
    for node_id, outage_times in outages.items():
        for beginning in outage_times:
            opoints.append((beginning, indices[node_id]))

    fig = plt.figure()
    ax = fig.add_subplot(111)
    plt.yticks(range(len(node_ids)), node_ids)
    ax.hlines(*zip(*apoints))
    ax.scatter(*zip(*opoints), c='r', marker='|')
    ax.set_xlabel('Date')
    ax.set_ylabel('Bismark Node ID')
    ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_minor_locator(DayLocator())
    if max_age is None:
        ax.xaxis.set_major_locator(DayLocator(interval=7))
    else:
        ax.xaxis.set_major_locator(DayLocator(interval=1))
    fig.autofmt_xdate()
    plt.ylim(-1, len(node_ids))
    plt.axvline(datetime.datetime.utcnow())
    if max_age is None:
        plt.title('Passive data availability for all time')
    else:
        plt.title('Passive data availability for the past %s' % max_age)
    plt.tight_layout()
    plt.savefig(filename)
