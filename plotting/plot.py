#!/usr/bin/env python

import datetime
import matplotlib
matplotlib.use('Cairo')
import matplotlib.pyplot as plt
from matplotlib.dates import DayLocator, HourLocator, DateFormatter
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
    cur.execute('''SELECT timestamp,bytes_transferred
                   FROM bytes_per_day WHERE node_id = %s''', (node,))
    points = []
    for row in cur:
        points.append((row[0].date(), float(row[1])/2**20))
    points.sort()

    fig = plt.figure()
    fig.suptitle('Daily traffic for %s' % node)
    ax = fig.add_subplot(111)
    xs, ys = zip(*points)
    ax.bar(xs, ys)
    ax.set_xlabel('Time')
    ax.set_ylabel('Megabytes')
    ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_major_locator(DayLocator())
    fig.autofmt_xdate()
    plt.savefig('%s_daily_traffic.pdf' % node)

def plot_hourly_traffic(conn, node):
    cur = conn.cursor()
    cur.execute('''SELECT timestamp,bytes_transferred
                   FROM bytes_per_hour WHERE node_id = %s''', (node,))
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

def plot_hourly_traffic_ports_no_http(conn, node):
    cur = conn.cursor()
    cur.execute('''SELECT timestamp,port,bytes_transferred
                   FROM bytes_per_port_per_hour
                   WHERE node_id = %s AND port != 80
                   ORDER BY bytes_transferred DESC''', (node,))

    dates = {}
    for row in cur:
        dates.setdefault(row[0], [])
        if len(dates[row[0]]) < 3:
            dates[row[0]].append((row[1], float(row[2])/2**10))

    lines = {}
    for timestamp, elements in dates.items():
        for port, bytes in elements:
            lines.setdefault(port, [])
            lines[port].append((timestamp, bytes))
    for port, line in lines.items():
        line.sort()
        current_timestamp = line[0][0]
        current_idx = 0
        line.insert(0, (line[0][0] - datetime.timedelta(hours=1), 0))
        while current_timestamp <= line[-1][0]:
            if current_timestamp < line[current_idx][0]:
                line.insert(current_idx, (current_timestamp, 0))
            current_idx += 1
            current_timestamp += datetime.timedelta(hours=1)
        line.append((current_timestamp, 0))

    fig = plt.figure()
    fig.suptitle('Hourly per-port traffic for %s' % node)
    ax = fig.add_subplot(111)
    for port, line in lines.items():
        xs, ys = zip(*line)
        ax.plot(xs, ys, label='Port %d' % port)
    ax.legend(loc='upper left')
    ax.set_xlabel('Time')
    ax.set_ylabel('Kilobytes')
    ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_minor_locator(HourLocator())
    ax.xaxis.set_major_locator(DayLocator())
    fig.autofmt_xdate()
    plt.savefig('%s_port_hourly_traffic_no_http.pdf' % node)
