#!/usr/bin/env python

from collections import defaultdict
import datetime
import matplotlib.pyplot as plt
from matplotlib.dates import AutoDateFormatter, AutoDateLocator
import os
import sqlite3

UPDATE_TIMEOUT = datetime.timedelta(seconds=60)

def plot_updates(conn, max_age=None, filename='plots/updates.pdf'):
    if max_age is None:
        oldest = datetime.datetime.min
    else:
        oldest = datetime.datetime.utcnow() - max_age
    
    cur = conn.cursor()
    cur.execute('SELECT DISTINCT node_id FROM update_statistics')
    node_ids = map(lambda row: row[0], cur)

    availabilities = defaultdict(list)
    outages = defaultdict(list)
    current_node_id = None
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
        if eventstamp - upper >= UPDATE_TIMEOUT:
            print 'Outage:', node_id, eventstamp - upper
            availabilities[node_id].append((lower, upper))
            outages[node_id].append(upper)
            lower = eventstamp
        upper = eventstamp
    if current_node_id is not None:
        availabilities[node_id].append((lower, upper))

    indices = {}
    for node_id in node_ids:
        indices[node_id] = node_ids.index(node_id)

    availability_intervals = []
    for node_id, intervals in availabilities.items():
        for lower, upper in intervals:
            availability_intervals.append((indices[node_id], lower, upper))
    outage_points = []
    for node_id, outage_times in outages.items():
        for beginning in outage_times:
            outage_points.append((beginning,
                                  indices[node_id] - 0.2,
                                  indices[node_id] + 0.2))

    fig = plt.figure()
    ax = fig.add_subplot(111)
    plt.yticks(range(len(node_ids)), node_ids)
    ax.hlines(*zip(*availability_intervals), lw=2)
    if len(outage_points) > 0:
        ax.vlines(*zip(*outage_points), lw=2, color='r')
    ax.set_xlabel('Date')
    ax.set_ylabel('Bismark Node ID')
    loc = AutoDateLocator()
    ax.xaxis.set_major_locator(loc)
    ax.xaxis.set_major_formatter(AutoDateFormatter(loc))
    fig.autofmt_xdate()
    plt.ylim(-1, len(node_ids))
    plt.axvline(datetime.datetime.utcnow())
    if max_age is None:
        plt.title('Passive data availability for all time')
    else:
        plt.title('Passive data availability for the past %s' % max_age)
    plt.tight_layout()
    plt.savefig(filename)

def main():
    os.system('nice -n 5'
              ' python /home/sburnett/git/bismark-passive-server/harnesses.py'
              ' updates'
              ' /data/users/bismark/data/passive'
              ' /data/users/bismark/data/passive/index.sqlite'
              ' /data/users/sburnett/passive_pickles'
              ' --db_filename=/data/users/sburnett/passive-databases/updates.sqlite')
    conn = sqlite3.connect('/data/users/sburnett/passive-databases/updates.sqlite')
    print 'Plotting daily status'
    plot_updates(
            conn,
            datetime.timedelta(days=1),
            '/home/sburnett/public_html/bismark-passive/status_daily.png')
    print 'Plotting weekly status'
    plot_updates(
            conn,
            datetime.timedelta(days=7),
            '/home/sburnett/public_html/bismark-passive/status_weekly.png')
    print 'Plotting overall status'
    plot_updates(conn,
                 None,
                 '/home/sburnett/public_html/bismark-passive/status.png')

if __name__ == '__main__':
    main()
