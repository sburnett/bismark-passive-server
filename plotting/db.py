import psycopg2
import sqlite3

def postgres(user='sburnett', database='bismark_openwrt_live_v0_1'):
    conn = psycopg2.connect(user=user, database=database)
    cur = conn.cursor()
    cur.execute('SET search_path TO bismark_passive')
    cur.close()
    conn.commit()
    return conn

def sqlite(filename):
    conn = sqlite3.connect(filename)
    conn.row_factory = sqlite3.Row
    return conn
