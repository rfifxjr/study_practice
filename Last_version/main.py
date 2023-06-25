import os
import sqlite3
import config
from datetime import datetime
import json


def create_logs_db():
    if os.path.exists(config.db_name):
        os.remove(config.db_name)
    conn = sqlite3.connect(config.db_name)
    c = conn.cursor()
    c.execute('''CREATE TABLE logs
                 (ip text, date text, request text, status_code integer, size integer)''')

    def parse_line(line):
        parts = line.split()
        ip = parts[0]
        date_str = f"{parts[3][1:]} {parts[4][:-1]}"
        date = datetime.strptime(date_str, '%d/%b/%Y:%H:%M:%S %z')
        date_formatted = date.strftime('%d/%b/%Y:%H:%M:%S %z')
        request = " ".join(parts[5:8])
        status_code = int(parts[8])
        size = int(parts[9])
        return (ip, date_formatted, request, status_code, size)

    with open(config.file_to_pars) as f:
        log_lines = f.readlines()
        log_data = map(parse_line, log_lines)
        c.executemany("INSERT INTO logs VALUES (?, ?, ?, ?, ?)", log_data)

    conn.commit()
    conn.close()


def create_logs_db_json():
    if os.path.exists(config.db_name_json):
        os.remove(config.db_name_json)
    conn = sqlite3.connect(config.db_name_json)
    c = conn.cursor()
    c.execute('''CREATE TABLE logs_json
               (ip text, date text, request text, status_code integer, size integer)''')

    def parse_log(log):
        ip = log['IP']
        date_str = f"{log['Date']} {log['Time']}"
        date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        date_formatted = date.strftime('%d/%b/%Y:%H:%M:%S %z')
        request = log['First_Line']
        status_code = log['Status']
        size = log['Size']
        return (ip, date_formatted, request, status_code, size)

    with open(config.file_to_pars_json) as f:
        logs = json.load(f)
        log_data = map(parse_log, logs)
        c.executemany("INSERT INTO logs_json VALUES (?, ?, ?, ?, ?)", log_data)

    conn.commit()
    conn.close()


def select_by_ip(ip):
    conn = sqlite3.connect(config.db_name)
    c = conn.cursor()
    c.execute("SELECT * FROM logs WHERE ip=?", (ip,))
    rows = c.fetchall()
    conn.close()
    return rows


def select_by_date(date):
    conn = sqlite3.connect(config.db_name)
    c = conn.cursor()
    date_formatted = date.strftime('%d/%b/%Y:%H:%M:%S %z')
    c.execute("SELECT * FROM logs WHERE date=?", (date_formatted,))
    rows = c.fetchall()
    conn.close()
    return rows


def select_by_date_range(start_date, end_date):
    conn = sqlite3.connect(config.db_name)
    c = conn.cursor()
    start_date_formatted = start_date.strftime('%d/%b/%Y:%H:%M:%S %z')
    end_date_formatted = end_date.strftime('%d/%b/%Y:%H:%M:%S %z')
    c.execute("SELECT * FROM logs WHERE date BETWEEN ? AND ?", (start_date_formatted, end_date_formatted))
    rows = c.fetchall()
    conn.close()
    return rows


def get_logs_by_ip_json(ip_address):
    with open(config.file_to_pars_json) as f:
        logs = json.load(f)
    return [log for log in logs if log['IP'] == ip_address]


def get_logs_by_date_json(date):
    with open(config.file_to_pars_json) as f:
        logs = json.load(f)
    return [log for log in logs if log['Date'] == date]


def get_logs_by_date_range_json(start_date, end_date):
    with open(config.file_to_pars_json) as f:
        logs = json.load(f)
    return [log for log in logs if start_date <= log['Date'] <= end_date]


create_logs_db()
create_logs_db_json()

print(select_by_ip('192.168.2.30'))
print(select_by_date(datetime.strptime('01/Jun/2004:12:27:35 +0300', '%d/%b/%Y:%H:%M:%S %z')))
print(select_by_date_range(datetime.strptime('01/Jun/2023:12:27:35 +0300', '%d/%b/%Y:%H:%M:%S %z'), datetime.strptime('07/Jun/2023:18:29:58 +0300', '%d/%b/%Y:%H:%M:%S %z')))

print(get_logs_by_ip_json('192.168.2.30'))
print(get_logs_by_date_json('2023-06-02'))
print(get_logs_by_date_range_json('1999-06-01', '2023-06-03'))