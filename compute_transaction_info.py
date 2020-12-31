# -*- coding: utf8 -*-
# __author__ = 'Fan()'
# Date: 2020/8/25

import os
import time
import random
import logging
import datetime

from clickhouse_driver import Client
from dateutil.parser import parse as date_parse
from logging.handlers import RotatingFileHandler

# 这个脚本只凑合实现了功能, 代码很丑陋

CH_SERVERS = (
    ('172.16.24.xxx1', 9000),
    ('172.16.24.xxx2', 9000),
    ('172.16.24.xxx3', 9000),
    ('172.16.24.xxx4', 9000),
    ('172.16.24.xxx5', 9000),
    ('172.16.24.xxx6', 9000)
)

CH_USER = ''
CH_PASS = ''

SETTINGS = {
    'distributed_aggregation_memory_efficient': 1,
    'group_by_two_level_threshold': 1,
    'group_by_two_level_threshold_bytes': 1
}

base_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(base_dir, 'compute_transaction_info.log')
logging.getLogger('').setLevel(logging.NOTSET)
rotatingHandler = RotatingFileHandler(log_file,
                              maxBytes=1024 * 1024 * 100,
                              backupCount=2)
rotatingHandler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)4d] %(levelname)s %(message)s')
rotatingHandler.setFormatter(formatter)
logging.getLogger('').addHandler(rotatingHandler)
logger = logging.getLogger(__name__)




def get_largest_transaction(start_datetime, end_datetime, prefix):
    start = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
    end = end_datetime.strftime('%Y-%m-%d %H:%M:%S')
    sql = f'''
        SELECT 
            toDateTime('{end}'),
            '{str(interval)}',
            gtid, 
            max(execute_time) - min(execute_time) AS transaction_spend_time, 
            (max(toUInt32(binlog_pos)) - min(toUInt32(binlog_pos))) + argMax(toUInt32(single_statement_size), toUInt32(binlog_pos)) AS transaction_size, 
            sum(single_statement_affected_rows) AS transaction_affected_rows
        FROM mysql_monitor.{prefix}_binlog
        WHERE (execute_time >= '{start}') AND (execute_time < '{end}')
        GROUP BY gtid
        ORDER BY transaction_size DESC
        LIMIT 1
    '''
    ind = random.randint(0, len(CH_SERVERS) - 1)
    host, port = CH_SERVERS[ind][0], CH_SERVERS[ind][1]
    client = Client(host=host, port=port, user=CH_USER, password=CH_PASS, settings=SETTINGS)
    res = client.execute(sql)
    client.disconnect()
    return res


def get_most_affected_rows_transaction(start_datetime, end_datetime, prefix):
    start = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
    end = end_datetime.strftime('%Y-%m-%d %H:%M:%S')
    sql = f'''
        SELECT 
            toDateTime('{end}'),
            '{str(interval)}',
            gtid, 
            max(execute_time) - min(execute_time) AS transaction_spend_time, 
            (max(toUInt32(binlog_pos)) - min(toUInt32(binlog_pos))) + argMax(toUInt32(single_statement_size), toUInt32(binlog_pos)) AS transaction_size, 
            sum(single_statement_affected_rows) AS transaction_affected_rows
        FROM mysql_monitor.{prefix}_binlog
        WHERE (execute_time >= '{start}') AND (execute_time < '{end}')
        GROUP BY gtid
        ORDER BY transaction_affected_rows DESC
        LIMIT 1
    '''
    ind = random.randint(0, len(CH_SERVERS) - 1)
    host, port = CH_SERVERS[ind][0], CH_SERVERS[ind][1]
    client = Client(host=host, port=port, user=CH_USER, password=CH_PASS, settings=SETTINGS)
    res = client.execute(sql)
    client.disconnect()
    return res


def get_most_time_consuming_transaction(start_datetime, end_datetime, prefix):
    start = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
    end = end_datetime.strftime('%Y-%m-%d %H:%M:%S')
    sql = f'''
        SELECT 
            toDateTime('{end}'),
            '{str(interval)}',
            gtid, 
            max(execute_time) - min(execute_time) AS transaction_spend_time, 
            (max(toUInt32(binlog_pos)) - min(toUInt32(binlog_pos))) + argMax(toUInt32(single_statement_size), toUInt32(binlog_pos)) AS transaction_size, 
            sum(single_statement_affected_rows) AS single_statement_affected_rows
        FROM mysql_monitor.{prefix}_binlog
        WHERE (execute_time >= '{start}') AND (execute_time < '{end}')
        GROUP BY gtid
        ORDER BY transaction_spend_time DESC
        LIMIT 1
    '''
    ind = random.randint(0, len(CH_SERVERS) - 1)
    host, port = CH_SERVERS[ind][0], CH_SERVERS[ind][1]
    client = Client(host=host, port=port, user=CH_USER, password=CH_PASS, settings=SETTINGS)
    res = client.execute(sql)
    client.disconnect()
    return res


def insert_to_ch(schema, table, res, start_time=None, end_time=None):
    if not res:
        logger.info(f"None Result, pass: {schema}.{table} {start_time}, {end_time}")
        return
    ind = random.randint(0, len(CH_SERVERS) - 1)
    host, port = CH_SERVERS[ind][0], CH_SERVERS[ind][1]

    if end_time:
        distributed_table = table.rstrip('_local')
        client = Client(host=host, port=port, user=CH_USER, password=CH_PASS)
        s = client.execute(f"select count(*) cnt from "
                           f"{schema}.{distributed_table} where end_time=toDateTime('{end_time}')")
        s = s[0][0]
        client.disconnect()
    else:
        s = 0

    if not s:
        sql = f'INSERT INTO {schema}.{table}(' \
              f'end_time, invertal, gtid, transaction_spend_time, transaction_size, single_statement_affected_rows) VALUES'
        client = Client(host=host, port=port, user=CH_USER, password=CH_PASS)
        client.execute(sql, res)
        logger.info(f"Success: {schema}.{table} {start_time}, {end_time}")
        client.disconnect()
    else:
        logger.info(f'Exists, pass: {schema}.{table} {start_time}, {end_time}')


if __name__ == '__main__':
    interval = 300
    # 目前订阅了下面三个库的binlog, 所以写死为列表了
    compute_list = ['cluster1', 'cluster2', 'cluster3']
    start_datetime = date_parse('2020-05-30 00:00:00')
    end_datetime = start_datetime + datetime.timedelta(minutes=interval)
    while True:
        if end_datetime >= datetime.datetime.now():
            time.sleep(300)
            continue
        s = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
        e = end_datetime.strftime('%Y-%m-%d %H:%M:%S')
        try:
            for prefix in compute_list:
                insert_to_ch('mysql_monitor', f'{prefix}_largest_transaction_local',
                             get_largest_transaction(start_datetime, end_datetime, prefix), s, e)
                insert_to_ch('mysql_monitor', f'{prefix}_most_time_consuming_transaction_local',
                             get_most_affected_rows_transaction(start_datetime, end_datetime, prefix), s, e)
                insert_to_ch('mysql_monitor', f'{prefix}_most_affected_rows_transaction_local',
                             get_most_time_consuming_transaction(start_datetime, end_datetime, prefix), s, e)
        except Exception as e:
            logger.error(e)
            continue

        start_datetime += datetime.timedelta(seconds=interval)
        end_datetime = start_datetime + datetime.timedelta(seconds=interval)
        time.sleep(0.5)

