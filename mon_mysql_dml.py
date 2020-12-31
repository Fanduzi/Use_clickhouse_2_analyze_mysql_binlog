#cat /home/devops/mon_mysql_dml.py
# -*- coding: utf8 -*-
# __author__ = 'Fan()'
# Date: 2020-05-26

"""
Usage:
        mon_mysql_dml.py (--bootstrap-servers=<host:port,host2:port2..>)
                         (--topic=<topic_name>)
                         (--ch_db=<db>)
                         (--ch_table=<table>)
                         [--sync=<event_count>]
                         [--k_user=<user>]
                         [--from-beginning | --from-end | --from-stored | --from-invalid]
                         [--partition=<partition_number>]
                         [--consumer-group=<g>]
                         [--verbose=<0>]
        mon_mysql_dml.py -h | --help
        mon_mysql_dml.py --version

Options:
        -h --help                                           打印帮助信息.
        -V, --version                                       版本信息.
        --bootstrap_servers=<host:port,host2:port2..>       kafka servers
        -c, --consumer-group=<g>                            consumer group name, default is topic name.
        -b, --from-beginning                                从头开始消费 [default: False]
        -e, --from-end                                      从最后开始消费 [default: True]
        -u, --k_user=<user>                                 kafka用户, 可选项
        -t, --topic=<topic_name>                            topic名称
        -p, --partition=<partition_number>                  topic分区号 [default: 0]
        -s, --sync=<event_count>                            每消费sync条binlog event写入clickhouse [default: 5000]
        --ch_db=<db>                                        clickhouse的数据库名
        --ch_table=<table>                                  clickhouse表名
        --verbose=<0>                                       输出详细信息0,1,2 默认0不输出 [default: 0]
"""
import random
import getpass

from docopt import docopt
from retrying import retry
from datetime import datetime
from clickhouse_driver import Client
from canal.protocol import CanalProtocol_pb2
from canal.protocol import EntryProtocol_pb2
from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_END, OFFSET_BEGINNING, OFFSET_STORED, OFFSET_INVALID


CH_SERVERS = (
    ('172.16.24.xxx1', 9000),
    ('172.16.24.xxx2', 9000),
    ('172.16.24.xxx3', 9000),
    ('172.16.24.xxx4', 9000),
    ('172.16.24.xxx5', 9000),
    ('172.16.24.xxx6', 9000)
)


class DocOptArgs:
    def __init__(self, args):
        self.topic = args['--topic']
        self.k_user = args['--k_user']
        self.verbose = int(args['--verbose'])
        self.partition = int(args['--partition'])
        self.bootstrap_servers = args['--bootstrap-servers']
        self.from_end = args['--from-end']
        self.from_beginning = args['--from-beginning']
        self.from_stored = args['--from-stored']
        self.from_invalid = args['--from-invalid']
        self.ch_db = args['--ch_db']
        self.ch_table = args['--ch_table']
        self.sync = int(args['--sync'])
        self.consumer_group = args['--consumer-group']

        if not self.consumer_group:
            self.consumer_group = self.topic

        if not self.k_user:
            self.k_password = None
        elif self.k_user == 'admin':
            self.k_password = 'your_kafka_password'
        else:
            self.k_password = getpass.getpass("please enter kafka password: ")


class MyConsumer(DocOptArgs):
    def __init__(self, docopt_args):
        self.args = docopt_args
        DocOptArgs.__init__(self, self.args)

        if self.verbose >= 1:
            print(self.args)

    def _on_send_response(self, err, partations):
        pt = partations[0]
        if isinstance(err, KafkaError):
            print('Topic {} 偏移量 {} 提交异常. {}'.format(pt.topic, pt.offset, err))
            raise Exception(err)

    def messages(self):
        config = {'bootstrap.servers': self.bootstrap_servers,
                  "group.id": self.consumer_group,
                  'enable.auto.commit': True,
                  "fetch.wait.max.ms": 3000,
                  "max.poll.interval.ms": 60000,
                  'session.timeout.ms': 60000,
                  "on_commit": self._on_send_response,
                  "default.topic.config": {"auto.offset.reset": "latest"}}
        if self.k_user and self.k_password:
            config['security.protocol'] = 'SASL_PLAINTEXT'
            config['sasl.mechanism'] = 'SCRAM-SHA-256'
            config['sasl.username'] = self.k_user
            config['sasl.password'] = self.k_password

        consumer = Consumer(config)
        if self.from_end:
            offset = OFFSET_END
        elif self.from_stored:
            offset = OFFSET_STORED
        elif self.from_beginning:
            offset = OFFSET_BEGINNING
        elif self.from_invalid:
            offset = OFFSET_INVALID
        # offset = OFFSET_END if self.from_end else OFFSET_BEGINNING
        pt = TopicPartition(self.topic, 0, offset)
        consumer.assign([pt])
        # consumer.seek(pt)

        try:
            while True:
                ret = consumer.consume(num_messages=100, timeout=0.1)
                if ret is None:
                    print("No message Continue!")
                    continue
                for msg in ret:
                    if msg.error() is None:
                        # protobuf binary
                        yield msg.value()
                    elif msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                    else:
                        raise Exception(msg.error())
        except Exception as e:
            print(e)
            consumer.close()
        except KeyboardInterrupt:
            consumer.close()


class Decoder:

    @staticmethod
    def create_canal_message(kafka_message):
        data = kafka_message
        packet = CanalProtocol_pb2.Packet()
        packet.MergeFromString(data)

        message = dict(id=0, entries=[])
        # 因为从kafka获取的canal写入的消息, 所以这个条件应该永远成立
        # if packet.type == CanalProtocol_pb2.PacketType.MESSAGES:
        messages = CanalProtocol_pb2.Messages()
        messages.MergeFromString(packet.body)

        for item in messages.messages:
            entry = EntryProtocol_pb2.Entry()
            entry.MergeFromString(item)
            message['entries'].append(entry)

        return message


# def get_event_type(event_type_number):
#     """
#     EntryProtocol_pb2.EventType.items()
#     [('EVENTTYPECOMPATIBLEPROTO2', 0),
#      ('INSERT', 1),
#      ('UPDATE', 2),
#      ('DELETE', 3),
#      ('CREATE', 4),
#      ('ALTER', 5),
#      ('ERASE', 6),
#      ('QUERY', 7),
#      ('TRUNCATE', 8),
#      ('RENAME', 9),
#      ('CINDEX', 10),
#      ('DINDEX', 11),
#      ('GTID', 12),
#      ('XACOMMIT', 13),
#      ('XAROLLBACK', 14),
#      ('MHEARTBEAT', 15)]
#     :param event_type_number:
#     :return:
#     """
#     for event in EntryProtocol_pb2.EventType.items():
#         if event_type_number == event[1]:
#             return event[0]
# 用EntryProtocol_pb2.EventType.Name就可以了, 不用自己写

@retry(stop_max_attempt_number=5, wait_random_min=1000, wait_random_max=2000)
def insert_to_ch(schema, table, entry_list):
    sql = f'INSERT INTO {schema}.{table}('
    columns, values = [], []
    for key in entry_list[0]:
        columns.append(key)
    sql += ','.join(columns) + ')' + ' VALUES'
    # print(sql)
    ind = random.randint(0, len(CH_SERVERS)-1)
    host, port = CH_SERVERS[ind][0], CH_SERVERS[ind][1]
    print(host)
    client = Client(host=host, port=port, user='your_clickhouse_user', password='your_clickhouse_password')
    client.execute(sql, entry_list)
    print(str(len(entry_list)) + ' insert')


if __name__ == '__main__':
    version = 'mon_mysql_dml 0.1.2'
    arguments = docopt(__doc__, version=version)

    consumer = MyConsumer(arguments)
    entry_list = []

    for message in consumer.messages():
        canal_message = Decoder.create_canal_message(message)
        entries = canal_message['entries']
        # begin;
        # insert values(1), (2), (3)
        # update
        # commit;
        # insert update为一个entries种的两个entry. 所以需要通过每个statement的gtid判断是否为同一个事物

        # -----
        # c30c6a02-4e32-11ea-84ec-fa163edcd14e:1-8890601
        # c30c6a02-4e32-11ea-84ec-fa163edcd14e:1-8890602
        # c30c6a02-4e32-11ea-84ec-fa163edcd14e:1-8890603
        # c30c6a02-4e32-11ea-84ec-fa163edcd14e:1-8890604
        # c30c6a02-4e32-11ea-84ec-fa163edcd14e:1-8890605
        # c30c6a02-4e32-11ea-84ec-fa163edcd14e:1-8890606
        # c30c6a02-4e32-11ea-84ec-fa163edcd14e:1-8890607
        # c30c6a02-4e32-11ea-84ec-fa163edcd14e:1-8890607
        # c30c6a02-4e32-11ea-84ec-fa163edcd14e:1-8890607
        # -----
        # 一个 entries 含多个事务

        for entry in entries:
            entry_type = entry.entryType
            # canal有时获取不到COMMIT, 如果commit记录事务中最后一个语句执行时间>=6s, 则可以获取,否则不行, 我认为是canal bug
            # 获取BEGIN意义不大, 往往它就是事务中第一个语句的执行时间
            # 如果事务语句执行完毕, 但是迟迟不提交, 那么不记录commit会导致事务时间计算严重失真.
            # 但如果记录COMMIT, CH表会增大最大约2倍(如果大多数事务都>6秒不提交),
            # 但如果大多数事务都是在语句执行完毕后就提交(<6秒), 那么canal有不会获取到commit, 也就不会记录到CH, CH表就不会增长很大.
            # 但是BEGIN每次都能获取到, 这肯定会导致CH表大大增大, 所以总和考虑过滤BEGIN
            if entry_type in [EntryProtocol_pb2.EntryType.TRANSACTIONBEGIN, ]:
                continue
            row_change = EntryProtocol_pb2.RowChange()
            row_change.MergeFromString(entry.storeValue)
            # event_type = row_change.eventType
            header = entry.header
            database = header.schemaName
            table = header.tableName
            binlog_file = header.logfileName
            binlog_pos = header.logfileOffset
            characterset = header.serverenCode
            execute_time = header.executeTime
            # 注意, 计算事务产生的binlog大小不能简单地sum(event_length),
            # 因为beginlog中还包含Table_map event 它也占用磁盘空间的, 但是canal不会解析它
            # 如果开启binlog_rows_query_log_events 还会产生Rows_query event event_type为QUERY, canal会解析它.

            # SELECT
            #     execute_time,
            #     schema,
            #     table,
            #     event_type,
            #     is_ddl,
            #     binlog_file,
            #     binlog_pos,
            #     single_statement_size
            # FROM rttax_binlog
            # WHERE gtid = 'xxx'
            # ORDER BY toUInt32(binlog_pos) ASC
            #
            # ┌────────execute_time─┬─schema─┬─table──────────┬─event_type─┬─is_ddl─┬─binlog_file──────┬─binlog_pos─┬─single_statement_size─┐
            # │ 2020-08-30 15:32:26 │        │ f_file_info    │ QUERY      │      0 │ mysql-bin.004775 │ 451044141  │ 593                   │
            # │ 2020-08-30 15:32:26 │ yos    │ f_file_info    │ INSERT     │      0 │ mysql-bin.004775 │ 451044825  │ 284                   │
            # │ 2020-08-30 15:32:26 │        │ f_file_storage │ QUERY      │      0 │ mysql-bin.004775 │ 451045109  │ 239                   │
            # │ 2020-08-30 15:32:26 │ yos    │ f_file_storage │ INSERT     │      0 │ mysql-bin.004775 │ 451045419  │ 116                   │
            # │ 2020-08-30 15:32:26 │        │ f_file_storage │ QUERY      │      0 │ mysql-bin.004775 │ 451045535  │ 239                   │
            # │ 2020-08-30 15:32:26 │ yos    │ f_file_storage │ INSERT     │      0 │ mysql-bin.004775 │ 451045845  │ 116                   │
            # └─────────────────────┴────────┴────────────────┴────────────┴────────┴──────────────────┴────────────┴───────────────────────┘
            #
            #
            # SELECT
            #     max(toUInt32(binlog_pos)) - min(toUInt32(binlog_pos)) AS transaction_size,
            #     sum(toUInt32(single_statement_size)) AS size
            # FROM mysql_monitor.rttax_binlog
            # WHERE gtid = 'xxx'
            # GROUP BY gtid
            #
            # ┌─transaction_size─┬─size─┐
            # │             1704 │ 1587 │
            # └──────────────────┴──────┘
            #
            # size比transaction_size小, 是因为size没有Table_map event,
            #     f_file_info的Table_map event大小为71
            #     f_file_storage的Table_map event大小为91
            #
            # 事务大小为 1587+2*71+91=1820 = 1704+116
            #
            #
            #
            # 所以计算事务大小应该用 max(binlog_pos)-min(binlog_pos) + 最后一个event_length
            event_length = header.eventLength
            gtid = header.gtid
            event_type_number = header.eventType
            event_type = EntryProtocol_pb2.EventType.Name(event_type_number)

            single_statement_affected_rows = len(row_change.rowDatas)

            data = dict(
                schema=database,
                table=table,
                event_type=event_type,
                is_ddl=1 if row_change.isDdl else 0,
                binlog_file=binlog_file,
                binlog_pos=str(binlog_pos),
                characterset=characterset,
                execute_time=datetime.fromtimestamp(execute_time / 1000),
                single_statement_affected_rows=single_statement_affected_rows,
                single_statement_size=str(event_length),
                gtid=header.gtid,
            )
            entry_list.append(data)

        if len(entry_list) >= consumer.sync:
            insert_to_ch(consumer.ch_db, consumer.ch_table, entry_list)
            entry_list = []


# /root/.pyenv/versions/canal_kafka_consume/bin/python mon_mysql_dml.py \
# --bootstrap-servers=x1:9092,x2:9092,x3:9092 \
# --topic=dba_prod_pay \
# --ch_db=mysql_monitor \
# --ch_table=pay_dml_local \
# --sync=5000 \
# --k_user=admin \
# --from-stored \
# --partition=0