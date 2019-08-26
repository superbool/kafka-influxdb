# -*- encoding: utf8 -*-
import sys
import requests
import time
import ConfigParser
import signal
import thread
import json
from confluent_kafka import Consumer, KafkaError
import logging

logging.basicConfig(format='%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s', level=logging.INFO)
# 全局变量，判断是否程序正常退出
thread_running = True


def time_now():
    return int(round(time.time() * 1000))


class InfluxdbClient:
    def __init__(self, config):
        logging.info("init influxdb. config:%s", config)
        self.url = config.get('url')
        self.username = config.get('username')
        self.password = config.get('password')
        self.database = config.get('database')
        self.url_write = self.url + '/write?db=' + self.database
        if not self.ping():
            raise Exception("Can't connect to influxdb:" + self.url)
        self.session = requests.Session()

    def ping(self):
        response = requests.get(self.url + '/ping', timeout=3)
        status = response.status_code
        if status < 200 or status >= 300:
            return False
        else:
            return True

    def write(self, metrics):
        try:
            response = self.session.post(self.url_write, data=metrics, timeout=3)
            return response.status_code
        except Exception as e:
            logging.exception("write to influxdb error")
            return -1

    def fail_handle(self, status, retry_times, metrics):
        # TODO 保存失败或者发送报警通知等
        logging.error("save fail status=%s,retry_times=%s", status, retry_times)
        pass

    def write_until_success(self, metrics):
        i = 0
        status = self.write(metrics)
        while (status < 200 or status >= 300):
            i += 1
            if i > 2:
                self.fail_handle(status, i, metrics)
            time.sleep(i)
            status = self.write(metrics)


def convert_message(msglist):
    try:
        return "\n".join(str(msg.value().decode('utf-8')) for msg in msglist)
    except Exception as e:
        logging.exception('convert_message error')
        # 由于下面的执行方法耗时太长，5000条数据，上面的只需要10ms 而下面的方法需要800ms左右
        # 所以优先用上面的转换，如果有异常了再使用下面的方式
        metrics = ''
        for msg in msglist:
            if msg is None:
                continue
            if msg.error():
                logging.error("Consumer error: %s", msg.error())
                continue
            metrics += msg.value().decode('utf-8') + '\n'
        return metrics


def init_kafka_consumer(kafka_consumer_config):
    logging.info('init kafka consumer. config:%s', kafka_consumer_config)
    topics = kafka_consumer_config.pop('consumer.topics')
    kafka_consumer = Consumer(kafka_consumer_config)
    kafka_consumer.subscribe(topics.split(','))
    logging.info('init kafka consumer finish')
    return kafka_consumer


# 核心消费写入代码
def read_and_write(kafka_consumer, influxdb, batch_size):
    while thread_running:
        try:
            time1 = time_now()
            msglist = kafka_consumer.consume(num_messages=batch_size, timeout=5.0)
            time2 = time_now()
            metrics = convert_message(msglist)
            if not metrics:
                continue
            time3 = time_now()
            influxdb.write_until_success(metrics)
            time4 = time_now()
            logging.info("consumer_time=%sms,convert_time=%sms,save_time=%sms,total_time=%sms,msg_len=%s",
                         time2 - time1, time3 - time2, time4 - time3, time4 - time1, len(msglist))
            # TODO 最好手动提交offset，但没有api,所以这里待定
        except Exception as e:
            logging.exception('read_and_write error')
    kafka_consumer.close()


# 监听退出信号，保证正常退出
def exit_handler(signum, frame):
    logging.info('thread exit wait save finish')
    global thread_running
    thread_running = False
    time.sleep(5)
    logging.info('thread exit')
    sys.exit()


# 从文件读配置
def read_config_from_file(config_path):
    config = ConfigParser.ConfigParser()
    config.read(config_path)
    kafka_consumer_config = {}
    for item in config.items("kafka.consumer"):
        kafka_consumer_config[item[0]] = item[1]

    influxdb_config = {}
    for item in config.items("influxdb"):
        influxdb_config[item[0]] = item[1]
    return kafka_consumer_config, influxdb_config


# 从接口读配置
def read_config_from_http(url):
    response = requests.get(url)
    config = response.json()
    kafka_consumer_config = config.get('kafka.consumer')
    influxdb_config = config.get('influxdb')
    return kafka_consumer_config, influxdb_config


if __name__ == '__main__':
    if len(sys.argv) < 2:
        logging.error('需要指定配置文件或读取配置的接口')
        sys.exit(1)
    conf_param = sys.argv[1]

    if conf_param.startswith('http'):
        kafka_consumer_config, influxdb_config = read_config_from_http(conf_param)
    else:
        kafka_consumer_config, influxdb_config = read_config_from_file(conf_param)

    # 从kafka消费的数量
    batch_size = int(kafka_consumer_config.pop('consumer.batch.size'))

    influxdb = InfluxdbClient(influxdb_config)
    kafka_consumer = init_kafka_consumer(kafka_consumer_config)

    signal.signal(signal.SIGINT, exit_handler)
    thread.start_new_thread(read_and_write, (kafka_consumer, influxdb, batch_size))

    while True:
        # 注意不能使用pass 否则会占用大量cpu
        time.sleep(3600)
