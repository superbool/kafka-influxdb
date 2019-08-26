### 一个高效的将kafka中influxdb line格式指标写入到influxdb的脚本

### 功能
1. 同步写入，从kafak消费的数据必须写入到influxdb后才继续消费再写入，否则会一直等待写入；
2. 批量消费，可以配置指定的批量条数；
3. 失败处理，写入influxdb失败循环等待，异常通知，程序退出保证写入；
4. 多配置方式，支持文件或者http接口的形式；


### 使用示例
#### 配置文件方式
```bash
python main.py ./simple.conf
```
simple.conf
```conf
#kafka相关配置
[kafka.consumer]
bootstrap.servers = localhost:9092
group.id = kafka-influxdb
auto.offset.reset = earliest
consumer.topics = metrics-test
consumer.batch.size = 1000

#influxdb相关配置
[influxdb]
url = http://localhost:8086
username = root
password = root
database = test
```

#### 通过接口的配置方式
```bash
python main.py http://localhost:8080/xxxx
```
配置http://localhost:8080/xxxx接口url可以自定义，只需要返回值返回以下格式的json文本即可
```json
{
    "kafka.consumer":{
        "bootstrap.servers":"localhost:9092",
        "group.id":"kafka-influxdb",
        "auto.offset.reset":"earliest",
        "consumer.topics":"metrics-test",
        "consumer.batch.size":"1000"
    },
    "influxdb":{
        "url":"http://localhost:8086",
        "username":"root",
        "password":"root",
        "database":"test"
    }
}
```