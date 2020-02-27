### 一个高效的将kafka中influxdb line格式指标写入到influxdb的脚本

### 功能
1. 同步写入，避免数据丢失，从kafak消费的数据必须写入到influxdb承购后才继续消费再写入，否则会一直等待写入；
2. 批量消费，可以配置指定的批量条数，能大幅度提高写入的效率；
3. 失败处理，写入influxdb失败循环等待，异常回调通知，程序退出前保证写入完成；
4. 多配置方式，支持文件或者http接口的形式,自动识别；


### 使用示例
#### 配置文件方式
```bash
python main.py ./simple.conf
```
simple.conf  key,value 
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
database = test
username = root
password = root
precision = ns
callback = http://localhost:8080/alert
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
        "database":"test",
        "username":"root",
        "password":"root",
        "precision":"ns",
        "callback":"http://localhost:8080/alert"
    }
}
```

## 参数释义

参数分成两部分：

### kafka.consumer

> bootstrap.servers      kafka集群地址，多个地址使用逗号分割
>
> group.id               kafka消费group组
>
> auto.offset.reset      初始消费位置 earliest从topic的最开始位置消费 latest从最新位置消费
>
> consumer.topics        消费的topic
>
> consumer.batch.size    单次消费的数据量

### influxdb

> url                    influxdb的地址
>
> database               要写入到的数据库
>
> username (可选)         用户名
>
> password (可选)         密码
>
> precision (可选)        写入的时间戳单位 [ns,u,ms,s,m,h]
>
> callback (可选)         当写入失败时，通知接口 json格式发送{"retry": retry_times, "status": status, "result": result}
