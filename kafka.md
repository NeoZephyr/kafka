##  环境搭建
### jdk
/etc/profile
```sh
export JAVA_HOME=
export ZOOKEEPER_HOME=
export PATH=$PATH:$JAVA_HOME/bin
export PATH=$PATH:$HADOOP_HOME/bin
```
```sh
source /etc/profile
```

zoo.cfg
```sh
# ZooKeeper 服务器心跳时间，单位为 ms
tickTime=2000
# 投票选举新 leader 的初始化时间
initLimit=10
# 响应超过 syncLimit * tickTime，leader 认为 follower 死掉，从服务器列表中删除 follower
syncLimit=5
# 数据目录
dataDir=/tmp/zookeeper/data
# 日志目录
dataLogDir=/tmp/zookeeper/log
# zooKeeper 对外服务端口
clientPort=2181
```
```sh
zkServer.sh start
zkServer.sh status
```

server.properties
```sh
# broker 的编号，如果集群中有多个 broker，则每个 broker 的编号需要设置的不同
broker.id=0

# broker 对外提供的服务入口地址
# listeners=PLAINTEXT://:9092
listeners=PLAINTEXT://node01:9092,PLAINTEXT://node02:9092,PLAINTEXT://node03:9092

# 存放消息日志文件的地址
log.dirs=/tmp/kafka-logs

# zooKeeper 集群地址
# zookeeper.connect=node01:2181/kafka
zookeeper.connect=node01:2181,node02:2181,node03:2181/kafka
```
其它参数：
`advertised.listeners`: 主要用于 IaaS（Infrastructure as a Service）环境，比如公有云上的机器通常配备有多块网卡，即包含私网网卡和公网网卡，对于这种情况而言，可以设置 `advertised.listeners` 参数绑定公网 IP 供外部客户端使用，而配置 `listeners` 参数来绑定私网 IP 地址供 broker 间通信使用

`message.max.bytes`: broker 所能接收消息的最大值，默认值为 1000012B

## 脚本命令
启动 kafka
```sh
kafka-server-start.sh server.properties
```
创建 topic
```sh
kafka-topics.sh --bootstrap-server node01:9092 --create --topic topic-demo --replication-factor 3 --partitions 10

kafka-topics.sh --zookeeper node01:2181 --create --topic topic-demo --replication-factor 3 --partitions 10
```
展示主题信息
```sh
kafka-topics.sh --bootstrap-server node01:9092 --describe --topic topic-demo
kafka-topics.sh --zookeeper node01:2181 --describe --topic topic-demo
```
```sh
kafka-topics.sh --bootstrap-server node01:9092 --list
kafka-topics.sh --zookeeper node01:9092 --list
```
```sh
kafka-console-producer.sh --broker-list node01:9092 --topic topic-demo
```
```sh
# --from-beginning
kafka-console-consumer.sh --bootstrap-server node01:9092 --topic topic-demo
```

## broker 重要配置
### log.dirs
指定了 Broker 需要使用的若干个文件目录路径。在生产环境中一般为 log.dirs 配置多个路径，使用用逗号进行分隔。如果有条件，最好保证这些目录挂载到不同的物理磁盘上。这样做有两个好处：
1. 提升读写性能：比起单块磁盘，多块物理磁盘同时读写数据有更高的吞吐量
2. 能够实现故障转移：自 1.1 开始，坏掉的磁盘上的数据会自动地转移到其他正常的磁盘上，而且 Broker 还能正常工作

### zookeeper.connect
zookeeper 负责协调管理并保存 Kafka 集群的所有元数据信息：比如集群中运行的 Broker、创建的 Topic、每个 Topic 的分区、分区的 Leader 副本信息等

```
zk1:2181,zk2:2181,zk3:2181/kafka
```

### listeners
监听器，告诉外部连接者要通过什么协议访问指定主机名和端口开放的 Kafka 服务

### advertised.listeners
声明用于用于对外发布的监听器

### auto.create.topics.enable
是否允许自动创建 Topic，建议设置成 false，即不允许自动创建 Topic

### unclean.leader.election.enable
是否允许 Unclean Leader 选举，建议设置成 false

如果设置成 false，就防止那些落后太多的副本竞选 Leader。这样的话，当保存数据比较多的副本都挂了的话，这个分区也就不可用了，因为没有 Leader 了。如果设置为 true，允许从那些落后太多的副本中选一个出来当 Leader。这样的话，数据就有可能丢失，因为这些副本保存的数据本来就不全

### auto.leader.rebalance.enable
是否允许定期进行 Leader 选举，建议设置成 false

设置为 true 表示允许定期地对一些 Topic 分区进行 Leader 重选举。即在满足一定的条件的情况下。更换换 Leader。比如 Leader A 一直表现得很好，但若该参数设置为 true，那么有可能一段时间后 Leader A 就要被强行卸任换成 Leader B。换一次 Leader 代价很高，原本向 A 发送请求的所有客户端都要切换成向 B 发送请求，而且这种换 Leader 本质上没有任何性能收益

### log.retention.{hour|minutes|ms}
控制一条消息数据被保存多长时间。从优先级上来说 ms 设置最高、minutes 次之、hour 最低。通常情况下设置 hour 级别的多一些，比如 log.retention.hour=168 表示默认保存 7 天的数据，自动删除 7 天前的数据

### log.retention.bytes
指定 Broker 为消息保存的总磁盘容量大小

这个值默认是 -1，表示在这台 Broker 上保存数据大小不做限制。当在云上构建多租户的 Kafka 集群，限制每个租户使用的磁盘空间，可以使用这个参数

### message.max.bytes
控制 Broker 能够接收的最大消息大小

默认值为 1000012，还不到 1MB。实际场景中突破 1MB 的消息都是屡见不鲜的，因此在线上环境中设置一个比较大的值还是比较保险的做法。毕竟它只是一个标尺而已，仅仅衡量 Broker 能够处理的最大消息大小，即使设置大一点也不会耗费什么磁盘空间

## topic 重要配置
### retention.ms
Topic 消息被保存的时长，默认是 7 天。一旦设置了这个值，它会覆盖掉 Broker 端的全局参数值

### retention.bytes
Topic 预留多大的磁盘空间，和全局参数作用相似，在多租户的 Kafka 集群中会被使用到。当前默认值是 -1，表示可以无限使用磁盘空间

### max.message.bytes
Kafka Broker 能够正常接收该 Topic 的最大消息大小

replica.fetch.max.bytes -> 复制
fetch.message.max.bytes -> 消费

## topic 参数设置
### 创建时进行设置
```sh
kafka-topics.sh --bootstrap-server node01:9092 --create --topic transaction --partitions 10 --replication-factor 3 --config retention.ms=15552000000 --config max.message.bytes=5242880
```

### 修改时设置（推荐）
```sh
kafka-configs.sh --zookeeper node01:2181 --entity-type topics --entity-name transaction --alter --add-config max.message.bytes=10485760
```

## jvm 参数
KAFKA_HEAP_OPTS：指定堆大小，默认 1GB
KAFKA_JVM_PERFORMANCE_OPTS：指定 GC 参数

```sh
export KAFKA_HEAP_OPTS=--Xms6g  --Xmx6g
export KAFKA_JVM_PERFORMANCE_OPTS= -server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent -Djava.awt.headless=true
```

## 生产者参数
### `acks`
```sh
properties.put(ProducerConfig.ACKS_CONFIG, "0");
```
指定分区中必须要有多少个副本收到这条消息，生产者才会认为这条消息是成功写入的。acks 涉及消息的可靠性和吞吐量之间的权衡，acks 参数有3种类型的值（都是字符串类型）：

1. acks = 1，默认值即为 1。生产者发送消息之后，只要分区的 leader 副本成功写入消息，那么它就会收到来自服务端的成功响应。如果消息无法写入 leader 副本，比如在 leader 副本崩溃、重新选举新的 leader 副本的过程中，那么生产者就会收到一个错误的响应，为了避免消息丢失，生产者可以选择重发消息。如果消息写入 leader 副本并返回成功响应给生产者，且在被其他 follower 副本拉取之前 leader 副本崩溃，那么此时消息还是会丢失，因为新选举的 leader 副本中并没有这条对应的消息。acks 设置为 1，是消息可靠性和吞吐量之间的折中方案

2. acks = 0。生产者发送消息之后不需要等待任何服务端的响应。如果在消息从发送到写入 Kafka 的过程中出现某些异常，导致 Kafka 并没有收到这条消息，那么生产者也无从得知，消息也就丢失了

3. acks = -1 或 acks = all。生产者在消息发送之后，需要等待 ISR 中的所有副本都成功写入消息之后才能够收到来自服务端的成功响应。但这并不意味着消息就一定可靠，因为 ISR 中可能只有 leader 副本，这样就退化成了 acks = 1 的情况。要获得更高的消息可靠性需要配合 `min.insync.replicas` 等参数的联动

### `max.request.size`
这个参数用来限制生产者客户端能发送的消息的最大值，默认值为 1MB。这个参数还涉及一些其他参数的联动，比如 broker 端的 `message.max.bytes` 参数

### `retries`
retries 参数用来配置生产者重试的次数，默认值为 0，即在发生异常的时候不进行任何重试动作。消息在从生产者发出到成功写入服务器之前可能发生一些临时性的异常，比如网络抖动、leader 副本的选举等，这种异常往往是可以自行恢复的，生产者可以通过配置 retries 大于 0 的值，以此通过内部重试来恢复而不是一味地将异常抛给生产者的应用程序。如果重试达到设定的次数，那么生产者就会放弃重试并返回异常。不过并不是所有的异常都是可以通过重试来解决的，比如消息太大，超过 `max.request.size` 参数配置的值时

### `retry.backoff.ms`
这个参数的默认值为 100，表示两次重试之间的时间间隔，避免无效的频繁重试。在配置 retries 和 `retry.backoff.ms` 之前，最好先估算一下可能的异常恢复时间，这样可以设定总的重试时间大于这个异常恢复时间，以此来避免生产者过早地放弃重试。

对于某些应用来说，顺序性非常重要，比如 MySQL 的 binlog 传输。如果将 acks 参数配置为非零值，并且 `max.in.flight.requests.per.connection` 参数配置为大于 1 的值，就可能出现错序的现象：如果第一批次消息写入失败，而第二批次消息写入成功，那么生产者会重试发送第一批次的消息，此时如果第一批次的消息写入成功，那么这两个批次的消息就出现了错序。一般而言，在需要保证消息顺序的场合建议把参数 `max.in.flight.requests.per.connection` 配置为 1

### `compression.type`
这个参数用来指定消息的压缩方式，默认值为 none，即默认情况下，消息不会被压缩。可以配置为 gzip, snappy 和 lz4。对消息进行压缩可以极大地减少网络传输量、降低网络 I/O，从而提高整体的性能。消息压缩是一种使用时间换空间的优化方式，如果对时延有一定的要求，则不推荐对消息进行压缩

### `connections.max.idle.ms`
这个参数用来指定在多久之后关闭闲置的连接，默认值是 9 分钟

### `linger.ms`
这个参数用来指定生产者发送 ProducerBatch 之前等待更多消息（ProducerRecord）加入 ProducerBatch 的时间，默认值为 0。生产者客户端会在 ProducerBatch 被填满或等待时间超过 `linger.ms` 值时发送出去。增大这个参数的值会增加消息的延迟，但是同时能提升一定的吞吐量

### `receive.buffer.bytes`
这个参数用来设置 Socket 接收消息缓冲区（SO_RECBUF）的大小，默认值为 32KB。如果设置为 -1，则使用操作系统的默认值。如果 Producer 与 Kafka 处于不同的机房，则可以适地调大这个参数值

### `send.buffer.bytes`
这个参数用来设置 Socket 发送消息缓冲区（SO_SNDBUF）的大小，默认值为 128KB。如果设置为 -1，则使用操作系统的默认值

### `request.timeout.ms`
这个参数用来配置 Producer 等待请求响应的最长时间，默认值为 30000ms。请求超时之后可以选择进行重试，这个参数需要比 broker 端参数 `replica.lag.time.max.ms` 的值要大，这样可以减少因客户端重试而引起的消息重复的概率

## 操作系统参数
### 文件描述符限制
设置为一个较大的值
```
ulimit -n 1000000
```

### 文件系统类型
XFS

### Swappiness
swap 空间一旦设置成 0，当物理内存耗尽时，操作系统会触发 OOM killer 这个组件，它会随机挑选一个进程然后 kill 掉，没有任何预警。可以设置成一个比较小的值，当开始使用 swap 空间时，能够观测到 Broker 性能开始出现急剧下降，从而方便进一步调优和诊断问题

### 提交时间
适当地增加提交间隔来降低物理磁盘的写操作






如果允许follower副本对外提供读服务（主写从读），首先会存在数据一致性的问题，消息从主节点同步到从节点需要时间，可能造成主从节点的数据不一致。主写从读无非就是为了减轻leader节点的压力，将读请求的负载均衡到follower节点，如果Kafka的分区相对均匀地分散到各个broker上，同样可以达到负载均衡的效果，没必要刻意实现主写从读增加代码实现的复杂程度


磁盘选择
使用普通机械硬盘即可
Kafka 使用磁盘的方式多是顺序读写操作，一定程度上规避了机械磁盘最大的劣势，即随机读写操作慢
Kafka 自己实现了冗余机制来提供高可靠性，另外在软件层面自行实现负载均衡，因此可以不搭建 RAID

规划磁盘容量需要考虑以下因素
新增消息数
消息留存时间
平均消息大小
备份数
是否启用压缩

Consumer 使用拉模式从服务端拉取消息，并且保存消费的具体位置，当消费者宕机后恢复上线时可以根据之前保存的消费位置重新拉取需要的消息进行消费

分区中的所有副本统称为 AR（Assigned Replicas）
所有与 leader 副本保持一定程度同步的副本（包括 leader 副本在内）组成ISR（In-Sync Replicas）
与 leader 副本同步滞后过多的副本（不包括 leader 副本）组成 OSR（Out-of-Sync Replicas）

leader 副本负责维护和跟踪 ISR 集合中所有 follower 副本的滞后状态，当 follower 副本落后太多或失效时，leader 副本会把它从 ISR 集合中剔除。如果 OSR 集合中有 follower 副本“追上”了 leader 副本，那么 leader 副本会把它从 OSR 集合转移至 ISR 集合。默认情况下，当 leader 副本发生故障时，只有在 ISR 集合中的副本才有资格被选举为新的 leader，而在 OSR 集合中的副本则没有任何机会（不过这个原则也可以通过修改相应的参数配置来改变）

HW 即 High Watermark，标识了一个特定的消息偏移量（offset），消费者只能拉取到这个 offset 之前的消息

LEO 即 Log End Offset，标识当前日志文件中下一条待写入消息的 offset

分区 ISR 集合中的每个副本都会维护自身的 LEO，而 ISR 集合中最小的 LEO 即为分区的 HW




解耦
削峰


一个 partition 只分布于一个 broker 上
一个 partition 对应一个文件夹
一个 partition 包含多个 segment
一个 segment 对应一个文件
记录之后被追加到 segment 中，不会被单独删除或者修改
清除过期日志时，直接删除一个或者多个 segment

Partitioner 决定 Producer 发送的消息到哪个 broker
实现 Partitioner 接口

```java
public class HashPartitioner implements Partitioner {
    public HashPartitioner(VerifiableProperties verifiableProperties) {}

    public int partition(Object key, int numPartitions) {
        if ((key instanceof Integer)) {
            return Math.abs(Integer.parseInt(key.toString())) % numPartitions;
        }
        return Math.abs(key.hashCode() % numPartitions);
    }
}

public class RoundRobinPartitioner implements Partitioner {
    private static AtomicLong next = new AtomicLong();

    public RoundRobinPartitioner(VerifiableProperties verifiableProperties) {}

    public int partition(Object key, int numPartitions) {
        long nextIndex = next.incrementAndGet();
        return (int)nextIndex % numPartitions;
    }
}
```
```java
public class ProducerDemo {

  static private final String TOPIC = "topic1";
  static private final String ZOOKEEPER = "localhost:2181";
  static private final String BROKER_LIST = "localhost:9092";
//  static private final int PARTITIONS = TopicAdmin.partitionNum(ZOOKEEPER, TOPIC);
  static private final int PARTITIONS = 3;


  public static void main(String[] args) throws Exception {
    Producer<String, String> producer = initProducer();
    sendOne(producer, TOPIC);
  }

  private static Producer<String, String> initProducer() {
    Properties props = new Properties();
    props.put("metadata.broker.list", BROKER_LIST);
    // props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("serializer.class", StringEncoder.class.getName());
    props.put("partitioner.class", HashPartitioner.class.getName());
    // props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
//    props.put("compression.codec", "0");
    props.put("producer.type", "async");
    props.put("batch.num.messages", "3");
    props.put("queue.buffer.max.ms", "10000000");
    props.put("queue.buffering.max.messages", "1000000");
    props.put("queue.enqueue.timeout.ms", "20000000");

    ProducerConfig config = new ProducerConfig(props);
    Producer<String, String> producer = new Producer<String, String>(config);
    return producer;
  }

  public static void sendOne(Producer<String, String> producer, String topic) throws InterruptedException {
    KeyedMessage<String, String> message1 = new KeyedMessage<String, String>(topic, "31", "test 31");
    producer.send(message1);
    Thread.sleep(5000);
    KeyedMessage<String, String> message2 = new KeyedMessage<String, String>(topic, "31", "test 32");
    producer.send(message2);
    Thread.sleep(5000);
    KeyedMessage<String, String> message3 = new KeyedMessage<String, String>(topic, "31", "test 33");
    producer.send(message3);
    Thread.sleep(5000);
    KeyedMessage<String, String> message4 = new KeyedMessage<String, String>(topic, "31", "test 34");
    producer.send(message4);
    Thread.sleep(5000);
    KeyedMessage<String, String> message5 = new KeyedMessage<String, String>(topic, "31", "test 35");
    producer.send(message5);
    Thread.sleep(5000);
    producer.close();
  }
}
```
```java
public class DemoConsumer {

  /**
   * @param args
   */
  public static void main(String[] args) {
     args = new String[]{"localhost:2181", "topic1", "group1", "consumer1"};
    if (args == null || args.length != 4) {
      System.err.print(
          "Usage:\n\tjava -jar kafka_consumer.jar ${zookeeper_list} ${topic_name} ${group_name} ${consumer_id}");
      System.exit(1);
    }
    String zk = args[0];
    String topic = args[1];
    String groupid = args[2];
    String consumerid = args[3];
    Properties props = new Properties();
    props.put("zookeeper.connect", zk);
    props.put("group.id", groupid);
    props.put("autooffset.reset", "largest");
    props.put("autocommit.enable", "true");
    props.put("client.id", "test");
    props.put("auto.commit.interval.ms", "1000");

    ConsumerConfig consumerConfig = new ConsumerConfig(props);
    ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
        consumerConnector.createMessageStreams(topicCountMap);

    KafkaStream<byte[], byte[]> stream1 = consumerMap.get(topic).get(0);
    ConsumerIterator<byte[], byte[]> it1 = stream1.iterator();
    while (it1.hasNext()) {
      MessageAndMetadata<byte[], byte[]> messageAndMetadata = it1.next();
      String message =
          String.format("Consumer ID:%s, Topic:%s, GroupID:%s, PartitionID:%s, Offset:%s, Message Key:%s, Message Payload: %s",
              consumerid,
              messageAndMetadata.topic(), groupid, messageAndMetadata.partition(),
              messageAndMetadata.offset(), new String(messageAndMetadata.key()),new String(messageAndMetadata.message()));
      System.out.println(message);
    }
  }
}
```

Sync Producer
低延迟
低吞吐率
无数据丢失

Async Producer
高延迟
高吞吐率
可能会有数据丢失

replica 个数由参数 replication-factor 决定，表示每个 partition 的副本数
replica 个数一定要小于等于 broker 数，对每个 partition 而言，每个 broker 上之后有一个 replica

leader 维护 isr

kafka 使用 zookeeper
topic 等配置管理
leader 选举
服务发现

zookeeper 节点类型
持久/临时
顺序/非顺序

注册 watch
Created event
Deleted event
Changed event
Child event

watch
1. 先得到通知，再得到数据
2. watch 被 fire 之后就取消，不会再 watch 后续变化，需要再次进行注册



```sh
tar -zxf zookeeper.tar.gz
mv zookeeper /usr/local/zookeeper
mkdir -p /var/lib/zookeeper
cat > /usr/local/zookeeper/zoo.cfg << EOF
tickTime=2000
dataDir=/var/lib/zookeeper
clientPort=2181
EOF

export JAVA_HOME=...
/usr/local/zookeeper/bin/zkServer.sh start
```
```sh
lsof -i:2181
```


```sh
tar -zxf kafka.tar.gz
mv kafka /usr/local/kafka
mkdir /tmp/kafka-logs
export JAVA_HOME=...
/usr/local/kafka/bin/kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties
```

```sh
# create
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

# describe
/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --describe --topic test

/usr/local/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181

/usr/local/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test

/usr/local/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning
```

broker 配置
所有 broker 都必须配置相同的  zookeeper.connect，broker.id 必须设置为唯一的值。kafka 使用 Zookeeper 来保存 broker、主题和分区的元数据信息。
```
broker.id
port
zookeeper.connect
log.dirs
num.recovery.threads,per.data.dir
auto,create.topics.enable
```

topic 配置
```
num.partitions

# 数据保留时间，默认 168 小时，即一周
log.retention.ms

# 数据保留大小
log.retention.bytes

# 日志片段大小，默认 1GB
log.segment.bytes

# 日志片段关闭时间
log.segment.ms

# 单个消息大小限制，默认 1MB
message.max.bytes
```

生产者
```
bootstrap.servers
key.serializer
value.serializer
```
```
# acks=0 表示生产者在成功写入消息之前不会等待任何来自服务器的响应。
# acks=1 表示只要集群的首领节点收到消息，生产者就会收到一个来自服务器的成功响应。
# acks=all 表示只有当所有参与复制的节点全部收到消息时，生产者才会收到一个来自服务器的成功响应。
acks

# 生产者内存缓冲区的大小，生产者用它缓冲要发送到服务器的消息。如果空间不足，调用 send 会被阻塞
buffer.memory

# 表示在调用 send() 或者 partitionsFor() 时的阻塞时间，阻塞时间超过时就会抛出异常
max.block.ms

# 默认情况下，消息发送不会被压缩。参数可以设置为 snappy、gzip 或者 lz4，表示不同的压缩算法
# snappy 占用较少的 cpu，提供较好的性能和可观的压缩比
# gzip 占用较多的 cpu，提供的压缩比更高
compression.type

# 表示当生产者从服务器接受到临时性错误时，能够重复发送消息的次数。如果达到这个次数，生产者会放弃重试并返回错误。每次重试间隔默认 100ms，可以通过参数 retry.backoff.ms 改变
retries

# 表示一个批次可以使用的内存大小，当有多个消息需要发送到同一个分区时，生产者会将这些消息放在同一批次中。
batch.size

# 表示生产者在发送批次之前等待更多消息加入批次的时间，生产者会在批次填满或者 linger.ms 达到时把批次发送出去。
linger.ms

# 任意字符串，用来识别消息的来源
client.id

# 表示生产者在收到服务器响应之前可以发送消息个数，设置为 1 可以保证消息是按照发送的顺序写入服务器的，即使有重试
max.in.flight.requests.per.connection

# 表示生产者在发送数据时等待服务器返回响应的时间
request.timeout.ms

# 表示生产者在获取元数据时等待服务器返回响应的时间
metadata.fetch.timeout.ms

# 指定 broker 等待同步副本返回消息确认的时间
timeout.ms

# 控制生产者发送的请求大小
max.request.size
```

```java
Properties props = new Properties();
props.put("bootstrap.servers", "broker1:9092,broker2:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props)
```

```java
ProducerRecord<String, String> record = new ProducerRecord<>("topic", "key", "value");

try {
    producer.send(record);
} catch (Exception e) {
    e.printStackTrace();
}
```
同步发送消息，返回 RecordMetaData 对象，包含了主键和分区信息
```java
ProducerRecord<String, String> record = new ProducerRecord<>("topic", "key", "value");

try {
    producer.send(record).get();
} catch (Exception e) {
    e.printStackTrace();
}
```
异步发送消息
```java
private class ProducerCallback implements Callback {
    public void onCompletion(RecordMetadata RecordMetadata, Exception e) {
        if (e != null) {
            e.printStackTrace();
        }
    }
}

ProducerRecord<String, String> record = new ProducerRecord<>("topic", "key", "value");
producer.send(record, new ProducerCallback());
```

ProducerRecord 对象中的键有两个用途，可以作为消息的附加信息，也可以用来决定消息写入主题对应的分区。如果键值为 null，就使用了默认的分区器，那么 ProducerRecord 将会被随机地发送到主题内各个可用的分区上。分区器使用轮询的算法将消息均衡地分布到各个分区上；如果键不为空，并且使用了默认的分区器，则会对键进行散列，根据散列值把消息映射到对应分区。

自定义分区器
```java
public class MyPartitioner implements Partitioner {
    public void configure(Map<String, ?> configs) {}

    public int partition(String topic, Object key, byte[] keyBytes,
        Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partions = cluster.partitionsForTopic(topic);

        int numPartitions = partions.size();

        if ((keyBytes == null) || (!(key instanceof String))) {
            throw new InvalidRecordException("...");
        }

        // 分配到最后一个分区
        if (((String) key).equals("pain"))
            return numPartitions;

        // 分配到其它分区
        return (Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1))
    }

    public void close() {}
}
```






