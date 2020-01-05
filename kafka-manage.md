创建主题
```sh
kafka-topics.sh --bootstrap-server broker_host:port --create --topic my_topic_name  --partitions 1 --replication-factor 1
```
从 Kafka 2.2 版本开始，推荐用 --bootstrap-server 参数替换 --zookeeper 参数。原因主要有两个：

1. 使用 --zookeeper 会绕过 Kafka 的安全体系
2. 使用 --bootstrap-server 与集群进行交互，越来越成为使用 Kafka 的标准姿势

查询主题
```sh
kafka-topics.sh --bootstrap-server broker_host:port --list
```

查询单个主题的详细数据
```sh
kafka-topics.sh --bootstrap-server broker_host:port --describe --topic <topic_name>
```

修改主题分区（目前 Kafka 不允许减少某个主题的分区数）
```sh
kafka-topics.sh --bootstrap-server broker_host:port --alter --topic <topic_name> --partitions <新分区数>
```

修改主题级别参数
```sh
kafka-configs.sh --zookeeper zookeeper_host:port --entity-type topics --entity-name <topic_name> --alter --add-config max.message.bytes=10485760
```

变更副本数
创建一个 json 文件，显式提供 50 个分区对应的副本数
```
{"version":1, "partitions":[
 {"topic":"__consumer_offsets","partition":0,"replicas":[0,1,2]}, 
  {"topic":"__consumer_offsets","partition":1,"replicas":[0,2,1]},
  {"topic":"__consumer_offsets","partition":2,"replicas":[1,0,2]},
  {"topic":"__consumer_offsets","partition":3,"replicas":[1,2,0]},
  ...
  {"topic":"__consumer_offsets","partition":49,"replicas":[0,1,2]}
]}`
```
```sh
kafka-reassign-partitions.sh --zookeeper zookeeper_host:port --reassignment-json-file reassign.json --execute
```

查看消费者组提交的位移数据
```sh
kafka-console-consumer.sh --bootstrap-server kafka_host:port --topic __consumer_offsets --formatter "kafka.coordinator.group.GroupMetadataManager\$OffsetsMessageFormatter" --from-beginning
```
查看消费者组的状态信息
```sh
kafka-console-consumer.sh --bootstrap-server kafka_host:port --topic __consumer_offsets --formatter "kafka.coordinator.group.GroupMetadataManager\$GroupMetadataMessageFormatter" --from-beginning
```

修改主题限速
```sh
kafka-configs.sh --zookeeper zookeeper_host:port --alter --add-config 'leader.replication.throttled.rate=104857600,follower.replication.throttled.rate=104857600' --entity-type brokers --entity-name 0
```
若该主题的副本分别在 0、1、2、3 多个 Broker 上，那么你还要依次为 Broker 1、2、3 执行这条命令

```sh
kafka-configs.sh --zookeeper zookeeper_host:port --alter --add-config 'leader.replication.throttled.replicas=*,follower.replication.throttled.replicas=*' --entity-type topics --entity-name test
```
为主题设置要限速的副本


主题分区迁移


删除主题
```sh
kafka-topics.sh --bootstrap-server broker_host:port --delete  --topic <topic_name>
```

主题删除失败
造成主题删除失败的原因有很多，最常见的原因有两个：副本所在的 Broker 宕机了；待删除主题的部分分区依然在执行迁移过程

1. 手动删除 ZooKeeper 节点 /admin/delete_topics 下以待删除主题为名的 znode
2. 手动删除该主题在磁盘上的分区目录
3. 在 ZooKeeper 中执行 rmr /controller，触发 Controller 重选举，刷新 Controller 缓存（可能造成大面积的分区 Leader 重选举）

__consumer_offsets 占用太多的磁盘

显式地用 jstack 命令查看一下 kafka-log-cleaner-thread 前缀的线程状态。通常情况下，这都是因为该线程挂掉了，无法及时清理此内部主题。倘若真是这个原因导致的，只能重启相应的 Broker 了


动态 Broker 参数使用场景
1. 动态调整 Broker 端各种线程池大小，实时应对突发流量
2. 动态调整 Broker 端连接信息或安全配置信息
3. 动态更新 SSL Keystore 有效期
4. 动态调整 Broker 端 Compact 操作性能
5. 实时变更 JMX 指标收集器 (JMX Metrics Reporter)

Kafka 将动态 Broker 参数保存在 ZooKeeper 中
```
ls /config/brokers
get /config/brokers/0
```

参数优先级：per-broker 参数 > cluster-wide 参数 > static 参数 > Kafka 默认值
```sh
# 设置全局值
# 如果要设置 cluster-wide 范围的动态参数，需要显式指定 entity-default
kafka-configs.sh --bootstrap-server kafka-host:port --entity-type brokers --entity-default --alter --add-config unclean.leader.election.enable=true
```
```sh
kafka-configs.sh --bootstrap-server kafka-host:port --entity-type brokers --entity-default --describe
```

```sh
kafka-configs.sh --bootstrap-server kafka-host:port --entity-type brokers --entity-name 1 --alter --add-config unclean.leader.election.enable=false
```
```sh
kafka-configs.sh --bootstrap-server kafka-host:port --entity-type brokers --entity-name 1 --describe
```

删除 cluster-wide 范围参数或 per-broker 范围参数
```sh
# 删除 cluster-wide 范围参数
kafka-configs.sh --bootstrap-server kafka-host:port --entity-type brokers --entity-default --alter --delete-config unclean.leader.election.enable
```

```sh
# 删除 per-broker 范围参数
kafka-configs.sh --bootstrap-server kafka-host:port --entity-type brokers --entity-name 1 --alter --delete-config unclean.leader.election.enable
```

查看动态 Broker 参数
直接运行无参数的 kafka-configs 脚本

最常用的动态 Broker 参数
log.retention.ms：修改日志留存时间
num.io.threads 和 num.network.threads
ssl.keystore.type、ssl.keystore.location、ssl.keystore.password 和 ssl.key.password

num.replica.fetchers
增加该参数值，确保有充足的线程可以执行 Follower 副本向 Leader 副本的拉取


## 重设位移
### Earliest 策略
表示将位移调整到主题当前最早位移处。这个最早位移不一定就是 0，因为很久远的消息会被 Kafka 自动删除，所以当前最早位移很可能是一个大于 0 的值

### Latest 策略
表示把位移重设成最新末端位移。如果总共向某个主题发送了 15 条消息，那么最新末端位移就是 15。如果想跳过所有历史消息，从最新的消息处开始消费的话，可以使用 Latest 策略

### Current 策略
表示将位移调整成消费者当前提交的最新位移。适用场景：修改代码重启消费者之后，发现有问题，需要回滚代码，同时也要把位移重设到消费者重启时的位置，那么可以使用 Current 策略

### Specified-Offset 策略
表示消费者把位移值调整到你指定的位移处。在实际使用过程中，可能会出现 corrupted 消息无法被消费的情形，此时消费者程序会抛出异常，无法继续工作。碰到这个问题，可以尝试使用 Specified-Offset 策略来规避

### Shift-By-N 策略
指定位移的相对数值

### DateTime 策略
指定一个时间，然后将位移重置到该时间之后的最早位移处。如果需要重新消费昨天的数据，那么可以使用该策略重设位移到昨天 0 点

### Duration 策略
给定相对的时间间隔，然后将位移调整到距离当前给定时间间隔的位移处，具体格式是 PnDTnHnMnS。如果想将位移调回到 15 分钟前，那么可以指定 PT0H15M0S

## 重设位移的方式
### 消费者 API
```java
void seek(TopicPartition partition, long offset);
void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata);
void seekToBeginning(Collection<TopicPartition> partitions);
void seekToEnd(Collection<TopicPartition> partitions);
```

Earliest 策略
```java
Properties consumerProperties = new Properties();

// 禁止自动提交位移
consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

String topic = "test";
try (final KafkaConsumer<String, String> consumer = 
  new KafkaConsumer<>(consumerProperties)) {
         consumer.subscribe(Collections.singleton(topic));
         consumer.poll(0);
         consumer.seekToBeginning(
  consumer.partitionsFor(topic).stream().map(partitionInfo ->          
    new TopicPartition(topic, partitionInfo.partition()))
    .collect(Collectors.toList()));
}
```

Latest 策略
```java
consumer.seekToEnd(
  consumer.partitionsFor(topic).stream().map(partitionInfo ->          
  new TopicPartition(topic, partitionInfo.partition()))
  .collect(Collectors.toList()));
```

Current 策略
```java
consumer.partitionsFor(topic).stream().map(info -> 
  new TopicPartition(topic, info.partition()))
  .forEach(tp -> {
  long committedOffset = consumer.committed(tp).offset();
  consumer.seek(tp, committedOffset);
});
```

Specified-Offset 策略
```java
long targetOffset = 1234L;
for (PartitionInfo info : consumer.partitionsFor(topic)) {
  TopicPartition tp = new TopicPartition(topic, info.partition());
  consumer.seek(tp, targetOffset);
}
```

Shift-By-N 策略
```java
for (PartitionInfo info : consumer.partitionsFor(topic)) {
         TopicPartition tp = new TopicPartition(topic, info.partition());
         long targetOffset = consumer.committed(tp).offset() + 123L; 
         consumer.seek(tp, targetOffset);
}
```

DateTime 策略
```java
long ts = LocalDateTime.of(
  2019, 6, 20, 20, 0).toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
Map<TopicPartition, Long> timeToSearch = 
         consumer.partitionsFor(topic).stream().map(info -> 
  new TopicPartition(topic, info.partition()))
  .collect(Collectors.toMap(Function.identity(), tp -> ts));

for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : 
  consumer.offsetsForTimes(timeToSearch).entrySet()) {
    consumer.seek(entry.getKey(), entry.getValue().offset());
}
```

Duration 策略
```java
Map<TopicPartition, Long> timeToSearch = consumer.partitionsFor(topic).stream()
         .map(info -> new TopicPartition(topic, info.partition()))
         .collect(Collectors.toMap(Function.identity(), tp -> System.currentTimeMillis() - 30 * 1000  * 60));

for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : 
         consumer.offsetsForTimes(timeToSearch).entrySet()) {
         consumer.seek(entry.getKey(), entry.getValue().offset());
}
```

### 命令行脚本
Earliest 策略
```sh
kafka-consumer-groups.sh --bootstrap-server kafka-host:port --group test-group --reset-offsets --all-topics --to-earliest –execute
```

Latest 策略
```sh
kafka-consumer-groups.sh --bootstrap-server kafka-host:port --group test-group --reset-offsets --all-topics --to-latest --execute
```

Current 策略
```sh
kafka-consumer-groups.sh --bootstrap-server kafka-host:port --group test-group --reset-offsets --all-topics --to-current --execute
```

Specified-Offset 策略
```sh
kafka-consumer-groups.sh --bootstrap-server kafka-host:port --group test-group --reset-offsets --all-topics --to-offset <offset> --execute
```

Shift-By-N 策略
```sh
kafka-consumer-groups.sh --bootstrap-server kafka-host:port --group test-group --reset-offsets --shift-by <offset_N> --execute
```

DateTime 策略
```sh
kafka-consumer-groups.sh --bootstrap-server kafka-host:port --group test-group --reset-offsets --to-datetime 2019-06-20T20:00:00.000 --execute
```

Duration 策略
```sh
kafka-consumer-groups.sh --bootstrap-server kafka-host:port --group test-group --reset-offsets --by-duration PT0H30M0S --execute
```

## 常用脚本
### 生产消息
```sh
kafka-console-producer.sh --broker-list kafka-host:port --topic test-topic --request-required-acks -1 --producer-property compression.type=lz4
```

### 消费消息
```sh
kafka-console-consumer.sh --bootstrap-server kafka-host:port --topic test-topic --group test-group --from-beginning --consumer-property enable.auto.commit=false 
```

### 测试生产者性能
```sh
# 发送 1 千万条消息，每条消息大小是 1KB
kafka-producer-perf-test.sh --topic test-topic --num-records 10000000 --throughput -1 --record-size 1024 --producer-props bootstrap.servers=kafka-host:port acks=-1 linger.ms=2000 compression.type=lz4
```

### 测试消费者性能
```sh
kafka-consumer-perf-test.sh --broker-list kafka-host:port --messages 10000000 --topic test-topic
```

### 查看主题消息总数
```sh
kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list kafka-host:port --time -2 --topic test-topic

test-topic:0:0
test-topic:1:0

kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list kafka-host:port --time -1 --topic test-topic

test-topic:0:5500000
test-topic:1:5500000
```

### 查看消息文件数据
```sh
# 显示的是消息批次（RecordBatch）或消息集合（MessageSet）的元数据信息
kafka-dump-log.sh --files ../data_dir/kafka_1/test-topic-1/00000000000000000000.log
```
```sh
# 查看每条具体的消息
kafka-dump-log.sh --files ../data_dir/kafka_1/test-topic-1/00000000000000000000.log --deep-iteration
```
```sh
# 查看消息里面的实际数据
kafka-dump-log.sh --files ../data_dir/kafka_1/test-topic-1/00000000000000000000.log --deep-iteration --print-data-log
```

### 查询消费者组位移
```sh
# CURRENT-OFFSET 表示该消费者当前消费的最新位移
# LOG-END-OFFSET 表示对应分区最新生产消息的位移
kafka-consumer-groups.sh --bootstrap-server kafka-host:port --describe --group test-group
```


## KafkaAdminClient
```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>2.3.0</version>
</dependency>
```

AdminClient 是一个双线程的设计：前端主线程和后端 I/O 线程

### 前端线程
负责将用户要执行的操作转换成对应的请求，然后再将请求发送到后端 I/O 线程的队列中

1. 构建对应的请求对象
2. 指定响应的回调逻辑，即从 Broker 端接收到请求对象之后要执行的动作
3. 将其请求对象放入到新请求队列


### 后端 IO 线程
IO 线程使用了新请求队列、待发送请求队列和处理中请求队列来承载不同时期的请求对象。使用 3 个队列的原因是：是目前新请求队列的线程安全是由 Java 的 monitor 锁来保证的。为了确保前端主线程不会因为 monitor 锁被阻塞，后端 I/O 线程会定期地将新请求队列中的所有实例全部搬移到待发送请求队列中进行处理。后续的待发送请求队列和处理中请求队列只由后端 IO 线程处理，因此无需任何锁机制来保证线程安全

当 IO 线程在处理某个请求时，它会显式地将该请求保存在处理中请求队列。一旦处理完成，IO 线程会自动地调用回调逻辑完成最后的处理。把这些都做完之后，IO 线程会通知前端主线程说结果已经准备完毕，这样前端主线程能够及时获取到执行操作的结果

后端 IO 线程名字的前缀是 kafka-admin-client-thread。有时候 AdminClient 程序貌似在正常工作，但执行的操作没有返回结果，或者 hang 住了，这可能是因为 IO 线程出现问题导致的。可以使用 jstack 命令去查看一下 AdminClient 程序，确认下 IO 线程是否在正常工作


```java

Properties props = new Properties();
props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-host:port");
props.put("request.timeout.ms", 600000);

try (AdminClient client = AdminClient.create(props)) {
}
```
```java
// 创建主题
String newTopicName = "test-topic";
try (AdminClient client = AdminClient.create(props)) {
        // 主题名称、分区数和副本数
        NewTopic newTopic = new NewTopic(newTopicName, 10, (short) 3);
        CreateTopicsResult result = client.createTopics(Arrays.asList(newTopic));
        result.all().get(10, TimeUnit.SECONDS);
}
```
```java
// 查询消费者组位移
String groupID = "test-group";
try (AdminClient client = AdminClient.create(props)) {
    ListConsumerGroupOffsetsResult result = client.listConsumerGroupOffsets(groupID);
    Map<TopicPartition, OffsetAndMetadata> offsets = 
        result.partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
    System.out.println(offsets);
}
```
```java
// 获取 Broker 磁盘占用
try (AdminClient client = AdminClient.create(props)) {
    DescribeLogDirsResult ret = client.describeLogDirs(Collections.singletonList(targetBrokerId)); // 指定Broker id
    long size = 0L;
    for (Map<String, DescribeLogDirsResponse.LogDirInfo> logDirInfoMap : ret.all().get().values()) {
        size += logDirInfoMap.values().stream().map(logDirInfo -> logDirInfo.replicaInfos).flatMap(
                topicPartitionReplicaInfoMap ->
                    topicPartitionReplicaInfoMap.values().stream().map(replicaInfo -> replicaInfo.size))
                       .mapToLong(Long::longValue).sum();
     }
     System.out.println(size);
}
```

## kafka 监控
### 主机监控
监控 Kafka 集群 Broker 所在的节点机器的性能

### JVM 监控
1. Full GC 发生频率和时长。长时间的停顿会令 Broker 端抛出各种超时异常
2. 活跃对象大小。这个指标是设定堆大小的重要依据
3. 应用线程总数

监控 Broker GC 日志，即以 kafkaServer-gc.log 开头的文件。一旦发现 Broker 进程频繁 Full GC，可以开启 G1 的 -XX:+PrintAdaptiveSizePolicy 开关，让 JVM 告诉你到底是谁引发了 Full GC

### 集群监控
1. 查看 Broker 进程是否启动，端口是否建立
2. 查看 Broker 端关键日志：服务器日志 server.log，控制器日志 controller.log，主题分区状态变更日志 state-change.log
3. 查看 Broker 端关键线程的运行状态。Log Compaction 线程：以 kafka-log-cleaner-thread 开头，一旦它挂掉了，所有 Compaction 操作都会中断；副本拉取消息的线程：以 ReplicaFetcherThread 开头，如果挂掉，则对应的 Follower 副本不再从 Leader 副本拉取消息，因而 Follower 副本的 Lag 会越来越大
4. 查看 Broker 端的关键 JMX 指标。BytesIn/BytesOut：Broker 端每秒入站和出站字节数，确保这组值不要接近网络带宽；NetworkProcessorAvgIdlePercent：网络线程池线程平均的空闲比例，通常来说，应该确保这个 JMX 值长期大于 30%；RequestHandlerAvgIdlePercent：I/O 线程池线程平均的空闲比例，该值应长期小于 30%；UnderReplicatedPartitions：即未充分备份的分区数，即并非所有的 Follower 副本都和 Leader 副本保持同步，表明分区有可能会出现数据丢失；ISRShrink/ISRExpand：即 ISR 收缩和扩容的频次指标；ActiveControllerCount：即当前处于激活状态的控制器的数量。正常情况下，Controller 所在 Broker 上的这个 JMX 指标值应该是 1，其他 Broker 上的这个值是 0。如果发现存在多台 Broker 上该值都是 1 的情况，一定要赶快处理，处理方式主要是查看网络连通性。这种情况通常表明集群出现了脑裂
5. 监控 Kafka 客户端。首先注意的是客户端所在的机器与 Kafka Broker 机器之间的网络往返时延；对于生产者而言，以 kafka-producer-network-thread 开头的线程负责实际消息发送，需要时刻关注；对于消费者而言，以 kafka-coordinator-heartbeat-thread 开头心跳线程也是必须要监控的一个线程。从 Producer 角度，需要关注的 JMX 指标是 request-latency，即消息生产请求的延时。这个 JMX 最直接地表征了 Producer 程序的 TPS；从 Consumer 角度来说，records-lag 和 records-lead 是两个重要的 JMX 指标。它们直接反映了 Consumer 的消费进度。如果使用了 Consumer Group，还需要关注 join rate 和 sync rate，它们说明了 Rebalance 的频繁程度。如果它们的值很高，那么就需要思考下 Rebalance 频繁发生的原因

## 监控工具
### JMXTool
--attributes: 要查询的 jmx 属性名称
--date-format: 日期格式
--jmx-url: jmx 接口，默认格式为 service:jmx:rmi:///jndi/rmi:<JMX 端口>/jmxrmi
--object-name: JMX MBean 名称
--reporting-interval: 实时查询的时间间隔，默认每 2 秒一次

```sh
# 查询 Broker 端每秒入站的流量
kafka-run-class.sh kafka.tools.JmxTool --object-name kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec --jmx-url service:jmx:rmi:///jndi/rmi://:9997/jmxrmi --date-format "YYYY-MM-dd HH:mm:ss" --attributes OneMinuteRate --reporting-interval 1000
```
```sh
# 查看当前激活的 Controller 数量
kafka-run-class.sh kafka.tools.JmxTool --object-name kafka.controller:type=KafkaController,name=ActiveControllerCount --jmx-url service:jmx:rmi:///jndi/rmi://:9997/jmxrmi --date-format "YYYY-MM-dd HH:mm:ss" --reporting-interval 1000
```

### Kafka Manager

### JMXTrans + InfluxDB + Grafana


## 性能调优
### 操作系统层
1. 最好在挂载文件系统时禁掉 atime 更新。atime 的全称是 access time，记录的是文件最后被访问的时间。记录 atime 需要操作系统访问 inode 资源，而禁掉 atime 可以避免 inode 访问时间的写入操作，减少文件系统的写操作数。可以执行 `mount -o noatime` 命令进行设置
2. 文件系统至少选择 ext4 或 XFS。XFS 文件系统具有高性能、高伸缩性等特点，特别适用于生产服务器
3. 建议将 swappiness 设置成一个很小的值，比如 1～10 之间，以防止 Linux 的 OOM Killer 开启随意杀掉进程。可以执行 `sysctl vm.swappiness=N` 来临时设置该值，如果要永久生效，可以修改 /etc/sysctl.conf 文件，增加 vm.swappiness=N，然后重启机器即可
4. 参数 ulimit -n，如果设置太小，会碰到 Too Many File Open 这类的错误
5. 参数 vm.max_map_count，如果设置太小，在一个主题数超多的 Broker 机器上，会碰到 OutOfMemoryError：Map failed 的严重错误，因此建议在生产环境中适当调大此值，比如将其设置为 655360。具体设置方法是修改 /etc/sysctl.conf 文件，增加 vm.max_map_count=655360，保存之后，执行 sysctl -p 命令使它生效
6. 操作系统页缓存大小对 Kafka 而言至关重要。给 Kafka 预留的页缓存越大越好，最小值至少要容纳一个日志段的大小，也就是 Broker 端参数 log.segment.bytes 的值。该参数的默认值是 1GB。预留出一个日志段大小，至少能保证 Kafka 可以将整个日志段全部放入页缓存，这样，消费者程序在消费时能直接命中页缓存，从而避免昂贵的物理磁盘 I/O 操作

### JVM 层
1. 设置堆大小，可以粗略地设置为 6 ～ 8GB。如果想精确调整的话，可以查看 GC log，特别关注 Full GC 之后堆上存活对象的总大小，然后把堆大小设置为该值的 1.5～2 倍。如果你发现 Full GC 没有被执行过，手动运行 `jmap -histo:live <pid>` 就能人为触发 Full GC
2. 建议使用 G1 收集器，比 CMS 收集器的优化难度小。尽力避免 Full GC 的出现，如果你的 Kafka 环境中经常出现 Full GC，可以配置 JVM 参数 -XX:+PrintAdaptiveSizePolicy，来探查一下到底是谁导致的 Full GC。使用 G1 还很容易碰到的一个问题，就是大对象（Large Object），反映在 GC 上的错误，就是 "too many humongous allocations"。所谓的大对象，一般是指至少占用半个区域（Region）大小的对象。如果区域尺寸是 2MB，那么超过 1MB 大小的对象就被视为是大对象。要解决这个问题，除了增加堆大小之外，你还可以适当地增加区域大小，设置方法是增加 JVM 启动参数 -XX:+G1HeapRegionSize=N。默认情况下，如果一个对象超过了 N/2，就会被视为大对象，从而直接被分配在大对象区。如果 Kafka 环境中的消息体都特别大，就很容易出现这种大对象分配的问题

### Broker 端
尽力保持客户端版本和 Broker 端版本一致

### 应用层
1. 不要频繁地创建 Producer 和 Consumer 对象实例
2. 用完及时关闭
3. 合理利用多线程来改善性能


## 调优吞吐量
### broker 端
1. 适当增加 num.replica.fetchers 参数值，但不用超过 cpu 核数
2. 调优 gc 参数以避免经常性出现 full gc

Broker 端参数 num.replica.fetchers 表示的是 Follower 副本用多少个线程来拉取消息，默认使用 1 个线程

### producer 端
1. 适当增加 batch.size 参数值，比如默认的 16kb 到 512kb 或 1mb
2. 适当增加 linger.ms 参数值，比如 10 ~ 100
3. 设置 compression.type=lz4 或者 zstd
4. 设置 acks=0 或 1
5. 设置 retries=0
6. 如果多线程共享同一个 producer 实例，就增加 buffer.memory 参数值

在 Producer 端，增加消息批次的大小以及批次缓存时间，即 batch.size 和 linger.ms，它们的默认值都偏小。由于我们的优化目标是吞吐量，最好不要设置 acks=all 以及开启重试，前者引入的副本同步时间通常都是吞吐量的瓶颈，而后者在执行过程中也会拉低 Producer 应用的吞吐量。如果在多个线程中共享一个 Producer 实例，就可能会碰到缓冲区不够用的情形。倘若频繁地遭遇 TimeoutException：Failed to allocate memory within the configured max blocking time 这样的异常，那么就必须显式地增加 buffer.memory 参数值，确保缓冲区总是有空间可以申请的

### consumer 端
1. 采用多 consumer 进程或者线程同时消费数据
2. 增加 fetch.min.bytes 参数值，比如设置成 1kb 或者更大

可以利用多线程方案增加整体吞吐量，也可以增加 fetch.min.bytes 参数值。默认是 1 字节，表示只要 Kafka Broker 端积攒了 1 字节的数据，就可以返回给 Consumer 端，这实在是太小了，我们可以让 Broker 端一次性多返回点数据吧


## 调优延时
### Broker 端
1. 增加 num.replica.fetchers 值以加快 Follower 副本的拉取速度，减少整个消息处理的延时

### Producer 端
1. 希望消息尽快地被发送出去，因此不要有过多停留，设置 linger.ms=0
2. 不启用压缩。因为压缩操作本身要消耗 CPU 时间，会增加消息发送的延时
3. 不设置 acks=all。Follower 副本同步往往是降低 Producer 端吞吐量和增加延时的首要原因

### Consumer 端
1. 保持 fetch.min.bytes=1 即可，也就是说，只要 Broker 端有能返回的数据，立即令其返回给 Consumer，缩短 Consumer 消费延时


