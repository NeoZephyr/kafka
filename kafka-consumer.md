## 订阅主题和分区
集合方式
```java
consumer.subscribe(Arrays.asList(topic1));
consumer.subscribe(Arrays.asList(topic2));
```
正则表达式方式
```java
consumer.subscribe(Pattern.compile("topic-.*"));
```
直接订阅某些主题的特定分区
```java
consumer.assign(Arrays.asList(new TopicPartition(TOPIC, 0)));
```

集合订阅的方式、正则表达式订阅的方式和指定分区的订阅方式分表代表了三种不同的订阅状态：AUTO_TOPICS、AUTO_PATTERN 和 USER_ASSIGNED（如果没有订阅，那么订阅状态为 NONE）。这三种状态是互斥的，在一个消费者中只能使用其中的一种

通过 subscribe() 方法订阅主题具有消费者自动再均衡的功能，在多个消费者的情况下可以根据分区分配策略来自动分配各个消费者与分区的关系。当消费组内的消费者增加或减少时，分区分配关系会自动调整，以实现消费负载均衡及故障自动转移。而通过 assign() 方法订阅分区时，是不具备消费者自动均衡的功能的


## 取消订阅
既可以取消通过集合方式实现的订阅，也可以取消通过正则表达式方式实现的订阅，还可以取消通过直接订阅方式实现的订阅
```java
consumer.unsubscribe();
consumer.subscribe(new ArrayList<String>());
consumer.assign(new ArrayList<TopicPartition>());
```


## 消费者组
对于消息中间件而言，一般有两种消息投递模式：点对点模式和发布/订阅模式。Kafka 同时支持两种消息投递模式：
1. 如果所有的消费者都隶属于同一个消费组，那么所有的消息都会被均衡地投递给每一个消费者，即每条消息只会被一个消费者处理，相当于点对点模式的应用
2. 如果所有的消费者都隶属于不同的消费组，那么所有的消息都会被广播给所有的消费者，即每条消息会被所有的消费者处理，相当于发布/订阅模式的应用


## 位移
老版本的 Consumer Group 把位移保存在 ZooKeeper 中，不过 ZooKeeper 这类元框架其实并不适合进行频繁的写更新，而 Consumer Group 的位移更新却是一个非常频繁的操作。这种大吞吐量的写操作会极大地拖慢 ZooKeeper 集群的性能，因此在新版本的 Consumer Group 中，Kafka 社区重新设计了 Consumer Group 的位移管理方式，采用了将位移保存在 Kafka 内部主题（__consumer_offsets）的方法

### 位移类型
假设消费者已经消费了分区 x 位置的消息，那么消费者的消费位移为 x，即为 lastConsumedOffset。但是，当前消费者需要提交的消费位移并不是 x，而是 x+1，表示下一条需要拉取的消息的位置

```java
TopicPartition tp = new TopicPartition(topic, 0);
consumer.assign(Arrays.asList(tp));
long lastConsumedOffset = -1;
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(1000);
    if (records.isEmpty()) {
        break;
    }
    List<ConsumerRecord<String, String>> partitionRecords
            = records.records(tp);
    lastConsumedOffset = partitionRecords
            .get(partitionRecords.size() - 1).offset();
    consumer.commitSync();
}

OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
long posititon = consumer.position(tp);

// x
System.out.println("comsumed offset is " + lastConsumedOffset);

// x + 1
System.out.println("commited offset is " + offsetAndMetadata.offset());

// x + 1
System.out.println("the offset of the next record is " + posititon);
```

### 位移主题
新版本 Consumer 的位移管理机制很简单，就是将 Consumer 的位移数据作为一条条普通的 Kafka 消息，提交到位移主题中。因此，位移主题的主要作用是保存 Kafka 消费者的位移信息

位移主题的消息格式有以下 3 种：
1. 位移主题的 Key 中应该保存消费者组 id，主题名，分区号
2. 保存 Consumer Group 信息的消息，主要是用于注册 Consumer Group
3. 用于删除 Group 过期位移甚至是删除 Group 的消息。消息体是 null，即空消息体

当 Kafka 集群中的第一个 Consumer 程序启动时，Kafka 会自动创建位移主题。位移主题的分区数受 Broker 端参数 `offsets.topic.num.partitions` 控制，默认值是 50。位移主题的副本数受 Broker 端另一个参数 `offsets.topic.replication.factor` 控制，默认值是 3

### 位移提交
#### 自动提交位移
Consumer 端有个参数 `enable.auto.commit` 设置为 true，则 Consumer 在后台默默地为你定期提交位移，提交间隔由一个专属的参数 `auto.commit.interval.ms` 来控制，该参数默认值是 5 秒，表明 Kafka 每 5 秒会为你自动提交一次位移。Kafka 会保证在开始调用 `poll()` 方法时，提交上次 `poll()` 返回的所有消息，因此能保证不出现消费丢失的情况，但可能会出现重复消费

自动位移提交的动作是在 `poll()` 方法的逻辑里完成的，在每次真正向服务端发起拉取请求之前会检查是否可以进行位移提交，如果可以，那么就会提交上一次轮询的位移

如果选择自动提交位移，那么只要 Consumer 一直启动着，它就会无限期地向位移主题写入消息。即使该主题没有任何新消息产生，位移主题中仍会不停地写入位移消息。显然 Kafka 只需要保留这类消息中的最新一条就可以了，之前的消息都是可以删除的。这就要求 Kafka 必须要有针对位移主题消息特点的消息删除策略，否则这种消息会越来越多，最终撑爆整个磁盘

Kafka 通过 Compaction 删除位移主题中的过期消息，避免该主题无限期膨胀。对于同一个 Key 的两条消息 M1 和 M2，如果 M1 的发送时间早于 M2，那么 M1 就是过期消息。Compact 的过程就是扫描日志的所有消息，剔除那些过期的消息，然后把剩下的消息整理在一起

Kafka 提供了专门的后台线程定期地巡检待 Compact 的主题，看看是否存在满足条件的可删除数据。这个后台线程叫 Log Cleaner。很多实际生产环境中出现过位移主题无限膨胀占用过多磁盘空间的问题，可能跟 Log Cleaner 线程的状态有关系

#### 手动提交位移
设置 `enable.auto.commit` 为 false，需要手动提交位移

同步提交方法：`commitSync()`，该方法会提交 `poll()` 返回的最新位移。同步提交会一直等待，直到位移被成功提交才会返回。如果提交过程中出现异常，该方法会将异常信息抛出
```java
while (true) {
    ConsumerRecords<String, String> records =
        consumer.poll(Duration.ofSeconds(1));
    process(records); // 处理消息
    try {
        consumer.commitSync();
    } catch (CommitFailedException e) {
        handle(e); // 处理提交失败异常
    }
}
```

异步提交方法：`commitAsync()`，调用该方法会立即返回，不会阻塞，因此不会影响 Consumer 应用的 TPS。由于它是异步的，Kafka 提供了回调函数，可以在提交之后记录日志或处理异常等
```java
while (true) {
    ConsumerRecords<String, String> records = 
        consumer.poll(Duration.ofSeconds(1));
    process(records);
    consumer.commitAsync((offsets, exception) -> {
        if (exception != null)
            handle(exception);
    });
}
```

如果使用 `commitAsync` 提交方法，出现问题时不会自动重试。因为它是异步操作，倘若提交失败后自动重试，那么它重试时提交的位移值可能早已经过期或不是最新值了。因此，异步提交的重试其实没有意义，所以 commitAsync 是不会重试的

对于常规性、阶段性的手动提交，调用 `commitAsync()` 避免程序阻塞，而在 Consumer 要关闭前，我们调用 `commitSync()` 方法执行同步阻塞式的位移提交，以确保 Consumer 关闭前能够保存正确的位移数据
```java
try {
    while(true) {
        ConsumerRecords<String, String> records = 
                    consumer.poll(Duration.ofSeconds(1));
        process(records); // 处理消息
        commitAysnc(); // 使用异步提交规避阻塞
    }
} catch(Exception e) {
    handle(e); // 处理异常
} finally {
    try {
        consumer.commitSync(); // 最后一次提交使用同步阻塞式提交
    } finally {
        consumer.close();
    }
}
```

如果 poll 方法返回很多条消息，我们可以每处理完一部分消息就提交一次位移，这样能够避免大批量的消息重新消费。手动提交提供了这样的方法：`commitSync(Map)` 和 `commitAsync(Map)`。它们的参数是一个 Map 对象，键就是 TopicPartition，即消费的分区，而值是一个 OffsetAndMetadata 对象，保存的主要是位移数据

分批次提交位移
```java
private Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
int count = 0;

while (true) {
    ConsumerRecords<String, String> records =
        consumer.poll(Duration.ofSeconds(1));
    for (ConsumerRecord<String, String> record: records) {
        process(record);  // 处理消息
        offsets.put(new TopicPartition(record.topic(), record.partition()),
                   new OffsetAndMetadata(record.offset() + 1)；
        if（count % 100 == 0）
            consumer.commitAsync(offsets, null); // 回调处理逻辑是null
        count++;
    }
}
```

按分区粒度提交位移
```java
try {
    while (isRunning.get()) {
        ConsumerRecords<String, String> records = consumer.poll(1000);
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionRecords =
                    records.records(partition);
            for (ConsumerRecord<String, String> record : partitionRecords) {
                process(record);
            }
            long lastConsumedOffset = partitionRecords
                    .get(partitionRecords.size() - 1).offset();
            consumer.commitSync(Collections.singletonMap(partition,
                    new OffsetAndMetadata(lastConsumedOffset + 1)));
        }
    }
} finally {
    consumer.close();
}
```

### 控制位移
#### 自动重置
当消费者查找不到所记录的消费位移、位移越界，会根据消费者客户端参数 `auto.offset.reset` 的配置来决定从何处开始进行消费。该参数的默认值为 latest，表示从分区末尾开始消费消息。如果将该参数配置为 earliest，那么消费者会从起始处开始消费；如果设置为 none，就会抛出 `NoOffsetForPartitionException` 异常

#### 手动重置
有些时候，需要从特定的位移处开始拉取消息，可以通过 KafkaConsumer 中的 `seek()` 方法实现
```java
public void seek(TopicPartition partition, long offset);
```

`seek()` 方法只能设置消费者分配到的分区的消费位置，而分区的分配是在 `poll()` 方法的调用过程中实现的。因此，在执行 `seek()` 方法之前需要先执行一次 `poll()` 方法。如果对未分配到的分区执行 `seek()` 方法，那么会报出 `IllegalStateException` 的异常
```java
KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.subscribe(Arrays.asList(topic));
Set<TopicPartition> assignment = new HashSet<>();

// 如果不为 0，则说明已经成功分配到了分区
while (assignment.size() == 0) {
    consumer.poll(Duration.ofMillis(100));

    // 获取消费者所分配到的分区信息
    assignment = consumer.assignment();
}

// 获取指定分区的末尾的消息位置
Map<TopicPartition, Long> offsets = consumer.endOffsets(assignment);
Map<TopicPartition, Long> offsets = consumer.beginningOffsets(assignment);

for (TopicPartition tp : assignment) {
    consumer.seek(tp, 10);
    // consumer.seek(tp, offsets.get(tp));
}
while (true) {
    ConsumerRecords<String, String> records =
            consumer.poll(Duration.ofMillis(1000));
    //consume the record.
}
```

有时候我们并不知道特定的消费位置，只是知道一个相关的时间点，此时可以使用 `offsetsForTimes()` 方法，查询具体时间点对应的分区位置，该方法会返回时间戳大于等于待查询时间的第一条消息对应的位置和时间戳
```java
Map<TopicPartition, Long> timestampToSearch = new HashMap<>();

for (TopicPartition tp : assignment) {
    timestampToSearch.put(tp, System.currentTimeMillis() - 8 * 3600 * 1000);
}

Map<TopicPartition, OffsetAndTimestamp> offsets =
    consumer.offsetsForTimes(timestampToSearch);

for (TopicPartition tp : assignment) {
    OffsetAndTimestamp offsetAndTimestamp = offsets.get(tp);
    if (offsetAndTimestamp != null) {
        consumer.seek(tp, offsetAndTimestamp.offset());
    }
}
```


## 消费控制
暂停某些分区在拉取操作时返回数据给客户端
```java
public void pause(Collection<TopicPartition> partitions);
```
恢复某些分区向客户端返回数据
```java
public void resume(Collection<TopicPartition> partitions);
```
返回被暂停的分区集合
```java
public Set<TopicPartition> paused();
```

`wakeup()` 方法是 KafkaConsumer 中唯一可以从其他线程里安全调用的方法，调用该方法后可以退出 `poll()` 的逻辑，并抛出 `WakeupException` 异常（`WakeupException` 异常只是一种跳出循环的方式，不需要处理）
```java
public void wakeup();
```

跳出循环以后一定要显式地执行关闭动作以释放运行过程中占用的各种系统资源：内存、Socket 连接等
```java
public void close();
```


## Rebalance
Rebalance 就是让一个 Consumer Group 下所有的 Consumer 实例就如何消费订阅主题的所有分区达成共识的过程

### 触发条件
#### 组成员数发生变更
比如有新的 Consumer 实例加入组或者离开组，抑或是有 Consumer 实例崩溃被踢出

#### 订阅主题数发生变更
Consumer Group 可以使用正则表达式的方式订阅主题，比如 `consumer.subscribe(Pattern.compile("t.*c"))`。在 Consumer Group 的运行过程中，当创建一个满足条件的主题后，就会发生 Rebalance

#### 订阅主题的分区数发生变更
Kafka 当前只能允许增加一个主题的分区数。当分区数增加时，就会触发订阅该主题的所有 Group 开启 Rebalance

### Rebalance 流程
重平衡的通知机制是通过心跳线程来完成的。当协调者开启重平衡后，它会将 REBALANCE_IN_PROGRESS 封装进心跳请求的响应中，发还给消费者实例。当消费者实例发现心跳响应中包含了 REBALANCE_IN_PROGRESS，就知道重平衡开始了。消费者端参数 `heartbeat.interval.ms` 表面上是设置了心跳的间隔时间，但实际作用是控制重平衡通知的频率

消费者组状态
1. Empty 状态，组内没有任何成员，但消费者可能存在已提交的位移数据，而且这些位移数据尚未过期
2. Dead 状态，组内没有任何组员，但组的元数据信息已经在协调者端被移除
3. PreparingRebalance 状态，消费者准备开启重平衡，此时所有成员都要重新请求加入消费者组
4. CompletingRebalance 状态，消费者组下所有成员已经加入，各个成员正在等待分配方案
5. Stable 状态，表明重平衡已经完成，组内各成员能够正常消费数据了

消费者组状态流转
1. 消费者组最开始是 Empty 状态
2. 重平衡开启，变为 PreparingRebalance 状态
3. 组成员加入，变为 CompletingRebalance 状态，等待分配方案
4. Leader 完成分配，流转到 Stable 状态，完成重平衡
5. 当有新成员加入或已有成员退出时，消费者组的状态从 Stable 直接跳到 PreparingRebalance 状态，此时，所有现存成员就必须重新申请加入组。如果所有成员都退出组后，消费者组状态变更为 Empty

Kafka 定期自动删除过期位移的条件就是，消费者组要处于 Empty 状态。因此，如果消费者组停掉了很长时间，那么 Kafka 很可能就把该组的位移数据删除了

#### 消费者端
加入组：当组内成员加入组时，它会向协调者发送 JoinGroup 请求。在该请求中，每个成员都要将自己订阅的主题上报，这样协调者就能收集到所有成员的订阅信息。一旦收集了全部成员的 JoinGroup 请求后，协调者会从这些成员中选择一个担任这个消费者组的领导者。通常情况下，第一个发送 JoinGroup 请求的成员自动成为领导者。选出领导者之后，协调者会把消费者组订阅信息封装进 JoinGroup 请求的响应体中，然后发给领导者，由领导者制定具体的分区消费分配方案

领导者消费者分配方案：领导者向协调者发送 SyncGroup 请求，将分配方案发给协调者。值得注意的是，其他成员也会向协调者发送 SyncGroup 请求，只不过请求体中并没有实际的内容。这一步的主要目的是让协调者接收分配方案，然后统一以 SyncGroup 响应的方式分发给所有成员，这样组内所有成员就都知道自己该消费哪些分区了

#### Broker 端
1. 新成员入组
新成员入组是指组处于 Stable 状态后，有成员加入。当协调者收到新的 JoinGroup 请求后，它会通过心跳请求响应的方式通知组内现有的所有成员，强制它们开启新一轮的重平衡

2. 组成员主动离组
消费者实例所在线程或进程调用 `close()` 方法主动通知协调者它要退出。协调者收到 LeaveGroup 请求后，依然会以心跳响应的方式通知其他成员

3. 组成员崩溃离组
崩溃离组是指消费者实例出现严重故障，突然宕机导致的离组。和主动离组不同，协调者通常需要等待一段时间才能感知到，这段时间一般是由消费者端参数 `session.timeout.ms` 控制的

4. 重平衡时协调者对组内成员提交位移的处理
正常情况下，每个组内成员都会定期汇报位移给协调者。当重平衡开启时，协调者会给予成员一段缓冲时间，要求每个成员必须在这段时间内快速地上报自己的位移信息，然后再开启正常的 JoinGroup/SyncGroup 请求发送

### Rebalance 弊端
1. 在 Rebalance 过程中，所有 Consumer 实例都会停止消费，等待 Rebalance 完成，影响 Consumer 端 TPS
2. Rebalance 的设计是所有 Consumer 实例共同参与，全部重新分配所有分区。其实更高效的做法是尽量减少分配方案的变动，这样消费者能够尽量复用之前与 Broker 建立的 TCP 连接
3. Rebalance 很慢

### Coordinator
在 Rebalance 过程中，所有 Consumer 实例共同参与，在协调者组件的帮助下，完成订阅主题分区的分配。其中，协调者负责为 Group 执行 Rebalance 以及提供位移管理和组成员管理等

Consumer 端应用程序在提交位移时，向 Coordinator 所在的 Broker 提交位移。Consumer 应用启动时，向 Coordinator 所在的 Broker 发送各种请求，然后由 Coordinator 负责执行消费者组的注册、成员管理记录等元数据管理操作。所有 Broker 在启动时，都会创建和开启相应的 Coordinator 组件。也就是说，所有 Broker 都有各自的 Coordinator 组件。Kafka 为某个 Consumer Group 确定 Coordinator 所在的 Broker 的算法有以下 2 个步骤：

1. 确定保存该 Group 数据的位移主题分区：`Math.abs(groupId.hashCode() % offsetsTopicPartitionCount)`
2. 找出该分区 Leader 副本所在的 Broker，该 Broker 即为对应的 Coordinator

### 避免 Rebalance
Rebalance 发生的最常见的原因就是 Consumer Group 下的 Consumer 实例数量发生变化。当我们启动一个配置有相同 group.id 值的 Consumer 程序时，实际上就向这个 Group 添加了一个新的 Consumer 实例。此时，Coordinator 会接纳这个新实例，将其加入到组中，并重新分配分区。通常来说，增加 Consumer 实例的操作都是计划内的，可能是出于增加 TPS 或提高伸缩性的需要

我们更关注的是 Group 下实例数减少的情况，即 Consumer 实例被 Coordinator 错误地认为已停止从而被踢出 Group。当 Consumer Group 完成 Rebalance 之后，每个 Consumer 实例都会定期地向 Coordinator 发送心跳请求，表明它还存活着。如果某个 Consumer 实例不能及时地发送这些心跳请求，Coordinator 就会认为该 Consumer 已经死了，从而将其从 Group 中移除，然后开启新一轮 Rebalance

Coordinator 判断 Consumer 的存活状态，受以下几个参数影响：
1. `session.timeout.ms`，默认值是 10 秒，如果 Coordinator 在 10 秒之内没有收到 Group 下某 Consumer 实例的心跳，它就会认为这个 Consumer 实例已经挂了。`session.timout.ms` 决定了 Consumer 存活性的时间间隔
2. `heartbeat.interval.ms`，控制发送心跳请求频率。这个值设置得越小，Consumer 实例发送心跳请求的频率就越高。频繁地发送心跳请求会额外消耗带宽资源，但好处是能够更加快速地知晓当前是否开启 Rebalance。因为，目前 Coordinator 通知各个 Consumer 实例开启 Rebalance 的方法，就是将 REBALANCE_NEEDED 标志封装进心跳请求的响应体中
3. `max.poll.interval.ms`，控制 Consumer 实际消费能力对 Rebalance 的影响。这个参数限定了 Consumer 端应用程序两次调用 poll 方法的最大时间间隔，默认值是 5 分钟，表示 Consumer 如果在 5 分钟之内无法消费完 poll 方法返回的消息，那么 Consumer 会主动发起离开组的请求，Coordinator 也会开启新一轮 Rebalance

避免 Rebalance，主要从以下几个方面考虑：
1. 避免未能及时发送心跳，导致 Consumer 被踢出 Group。仔细地设置 `session.timeout.ms` 和 `heartbeat.interval.ms` 的值。设置 `session.timeout.ms = 6s`，`heartbeat.interval.ms = 2s`。保证 Consumer 实例被踢出之前，能够发送至少 3 轮的心跳请求
2. 避免 Consumer 消费时间过长，将 `max.poll.interval.ms` 设置得大一点，为业务处理逻辑留下充足的时间。这样，Consumer 就不会因为处理这些消息的时间太长而引发 Rebalance 了
3. 如果参数设置合适，却还是出现了 Rebalance，那么可以排查一下 Consumer 端的 GC 表现，比如是否出现了频繁的 Full GC 导致的长时间停顿，从而引发了 Rebalance

### CommitFailedException
Consumer 客户端在提交位移时出现了不可恢复的严重异常，有以下两种原因导致该异常发生：

#### 原因一
当消息处理的总时间超过预设的 `max.poll.interval.ms` 参数值时，Consumer 端会抛出 `CommitFailedException` 异常。要防止这种场景下抛出异常，有以下 4 种方法：
1. 缩短单条消息处理的时间
2. 增加 Consumer 端允许下游系统消费一批消息的最大时长，调整 `max.poll.interval.ms`
3. 减少下游系统一次性消费的消息总数，调整 `max.poll.records`
4. 下游系统使用多线程来加速消费

#### 原因二
消费者组和独立消费者在使用之前都要指定 group.id，如果同时出现了相同 `group.id` 的消费者组程序和独立消费者程序，那么当独立消费者程序手动提交位移时，Kafka 就会立即抛出 `CommitFailedException` 异常

增加期望的时间间隔 max.poll.interval.ms 参数值。减少 poll 方法一次性返回的消息数量，即减少 max.poll.records 参数值

### ConsumerRebalanceListener
调用 `subscribe()` 方法时提及再均衡监听器 `ConsumerRebalanceListener`，该接口包含以下两个方法：
```java
void onPartitionsRevoked(Collection<TopicPartition> partitions);
```
这个方法会在再均衡开始之前和消费者停止读取消息之后被调用。可以通过这个回调方法来处理消费位移的提交，以此来避免一些不必要的重复消费现象的发生。参数 partitions 表示再均衡前所分配到的分区

```java
void onPartitionsAssigned(Collection<TopicPartition> partitions);
```
这个方法会在重新分配分区之后和消费者开始读取消费之前被调用。参数 partitions 表示再均衡后所分配到的分区

```java
Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        consumer.commitSync(currentOffsets);
        currentOffsets.clear();
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
});

try {
    while (isRunning.get()) {
        ConsumerRecords<String, String> records =
            consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {
            //process the record.
            currentOffsets.put(
                    new TopicPartition(record.topic(), record.partition()),
                    new OffsetAndMetadata(record.offset() + 1));
        }
        consumer.commitAsync(currentOffsets, null);
    }
} finally {
    consumer.close();
}
```
再均衡监听器还可以配合外部存储使用：在 `onPartitionsRevoked` 方法中将消费位移保存在数据库中，然后再 `onPartitionsAssigned` 方法中读取位移并通过 `seek` 确定再均衡之后的分区位移


## 反序列化
```java
public class CompanyDeserializer implements Deserializer<Company> {
    public void configure(Map<String, ?> configs, boolean isKey) {}

    public Company deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (data.length < 8) {
            throw new SerializationException("Size of data received " +
                    "by DemoDeserializer is shorter than expected!");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int nameLen, addressLen;
        String name, address;

        nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);
        addressLen = buffer.getInt();
        byte[] addressBytes = new byte[addressLen];
        buffer.get(addressBytes);

        try {
            name = new String(nameBytes, "UTF-8");
            address = new String(addressBytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error occur when deserializing!");
        }

        return new Company(name,address);
    }

    public void close() {}
}
```


## 控制器
主要作用是在 ZooKeeper 的帮助下管理和协调整个 Kafka 集群。集群中任意一台 Broker 都能充当控制器的角色，但是，在运行过程中，只能有一个 Broker 成为控制器，行使其管理和协调的职责。可以通过名为 activeController 的 JMX 指标实时监控控制器的存活状态

Broker 在启动时，会尝试去 ZooKeeper 中创建 /controller 节点。Kafka 当前选举控制器的规则是：第一个成功创建 /controller 节点的 Broker 会被指定为控制器

控制器的职责大致可以分为 5 种
1. 主题管理（创建、删除、增加分区）

2. 分区重分配
分区重分配主要是指对已有主题分区进行细粒度的分配功能

3. Preferred 领导者选举
Kafka 为了避免部分 Broker 负载过重而提供的一种换 Leader 的方案

4. 集群成员管理（新增 Broker、Broker 主动关闭、Broker 宕机）
控制器组件会利用 Watch 机制检查 ZooKeeper 的 /brokers/ids 节点下的子节点数量变更

5. 数据服务
向其他 Broker 提供数据服务。控制器上保存了最全的集群元数据信息，其他所有 Broker 会定期接收控制器发来的元数据更。这里面比较重要的数据有：
所有主题信息，包括具体的分区信息，比如领导者副本，ISR 集合中的副本等；所有 Broker 信息，包括当前运行中的 Broker，正在关闭中的 Broker 等；所有涉及运维任务的分区，包括当前正在进行 Preferred 领导者选举以及分区重分配的分区列表

值得注意的是，这些数据其实在 ZooKeeper 中也保存了一份。每当控制器初始化时，它都会从 ZooKeeper 上读取对应的元数据并填充到自己的缓存中


## 水位
### 高水位
高水位的作用:
1. 定义消息可见性，即标识分区下的哪些消息是可以被消费者消费的
2. 帮助 Kafka 完成副本同步

在分区高水位以下的消息被认为是已提交消息，反之就是未提交消息（位移值等于高水位的消息也属于未提交消息）。消费者只能消费已提交消息

### LEO
日志末端位移（Log End Offset）LEO 表示副本写入下一条消息的位移值。介于高水位和 LEO 之间的消息就属于未提交消息。Kafka 所有副本都有对应的高水位和 LEO 值，Leader 副本的高水位即为分区的高水位

在 Leader 副本所在的 Broker 上，还保存了其他 Follower 副本的 LEO 值。它们的主要作用是，帮助 Leader 副本确定其高水位，也就是分区高水位

### 水位更新
#### Leader 副本
处理生产者请求的逻辑：
1. 写入消息到本地磁盘，更新 LEO 值
2. 获取 Leader 副本所在 Broker 端保存的所有远程副本（与 Leader 副本同步） 的 LEO 值: LEO{0-n}
3. 获取 Leader 副本高水位值：currentHW
4. 更新 currentHW = max(currentHW, min(LEO{0-n}))

处理 Follower 副本拉取消息的逻辑：
1. 读取磁盘（或页缓存）中的消息数据
2. 使用 Follower 副本发送请求中的位移（从该位移开始拉取消息）值更新远程副本 LEO 值
3. 更新分区高水位值（具体步骤与处理生产者请求的步骤相同）

#### Follower 副本
从 Leader 拉取消息的处理逻辑：
1. 写入消息到本地磁盘，更新 LEO 值
2. 获取 Leader 发送的高水位值：currentHW
3. 获取更新过的 LEO 值：currentLEO
4. 更新高水位为 min(currentHW, currentLEO)

### 副本同步
#### 同步条件
1. 远程 Follower 副本在 ISR 中
2. 远程 Follower 副本 LEO 值落后于 Leader 副本 LEO 值的时间，不超过 Broker 端参数 `replica.lag.time.max.ms` 的值。如果使用默认值的话，就是不超过 10 秒

#### 同步示例
T1 时刻
leader: HW = 0, LEO = 0, Remote LEO = 0
follower: HW = 0, LEO = 0

T2 时刻，生产者给主题分区发送一条消息后
leader: HW = 0, LEO = 1, Remote LEO = 0
follower: HW = 0, LEO = 0

T3 时刻，Follower 尝试从 Leader 拉取消息（fetchOffset = 0），Leader 返回当前高水位
leader: HW = 0, LEO = 1, Remote LEO = 0
follower: HW = 0, LEO = 1

T4 时刻，Follower 尝试从 Leader 拉取消息（fetchOffset = 1），Leader 返回当前高水位
leader: HW = 1, LEO = 1, Remote LEO = 1
follower: HW = 1, LEO = 1

### Leader Epoch
Leader 副本高水位更新和 Follower 副本高水位更新在时间上是存在错配的，可能会导致数据丢失或者数据不一致。为避免出现这种情况，引入 Leader Epoch

Leader Epoch 由两部分数据组成
1. Epoch，一个单调增加的版本号。每当副本领导权发生变更时，都会增加该版本号。小版本号的 Leader 被认为是过期 Leader，不能再行使 Leader 权力
2. 起始位移（Start Offset）。Leader 副本在该 Epoch 值上写入的首条消息的位移

Kafka Broker 会在内存中为每个分区都缓存 Leader Epoch 数据，同时定期地将这些信息持久化到一个 checkpoint 文件中。当 Leader 副本写入消息到磁盘时，Broker 会尝试更新这部分缓存。如果该 Leader 是首次写入消息，那么 Broker 会向缓存中增加一个 Leader Epoch 条目，否则就不做更新。这样，每次有 Leader 变更时，新的 Leader 副本会查询这部分缓存，取出对应的 Leader Epoch 的起始位移，以避免数据丢失和不一致的情况


## 消息消费
### 按分区维度消费
```java
ConsumerRecords<String, String> records =
    consumer.poll(Duration.ofMillis(1000));
for (TopicPartition tp : records.partitions()) {
    for (ConsumerRecord<String, String> record : records.records(tp)) {
        System.out.println(record.partition() + " : " + record.value());
    }
}
```

### 按主题维度消费
```java
ConsumerRecords<String, String> records =
    consumer.poll(Duration.ofMillis(1000));
for (String topic : topicList) {
    for (ConsumerRecord<String, String> record : 
            records.records(topic)) {
        System.out.println(record.topic() + " : " + record.value());
    }
}
```

### 多线程消费
鉴于 KafkaConsumer 不是线程安全的，有以下两套多线程方案：
1. 每个线程维护专属的 KafkaConsumer 实例，负责完整的消息获取、消息处理流程

使用该方案有以下优点：
实现起来简单，多个线程之间没有任何交互，省去了很多保障线程安全方面的开销；由于每个线程使用专属的 KafkaConsumer 实例来执行消息获取和消息处理逻辑，Kafka 主题中的每个分区都能保证只被一个线程处理，很容易实现分区内的消息消费顺序

该方案的缺点如下：
每个线程都维护自己的 KafkaConsumer 实例会占用更多的系统资源，比如内存、TCP 连接等；能使用的线程数受限于 Consumer 订阅主题的总分区数；每个线程完整地执行消息获取和消息处理逻辑，一旦消息处理逻辑很重，造成消息处理速度慢，就很容易出现不必要的 Rebalance，从而引发整个消费者组的消费停滞

```java

public class KafkaConsumerRunner implements Runnable {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConsumer consumer;

    public void run() {
        try {
            consumer.subscribe(Arrays.asList("topic"));
            while (!closed.get()) {
                ConsumerRecords records =
                    consumer.poll(Duration.ofMillis(10000));
                //  执行消息处理逻辑
            }
        } catch (WakeupException e) {    
            // Ignore exception if closing
            if (!closed.get())
                throw e;
        } finally {
            consumer.close();
        }
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}
```

2. 消费者程序使用单或多线程获取消息，同时创建多个消费线程执行消息处理逻辑

该方案最大优势就在于它的高伸缩性，我们可以独立地调节消息获取的线程数，以及消息处理的线程数，而不必考虑两者之间是否相互影响。如果你的消费获取速度慢，就增加消费获取的线程；如果消息的处理速度慢，就增加 Worker 线程池线程

该方案的缺点如下：
实现难度大，需要管理两组线程；由于将消息获取和消息处理分开了，无法保证分区内的消费顺序；引入了多组线程，使得整个消息消费链路被拉长，最终导致正确位移提交会变得异常困难，结果就是可能会出现消息的重复消费

```java
private final KafkaConsumer<String, String> consumer;
private ExecutorService executors;

private int workerNum = 5;
executors = new ThreadPoolExecutor(
  workerNum, workerNum, 0L, TimeUnit.MILLISECONDS,
  new ArrayBlockingQueue<>(1000), 
  new ThreadPoolExecutor.CallerRunsPolicy());

while (true)  {
    ConsumerRecords<String, String> records = 
        consumer.poll(Duration.ofSeconds(1));
    for (final ConsumerRecord record : records) {
        executors.submit(new Worker(record));
    }
}
```


## 消费进度
消费者 Lag 表示消费的滞后程度。如果一个消费者 Lag 值很大，表示消费速度无法跟上生产速度。这有可能导致要消费的数据已经不在操作系统的页缓存中了，而这些数据就会失去享有 Zero Copy 技术的资格，以至于消费者就不得不从磁盘上读取数据，从而进一步拉大消费与生产的差距，导致那些 Lag 原本就很大的消费者会越来越慢，Lag 也会越来越大

监控消费进度
1. kafka-consumer-groups 脚本监控消费者消费进度

查看某个给定消费者的 Lag 值
```sh
kafka-consumer-groups.sh --bootstrap-server <broker> --describe --group <group>
```

2. API 查询当前分区最新消息位移和消费者组最新消费消息位移
```java
public static Map<TopicPartition, Long> lagOf(String groupID, String bootstrapServers) throws TimeoutException {
    Properties props = new Properties();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    try (AdminClient client = AdminClient.create(props)) {

        // 获取给定消费者组的最新消费消息的位移
        ListConsumerGroupOffsetsResult result = client.listConsumerGroupOffsets(groupID);
        try {
            Map<TopicPartition, OffsetAndMetadata> consumedOffsets = result.partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

                // 获取订阅分区的最新消息位移
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumedOffsets.keySet());
                return endOffsets.entrySet().stream().collect(
                        Collectors.toMap(
                            entry -> entry.getKey(),
                            entry -> entry.getValue() - consumedOffsets.get(entry.getKey()).offset()
                        ));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // 处理中断异常
            return Collections.emptyMap();
        } catch (ExecutionException e) {
            // 处理ExecutionException
            return Collections.emptyMap();
        } catch (TimeoutException e) {
            throw new TimeoutException("Timed out when getting lag for consumer group " + groupID);
        }
    }
}
```

3. Kafka JMX 监控指标
Kafka 消费者提供了一个名为 kafka.consumer:type=consumer-fetch-manager-metrics,client-id="{client-id}" 的 JMX 指标。其中有两组属性：records-lag-max 和 records-lead-min，分别表示此消费者在测试窗口时间内曾经达到的最大的 Lag 值和最小的 Lead 值。其中，Lead 值是指消费者最新消费消息的位移与分区当前第一条消息位移的差值。显然，Lag 越大，Lead 就越小，反之同理


## TCP 连接
在调用 poll 方法时被创建的，而 poll 方法内部有 3 个时机可以创建 TCP 连接

### 创建连接
1. 发起 FindCoordinator 请求
当消费者程序首次启动调用 poll 方法时，它需要向 Kafka 集群（集群中的任意服务器）发送一个名为 FindCoordinator 的请求，以获取对应的协调者（驻留在 Broker 端的内存中，负责消费者组的组成员管理和各个消费者的位移提交管理）。然后，消费者会创建一个 Socket 连接

2. 连接协调者
Broker 处理完 FindCoordinator 请求之后，会返还对应的响应结果，告诉消费者哪个 Broker 是真正的协调者，因此，消费者知晓了真正的协调者后，会创建连向该 Broker 的 Socket 连接。只有成功连入协调者，协调者才能开启正常的组协调操作，比如加入组、等待组分配方案、心跳请求处理、位移获取、位移提交等

3. 消费数据
消费者会为每个要消费的分区创建与该分区领导者副本所在 Broker 连接的 TCP

需要注意的是，当第三类 TCP 连接成功创建后，消费者程序就会废弃第一类 TCP 连接，之后在定期请求元数据时，它会改为使用第三类 TCP 连接

### 关闭连接
1. 主动关闭
手动调用 KafkaConsumer.close() 方法，或者是执行 Kill 命令

2. 自动关闭
由消费者端参数 `connection.max.idle.ms` 控制，该参数默认值是 9 分钟，即如果某个 Socket 连接上连续 9 分钟都没有任何请求，那么消费者会强行杀掉这个 Socket 连接


### 拦截器
消费者拦截器包含以下方法：
```java
public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records);
```
消费者会在 `poll()` 方法返回之前调用拦截器的 `onConsume()` 方法来对消息进行相应的定制化操作，比如修改返回的消息内容、按照某种规则过滤消息。如果该方法中抛出异常，那么会被捕获并记录到日志中，但是异常不会再向上传递

```java
public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets);
```
消费者会在提交完消费位移之后调用拦截器的 `onCommit()` 方法，可以使用这个方法来记录跟踪所提交的位移信息

在某些业务场景中会对消息设置一个有效期的属性，如果某条消息在既定的时间窗口内无法到达，那么就会被视为无效，它也就不需要再被继续处理了
```java
public class ConsumerInterceptorTTL implements 
    ConsumerInterceptor<String, String> {
    
    private static final long EXPIRE_INTERVAL = 10 * 1000;

    public ConsumerRecords<String, String> onConsume(
        ConsumerRecords<String, String> records) {
        long now = System.currentTimeMillis();
        Map<TopicPartition, List<ConsumerRecord<String, String>>> newRecords 
            = new HashMap<>();
        for (TopicPartition tp : records.partitions()) {
            List<ConsumerRecord<String, String>> tpRecords = 
            records.records(tp);
            List<ConsumerRecord<String, String>> newTpRecords = new ArrayList<>();
            for (ConsumerRecord<String, String> record : tpRecords) {
                if (now - record.timestamp() < EXPIRE_INTERVAL) {
                    newTpRecords.add(record);
                }
            }
            if (!newTpRecords.isEmpty()) {
                newRecords.put(tp, newTpRecords);
            }
        }
        return new ConsumerRecords<>(newRecords);
    }

    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((tp, offset) -> 
                System.out.println(tp + ":" + offset.offset()));
    }

    public void close() {}

    public void configure(Map<String, ?> configs) {}
}
```

```java
props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
    ConsumerInterceptorTTL.class.getName());
```

不过使用这种功能时需要注意的是：在一次消息拉取的批次中，可能含有最大偏移量的消息会被消费者拦截器过滤

在消费者中也有拦截链的概念，和生产者的拦截链一样，也是按照 `interceptor.classes` 参数配置的拦截器的顺序来一一执行的（配置的时候，各个拦截器之间使用逗号隔开）。同样也要提防副作用的发生。如果在拦截链中某个拦截器执行失败，那么下一个拦截器会接着从上一个执行成功的拦截器继续执行
