## 生产者原理
生产者客户端由两个线程协调运行，这两个线程分别为主线程和 Sender 线程

### TCP 连接
#### 创建
1. KafkaProducer 实例创建时启动 Sender 线程，从而与 `bootstrap.servers` 中所有 Broker 建立 TCP 连接。Producer 一旦连接到集群中的任一台 Broker，就能拿到整个集群的 Broker 信息

2. KafkaProducer 实例首次更新元数据信息之后，如果发现与某些 Broker 没有连接，那么它就会创建一个 TCP 连接

3. 如果 Producer 端发送消息到某台 Broker 时发现没有与该 Broker 的 TCP 连接，那么也会立即创建连接

#### 关闭
用户主动关闭：调用 `producer.close()` 方法关闭
Kafka 自动关闭：如果设置 Producer 端 `connections.max.idle.ms` 参数大于 0，则与 `bootstrap.servers` 中 Broker 建立的 TCP 连接会被自动关闭；如果设置该参数为 -1，那么与 `bootstrap.servers` 中 Broker 建立的 TCP 连接将成为永久长连接，从而成为僵尸连接。`connections.max.idle.ms` 参数默认为 9 分钟

Kafka 自动关闭，属于被动关闭的场景，会产生大量的 CLOSE_WAIT 连接，因此 Producer 端没有机会显式地观测到此连接已被中断

### 主线程
在主线程中由 KafkaProducer 创建消息，然后通过拦截器、序列化器和分区器的作用之后缓存到消息累加器（RecordAccumulator，也称为消息收集器）中

### RecordAccumulator
在 RecordAccumulator 的内部为每个分区都维护了一个双端队列，用来缓存消息以便 Sender 线程可以批量发送，进而减少网络传输的资源消耗以提升性能。当消息写入缓存时，追加到对应分区的双端队列的尾部

队列中的内容就是 ProducerBatch，ProducerBatch 中可以包含一至多个 ProducerRecord，这样可以使字节的使用更加紧凑，与此同时，将较小的 ProducerRecord 拼凑成一个较大的 ProducerBatch，也可以减少网络请求的次数以提升整体的吞吐量。如果生产者客户端需要向很多分区发送消息，则可以将 `buffer.memory` 参数适当调大缓存大小以增加整体的吞吐量，该参数默认值为 32MB

如果生产者发送消息的速度超过发送到服务器的速度，则会导致生产者空间不足，这个时候 KafkaProducer 的 send() 方法调用要么被阻塞，要么抛出异常，这个取决于参数 `max.block.ms` 的配置，此参数的默认值为 60 秒

### BufferPool
在 Kafka 生产者客户端中，通过 `java.io.ByteBuffer` 实现消息内存的创建和释放。在 RecordAccumulator 的内部还有一个 BufferPool，它主要用来实现 ByteBuffer 的复用，以实现缓存的高效利用。不过 BufferPool 只针对特定大小的 ByteBuffer 进行管理，而其他大小的 ByteBuffer 不会缓存进 BufferPool 中，这个特定的大小由 `batch.size` 参数来指定，默认值为 16KB。可以适当地调大 `batch.size` 参数以便多缓存一些消息

当一条消息（ProducerRecord）流入 RecordAccumulator 时，会先寻找与消息分区所对应的双端队列（如果没有则新建），再从这个双端队列的尾部获取一个 ProducerBatch（如果没有则新建），查看 ProducerBatch 中是否还可以写入这个 ProducerRecord，如果可以则写入，如果不可以则需要创建一个新的 ProducerBatch。在新建 ProducerBatch 时评估这条消息的大小是否超过 `batch.size` 参数的大小，如果不超过，那么就以 batch.size 参数的大小来创建 ProducerBatch，这样在使用完这段内存区域之后，可以通过 BufferPool 的管理来进行复用；如果超过，那么就以评估的大小来创建 ProducerBatch，这段内存区域不会被复用

### Sender 线程
Sender 线程负责从 RecordAccumulator 中的双端队列头部读取消息并将其发送到 Kafka 中

Sender 从 RecordAccumulator 中获取缓存的消息之后，会进一步将原本 `<分区, Deque< ProducerBatch>>` 的保存形式转变成 `<Node, List< ProducerBatch>>` 的形式。这是因为，生产者客户端只向具体的 broker 节点发送消息，而并不关心消息属于哪一个分区；而对于 KafkaProducer 的应用逻辑而言，只关注向哪个分区中发送哪些消息

Sender 最终会将消息封装成 `<Node, Request>` 的形式，从而将 Request 请求发往各个 Node，这里的 Request 是指 Kafka 的各种协议请求，对于消息发送而言就是指具体的 ProduceRequest

### InFlightRequests
请求在从 Sender 线程发往 Kafka 之前还会保存到 InFlightRequests 中，InFlightRequests 保存对象的具体形式为 `Map<NodeId, Deque>`，缓存了已经发出去但还没有收到响应的请求（NodeId 是一个 String 类型，表示节点的 id 编号）。可以通过配置参数 `max.in.flight.requests.per.connection` 限制每个连接（也就是客户端与 Node 之间的连接）最多缓存的请求数，默认值为 5，即每个连接最多只能缓存 5 个未响应的请求，超过该数值之后就不能再向这个连接发送更多的请求了，除非有缓存的请求收到了响应

通过 InFlightRequests 还可以获得 leastLoadedNode（负载最小 Node），也就是在 InFlightRequests 中还未确认请求最少的 Node。选择 leastLoadedNode 发送请求可以使它能够尽快发出，避免因网络拥塞等异常而影响整体的进度。leastLoadedNode 的概念可以用于多个应用场合，比如元数据请求、消费者组播协议的交互。

### 元数据更新
元数据是指 Kafka 集群的元数据，这些元数据具体记录了集群中的主题，主题拥有的分区，每个分区的 leader 副本所在的节点，follower 副本所在的节点，AR、ISR 等集合中的副本，集群中的节点，控制器节点等信息

元数据更新场景：
1. 当 Producer 尝试给一个不存在的主题发送消息时，Broker 会告诉 Producer 说这个主题不存在。此时 Producer 会发送 METADATA 请求给 Kafka 集群，去尝试获取最新的元数据信息
2. 当超过 `metadata.max.age.ms` 时间没有更新元数据，不论集群那边是否有变化，Producer 都会发送 METADATA 请求给 Kafka 集群，强制刷新一次元数据以保证它是最及时的数据。参数`metadata.max.age.ms` 的默认值为 5 分钟

当需要更新元数据时，会先挑选出 leastLoadedNode，然后向这个 Node 发送 MetadataRequest 请求来获取具体的元数据信息。这个更新操作是由 Sender 线程发起的，在创建完 MetadataRequest 之后同样会存入 InFlightRequests，之后的步骤就和发送消息时的类似。元数据虽然由 Sender 线程负责更新，但是主线程也需要读取这些信息，这里的数据同步通过 synchronized 和 final 关键字来保障

## 序列化
生产者使用的序列化器和消费者使用的反序列化器是需要一一对应的

```java
public class StringSerializer implements Serializer<String> {
    private String encoding = "UTF8";

    // 该方法在创建 KafkaProducer 实例的时候调用的，主要用来确定编码类型
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null)
            encodingValue = configs.get("serializer.encoding");
        if (encodingValue instanceof String)
            encoding = (String) encodingValue;
    }

    @Override
    public byte[] serialize(String topic, String data) {
        try {
            if (data == null)
                return null;
            else
                return data.getBytes(encoding);
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when serializing string to byte[] due to unsupported encoding " + encoding);
        }
    }
}
```

自定义序列化器
```java
public class CompanySerializer implements Serializer<Company> {
    @Override
    public void configure(Map configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, Company data) {
        if (data == null) {
            return null;
        }
        byte[] name, address;
        try {
            if (data.getName() != null) {
                name = data.getName().getBytes("UTF-8");
            } else {
                name = new byte[0];
            }
            if (data.getAddress() != null) {
                address = data.getAddress().getBytes("UTF-8");
            } else {
                address = new byte[0];
            }
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + name.length + address.length);
            buffer.putInt(name.length);
            buffer.put(name);
            buffer.putInt(address.length);
            buffer.put(address);
            return buffer.array();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public void close() {}
}
```

## 分区
### 分区器
如果消息 ProducerRecord 中没有指定 partition 字段，就需要依赖分区器，根据 key 这个字段来计算 partition 的值。默认分区器是 `org.apache.kafka.clients.producer.internals.DefaultPartitioner`，它实现了 `org.apache.kafka.clients.producer.Partitioner` 接口

在默认分区器 `DefaultPartitioner` 的实现中，如果 key 不为 null，那么默认的分区器会对 key 进行哈希，最终根据得到的哈希值来计算分区号，拥有相同 key 的消息会被写入同一个分区。如果 key 为 null，那么消息将会以轮询的方式发往主题内的各个可用分区

### 分区策略
#### 轮询策略
Kafka Java 生产者 API 默认提供的分区策略。轮询策略有非常优秀的负载均衡表现，它总是能保证消息最大限度地被平均分配到所有分区上，故默认情况下它是最合理的分区策略

#### 随机策略
```java
List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
return ThreadLocalRandom.current().nextInt(partitions.size());
```

#### 按消息键保序策略
Kafka 默认分区策略同时实现了两种策略：如果指定了 Key，那么默认实现按消息键保序策略；如果没有指定 Key，则使用轮询策略

## 拦截器
生产者拦截器需要实现 `org.apache.kafka.clients.producer.ProducerInterceptor` 接口

KafkaProducer 在将消息序列化和计算分区之前会调用生产者拦截器的 onSend() 方法来对消息进行相应的定制化操作。一般来说最好不要修改消息 ProducerRecord 的 topic、key 和 partition 等信息，如果要修改，则需确保对其有准确的判断，否则会与预想的效果出现偏差。比如修改 key 不仅会影响分区的计算，同样会影响 broker 端日志压缩（Log Compaction）的功能。

KafkaProducer 会在消息被应答（Acknowledgement）之前或消息发送失败时调用生产者拦截器的 onAcknowledgement() 方法，优先于用户设定的 Callback 之前执行。这个方法运行在 Producer 的 I/O 线程中，所以这个方法中实现的代码逻辑越简单越好，否则会影响消息的发送速度

close() 方法主要用于在关闭拦截器时执行一些资源的清理工作。在这 3 个方法中抛出的异常都会被捕获并记录到日志中，但并不会再向上传递

KafkaProducer 中可以指定多个拦截器以形成拦截链。拦截链会按照配置的顺序来一一执行（配置的时候，各个拦截器之间使用逗号隔开）

如果拦截链中的某个拦截器的执行需要依赖于前一个拦截器的输出，那么当前一个拦截器由于异常而执行失败，那么这个拦截器也就跟着无法继续执行。在拦截链中，如果某个拦截器执行失败，那么下一个拦截器会接着从上一个执行成功的拦截器继续执行

## 压缩
在 Kafka 中，压缩可能发生在两个地方：生产者端和 Broker 端

生产者程序中配置 compression.type 参数即表示启用指定类型的压缩算法
```java
// 开启GZIP压缩
properties.put("compression.type", "gzip");
```
表明该 Producer 的压缩算法使用的是 GZIP。这样 Producer 启动后生产的每个消息集合都是经 GZIP 压缩过的，故而能很好地节省网络传输带宽以及 Kafka Broker 端的磁盘占用

其实大部分情况下 Broker 从 Producer 端接收到消息后仅仅是原封不动地保存而不会对其进行任何修改，有两种例外情况就可能让 Broker 重新压缩消息：
1. Broker 端指定了和 Producer 端不同的压缩算法
2. Broker 端发生了消息格式转换

Kafka 将启用的压缩算法封装进消息集合中，这样当 Consumer 读取到消息集合时解压缩还原成之前的消息

除了在 Consumer 端解压缩，每个压缩过的消息集合在 Broker 端写入时都要发生解压缩操作，目的就是为了对消息执行各种验证。这种解压缩对 Broker 端性能是有一定影响的，特别是对 CPU 的使用率而言

## 幂等性
```java
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
```

`enable.idempotence` 被设置成 true 后，Producer 自动升级成幂等性 Producer，Kafka 自动进行消息去重。底层具体的原理很简单：就是在 Broker 端多保存一些字段。当 Producer 发送了具有相同字段值的消息后，Broker 能够判断消息是否已经重复，并将重复消息丢弃掉

幂等性具有以下局限：
1. 只能保证单分区上的幂等性，即一个幂等性 Producer 能够保证某个主题的一个分区上不出现重复消息，它无法实现多个分区的幂等性
2. 只能实现单会话上的幂等性，不能实现跨会话的幂等性。这里的会话，可以理解为 Producer 进程的一次运行。当重启 Producer 进程之后，这种幂等性保证就丧失了

## 事务
事务型 Producer 能够保证将消息原子性地写入到多个分区中。这批消息要么全部写入成功，要么全部失败。另外，Producer 重启后，Kafka 依然保证发送消息的精确一次处理

设置事务型 Producer
1. 开启 `enable.idempotence = true`
2. 设置 Producer 端参数 transactional.id 最好为其设置一个有意义的名字

```java
producer.initTransactions();
try {
    producer.beginTransaction();
    producer.send(record1);
    producer.send(record2);
    producer.commitTransaction();
} catch (KafkaException e) {
    producer.abortTransaction();
}
```

这段代码能够保证 Record1 和 Record2 要么全部提交成功，要么全部写入失败。即使写入失败，Kafka 也会把它们写入到底层的日志中，也就是说 Consumer 还是会看到这些消息。因此在 Consumer 端，读取事务型 Producer 发送的消息也是需要一些变更的：设置 `isolation.level` 参数的值即可。这个参数有两个取值：
1. read_uncommitted：这是默认值，表明 Consumer 能够读取到 Kafka 写入的任何消息，不论事务型 Producer 提交事务还是终止事务，其写入的消息都可以读取
2. read_committed：表明 Consumer 只会读取事务型 Producer 成功提交事务写入的消息。当然它也能看到非事务型 Producer 写入的所有消息

## 异步发送
异步发送的方式，一般是在 `send()` 方法里指定一个 `Callback` 的回调函数，Kafka 在返回响应时调用该函数来实现异步的发送确认

```java
producer.send(record1, callback1);
producer.send(record2, callback2);
```
对于同一个分区而言，如果消息 record1 于 record2 之前先发送，那么 KafkaProducer 就可以保证对应的 callback1 在 callback2 之前调用，也就是说，回调函数的调用也可以保证分区有序

## 关闭客户端
`close()` 方法会阻塞等待之前所有的发送请求完成后再关闭 KafkaProducer。与此同时，KafkaProducer 还提供了一个带超时时间的 `close()` 方法。如果调用了带超时时间 timeout 的 `close()` 方法，那么只会在等待 timeout 时间内来完成所有尚未完成的请求处理，然后强行退出。在实际应用中，一般使用的都是无参的 `close()` 方法

## 异常
### 可重试的异常
常见的可重试异常有：`NetworkException`、`LeaderNotAvailableException`、`UnknownTopicOrPartitionException`、`NotEnoughReplicasException`、`NotCoordinatorException` 等

对于可重试的异常，如果配置了 retries 参数，那么只要在规定的重试次数内自行恢复了，就不会抛出异常。retries 参数的默认值为 0，配置方式参考如下：
```java
props.put(ProducerConfig.RETRIES_CONFIG, 3);
```
如果重试了 3 次之后还没有恢复，那么仍会抛出异常，交给外层处理这些异常

### 不可重试的异常
例如 `RecordTooLargeException` 异常，表示所发送的消息太大，`KafkaProducer` 对此不会进行任何重试，直接抛出异常
