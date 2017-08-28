package org.apache.kafka.clients.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.ExtendedSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.MockMetricsReporter;
import org.apache.kafka.test.MockPartitioner;
import org.apache.kafka.test.MockProducerInterceptor;
import org.apache.kafka.test.MockSerializer;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.core.classloader.annotations.PrepareOnlyThisForTest;
import org.powermock.modules.junit4.PowerMockRunner;

//@RunWith(PowerMockRunner.class)
//@PowerMockIgnore("javax.management.*")
public class KafkaProducerTest {

	@Test
	public void test() {
		fail("Not yet implemented");
	}

	/**
	 * 带有Serializer的构造方法初始化KafkaProducer
	 */
    @Test
    public void testConstructorWithSerializers() {
        Properties producerProps = new Properties();
        /**
         * bootstrap.servers
         * 用于建立与kafka集群连接的host/port组。数据将会在所有servers上均衡加载，不管哪些server是指定用于bootstrapping。
         * 这个列表仅仅影响初始化的hosts（用于发现全部的servers）。这个列表格式：host1:port1,host2:port2,…因为这些server仅仅是用于初始化的连接，
         * 以发现集群所有成员关系（可能会动态的变化），这个列表不需要包含所有的servers（你可能想要不止一个server，尽管这样，可能某个server宕机了）。
         * 如果没有server在这个列表出现，则发送数据会一直失败，直到列表可用。
         */
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        new KafkaProducer<>(producerProps, new ByteArraySerializer(), new ByteArraySerializer()).close();
    }
    
/**
 * 不带有Serializer的构造方法初始化KafkaProducer
 */
   @Test(expected = ConfigException.class)
   public void testNoSerializerProvided() {
       Properties producerProps = new Properties();
       producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
       new KafkaProducer(producerProps);
   }

   /**
    * 度量资源初始化和关闭的情况
    */
   @Test
   public void testConstructorFailureCloseResource() {
       Properties props = new Properties();
       /**
        * client.id
        * 当向server发出请求时，这个字符串会发送给server。目的是能够追踪请求源头，以此来允许ip/port许可列表之外的一些应用可以发送信息。
        * 这项应用可以设置任意字符串，因为没有任何功能性的目的，除了记录和跟踪
        */
       props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "testConstructorClose");
       props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "some.invalid.hostname.foo.bar.local:9999");
       /**
        * metric.reporters
        * 类的列表，用于衡量指标。实现MetricReporter接口，将允许增加一些类，这些类在新的衡量指标产生时就会改变。JmxReporter总会包含用于注册JMX统计
        * 使用方法参考KafkaProducerTest.testConstructorFailureCloseResource
        */
       props.setProperty(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());

       final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();
       final int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();
       try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
           fail("should have caught an exception and returned");
       } catch (KafkaException e) {
           assertEquals(oldInitCount + 1, MockMetricsReporter.INIT_COUNT.get());
           assertEquals(oldCloseCount + 1, MockMetricsReporter.CLOSE_COUNT.get());
           assertEquals("Failed to construct kafka producer", e.getMessage());
       }
   }

   /**
    * 度量资源初始化及关闭情况
    * @throws Exception
    */
   @Test
   public void testSerializerClose() throws Exception {
       Map<String, Object> configs = new HashMap<>();
       configs.put(ProducerConfig.CLIENT_ID_CONFIG, "testConstructorClose");
       configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
       configs.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
       /**
        * security.protocol
        * PLAINTEXT
        */
       configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL);
       final int oldInitCount = MockSerializer.INIT_COUNT.get();
       final int oldCloseCount = MockSerializer.CLOSE_COUNT.get();

       /**
        * 在KafkaProducer 的构造方法中   List<MetricsReporter> reporters = config.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
        * MetricsReporter.class);使MockMetricsReporter的configure 被调用，接下来几个测试方法中针对configure的统计也是类似情况
        */
       KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(
               configs, new MockSerializer(), new MockSerializer());
       assertEquals(oldInitCount + 2, MockSerializer.INIT_COUNT.get());
       assertEquals(oldCloseCount, MockSerializer.CLOSE_COUNT.get());

       producer.close();
       assertEquals(oldInitCount + 2, MockSerializer.INIT_COUNT.get());
       assertEquals(oldCloseCount + 2, MockSerializer.CLOSE_COUNT.get());
   }

   /**
            * 　Producer拦截器(interceptor)是个相当新的功能，它和consumer端interceptor是在Kafka 0.10版本被引入的，主要用于实现clients端的定制化控制逻辑。
   　　  * 对于producer而言，interceptor使得用户在消息发送前以及producer回调逻辑前有机会对消息做一些定制化需求，比如修改消息等。同时，producer允许用户指定多个
   	 * interceptor按序作用于同一条消息从而形成一个拦截链(interceptor chain)。Intercetpor的实现接口是org.apache.kafka.clients.producer.ProducerInterceptor，其定义的方法包括：
   　　  * onSend(ProducerRecord)：该方法封装进KafkaProducer.send方法中，即它运行在用户主线程中的。Producer确保在消息被序列化以计算分区前调用该方法。用户可以在该方法中对消息做任何操作，
   	 * 但最好保证不要修改消息所属的topic和分区，否则会影响目标分区的计算
   　　  * onAcknowledgement(RecordMetadata, Exception)：该方法会在消息被应答之前或消息发送失败时调用，并且通常都是在producer回调逻辑触发之前。onAcknowledgement运行在producer的IO线程中，
   	 * 因此不要在该方法中放入很重的逻辑，否则会拖慢producer的消息发送效率
   　　  * close：关闭interceptor，主要用于执行一些资源清理工作
   　　  * 如前所述，interceptor可能被运行在多个线程中，因此在具体实现时用户需要自行确保线程安全。另外倘若指定了多个interceptor，则producer将按照指定顺序调用它们，并仅仅是捕获每个interceptor
   	 * 可能抛出的异常记录到错误日志中而非在向上传递。这在使用过程中要特别留意。
   　　  * 本文实现一个简单的双interceptor组成的拦截链。第一个interceptor会在消息发送前将时间戳信息加到消息value的最前部；第二个interceptor会在消息发送后更新成功发送消息数或失败发送消息数。这两个interceptor实现的逻辑其实都很简单，特别是第一个interceptor做的事情实际上并无太多的实际意义，只是为了演示如何使用interceptor以及连接链。
        * @throws Exception
        */
   @Test
   public void testInterceptorConstructClose() throws Exception {
       try {
           Properties props = new Properties();
           // test with client ID assigned by KafkaProducer
           props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
           props.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MockProducerInterceptor.class.getName());
           props.setProperty(MockProducerInterceptor.APPEND_STRING_PROP, "something");

           KafkaProducer<String, String> producer = new KafkaProducer<String, String>(
                   props, new StringSerializer(), new StringSerializer());
           
           ProducerRecord<String, String> record =  new ProducerRecord<>("tsuperviseprivate",  "supervise_add_org", "test message");
           producer.send(record);
           
           assertEquals(1, MockProducerInterceptor.INIT_COUNT.get());
           assertEquals(0, MockProducerInterceptor.CLOSE_COUNT.get());

           // Cluster metadata will only be updated on calling onSend.
           Assert.assertNull(MockProducerInterceptor.CLUSTER_META.get());

           producer.close();
           assertEquals(1, MockProducerInterceptor.INIT_COUNT.get());
           assertEquals(1, MockProducerInterceptor.CLOSE_COUNT.get());
       } finally {
           // cleanup since we are using mutable static variables in MockProducerInterceptor
           MockProducerInterceptor.resetCounters();
       }
   }
   
   
   /**
    * 发送消息时的消息路由(分区策略)
    * 分区的策略
	* 我在producer中没有定义分区策略，也就是说程序采用默认的kafka.producer.DefaultPartitioner，来看看源码中是怎么定义的：
	*  默认为kafka.producer.DefaultPartitioner，取模其核心思想就是对每个消息的key的hash值对partition数取模得到
	* partitioner.class = kafka.producer.DefaultPartitioner
    * @throws Exception
    */
   @Test
   public void testPartitionerClose() throws Exception {
       try {
           Properties props = new Properties();
           props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
           props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, MockPartitioner.class.getName());

           KafkaProducer<String, String> producer = new KafkaProducer<String, String>(
                   props, new StringSerializer(), new StringSerializer());
           assertEquals(1, MockPartitioner.INIT_COUNT.get());
           assertEquals(0, MockPartitioner.CLOSE_COUNT.get());

           /**
            * 参考KafkaProducer的方法有对MockPartitioner的close方法的调用，才导致MockPartitioner.CLOSE_COUNT会加一
            *  ClientUtils.closeQuietly(interceptors, "producer interceptors", firstException);
            *  ClientUtils.closeQuietly(metrics, "producer metrics", firstException);
            *  ClientUtils.closeQuietly(keySerializer, "producer keySerializer", firstException);
            *  ClientUtils.closeQuietly(valueSerializer, "producer valueSerializer", firstException);
            *  ClientUtils.closeQuietly(partitioner, "producer partitioner", firstException);
            */
           producer.close();
           assertEquals(1, MockPartitioner.INIT_COUNT.get());
           assertEquals(1, MockPartitioner.CLOSE_COUNT.get());
       } finally {
           // cleanup since we are using mutable static variables in MockPartitioner
           MockPartitioner.resetCounters();
       }
   }
   
   /**
    * 设置发送数据和接受数据缓冲区
    * 关于kafka收发消息相关的配置项
	*1.在Broker Server中属性(这些属性需要在Server启动时加载)：
	*每次Broker Server能够接收的最大包大小，该参数要与consumer的fetch.message.max.bytes属性进行匹配使用
	* message.max.bytes 1000000(默认) 
	*Broker Server中针对Producer发送方的数据缓冲区。Broker Server会利用该缓冲区循环接收来至Producer的数据 包，缓冲区过小会导致对该数据包的分段数量增加，但不会影响数据包尺寸限制问题。
	*socket.send.buffer.bytes 100 * 1024(默认)
	*Broker Server中针对Consumer接收方的数据缓冲区。意思同上。
	socket.receive.buffer.bytes 100 * 1024(默认)
	*Broker Server中针对每次请求最大的缓冲区尺寸，包括Prodcuer和Consumer双方。该值必须大于 message.max.bytes属性
	* socket.request.max.bytes 100 * 1024 * 1024(默认)
	*2.在Consumer中的属性(这些属性需要在程序中配置Consumer时设置)
	*Consumer用于接收来自Broker的数据缓冲区,意思同socket.send.buffer.bytes。
	*socket.receive.buffer.bytes 64 * 1024(默认)
	*Consumer用于每次接收消息包的最大尺寸，该属性需要与Broker中的message.max.bytes属性配对使用
	* fetch.message.max.bytes 1024 * 1024(默认)
	*3.在Producer中的属性(这些属性需要在程序中配置Consumer时设置)
	*Producer用于发送数据缓冲区,意思同socket.send.buffer.bytes。
	*send.buffer.bytes 100 * 1024（默认）
	*
	*如果设置为-1 则用操作系统默认缓冲区大小
    */
   @Test
   public void testOsDefaultSocketBufferSizes() throws Exception {
       Map<String, Object> config = new HashMap<>();
       config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
       /**
        * send.buffer.bytes
        */
       config.put(ProducerConfig.SEND_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
       /**
        * receive.buffer.bytes
        */
       config.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
       new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer()).close();
   }
   
   /**
    * 方面例子，无效的发送数据缓冲区size设置，不能是-2
    * @throws Exception
    */
   @Test(expected = KafkaException.class)
   public void testInvalidSocketSendBufferSize() throws Exception {
       Map<String, Object> config = new HashMap<>();
       config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
       config.put(ProducerConfig.SEND_BUFFER_CONFIG, -2);
       new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
   }
   /**
    * 方面例子，无效的接受数据缓冲区size设置，不能是-2
    * @throws Exception
    */
   @Test(expected = KafkaException.class)
   public void testInvalidSocketReceiveBufferSize() throws Exception {
       Map<String, Object> config = new HashMap<>();
       config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
       config.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, -2);
       new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
   }
   
   
   
   //对于final类，有private函数及static函数的类等，必须使用此注解，之后才能着Stubbing
   @PrepareOnlyThisForTest(Metadata.class)
   @Test
   public void testMetadataFetch() throws Exception {
       Properties props = new Properties();
       props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
       KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
       Metadata metadata = PowerMock.createNiceMock(Metadata.class);
       MemberModifier.field(KafkaProducer.class, "metadata").set(producer, metadata);

       String topic = "topic";
       ProducerRecord<String, String> record = new ProducerRecord<>(topic, "value");
       Collection<Node> nodes = Collections.singletonList(new Node(0, "host1", 1000));
       final Cluster emptyCluster = new Cluster(null, nodes,
               Collections.<PartitionInfo>emptySet(),
               Collections.<String>emptySet(),
               Collections.<String>emptySet());
       final Cluster cluster = new Cluster(
               "dummy",
               Collections.singletonList(new Node(0, "host1", 1000)),
               Arrays.asList(new PartitionInfo(topic, 0, null, null, null)),
               Collections.<String>emptySet(),
               Collections.<String>emptySet());

       // Expect exactly one fetch for each attempt to refresh while topic metadata is not available
       final int refreshAttempts = 5;
       EasyMock.expect(metadata.fetch()).andReturn(emptyCluster).times(refreshAttempts - 1);
       EasyMock.expect(metadata.fetch()).andReturn(cluster).once();
       EasyMock.expect(metadata.fetch()).andThrow(new IllegalStateException("Unexpected call to metadata.fetch()")).anyTimes();
       PowerMock.replay(metadata);
       producer.send(record);
       PowerMock.verify(metadata);

       // Expect exactly one fetch if topic metadata is available
       PowerMock.reset(metadata);
       EasyMock.expect(metadata.fetch()).andReturn(cluster).once();
       EasyMock.expect(metadata.fetch()).andThrow(new IllegalStateException("Unexpected call to metadata.fetch()")).anyTimes();
       PowerMock.replay(metadata);
       producer.send(record, null);
       PowerMock.verify(metadata);

       // Expect exactly one fetch if topic metadata is available
       PowerMock.reset(metadata);
       EasyMock.expect(metadata.fetch()).andReturn(cluster).once();
       EasyMock.expect(metadata.fetch()).andThrow(new IllegalStateException("Unexpected call to metadata.fetch()")).anyTimes();
       PowerMock.replay(metadata);
       producer.partitionsFor(topic);
       PowerMock.verify(metadata);
   }

   @PrepareOnlyThisForTest(Metadata.class)
   @Test
   public void testMetadataFetchOnStaleMetadata() throws Exception {
       Properties props = new Properties();
       props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
       KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
       Metadata metadata = PowerMock.createNiceMock(Metadata.class);
       MemberModifier.field(KafkaProducer.class, "metadata").set(producer, metadata);

       String topic = "topic";
       ProducerRecord<String, String> initialRecord = new ProducerRecord<>(topic, "value");
       // Create a record with a partition higher than the initial (outdated) partition range
       ProducerRecord<String, String> extendedRecord = new ProducerRecord<>(topic, 2, null, "value");
       Collection<Node> nodes = Collections.singletonList(new Node(0, "host1", 1000));
       final Cluster emptyCluster = new Cluster(null, nodes,
               Collections.<PartitionInfo>emptySet(),
               Collections.<String>emptySet(),
               Collections.<String>emptySet());
       final Cluster initialCluster = new Cluster(
               "dummy",
               Collections.singletonList(new Node(0, "host1", 1000)),
               Arrays.asList(new PartitionInfo(topic, 0, null, null, null)),
               Collections.<String>emptySet(),
               Collections.<String>emptySet());
       final Cluster extendedCluster = new Cluster(
               "dummy",
               Collections.singletonList(new Node(0, "host1", 1000)),
               Arrays.asList(
                       new PartitionInfo(topic, 0, null, null, null),
                       new PartitionInfo(topic, 1, null, null, null),
                       new PartitionInfo(topic, 2, null, null, null)),
               Collections.<String>emptySet(),
               Collections.<String>emptySet());

       // Expect exactly one fetch for each attempt to refresh while topic metadata is not available
       final int refreshAttempts = 5;
       EasyMock.expect(metadata.fetch()).andReturn(emptyCluster).times(refreshAttempts - 1);
       EasyMock.expect(metadata.fetch()).andReturn(initialCluster).once();
       EasyMock.expect(metadata.fetch()).andThrow(new IllegalStateException("Unexpected call to metadata.fetch()")).anyTimes();
       PowerMock.replay(metadata);
       producer.send(initialRecord);
       PowerMock.verify(metadata);

       // Expect exactly one fetch if topic metadata is available and records are still within range
       PowerMock.reset(metadata);
       EasyMock.expect(metadata.fetch()).andReturn(initialCluster).once();
       EasyMock.expect(metadata.fetch()).andThrow(new IllegalStateException("Unexpected call to metadata.fetch()")).anyTimes();
       PowerMock.replay(metadata);
       producer.send(initialRecord, null);
       PowerMock.verify(metadata);

       // Expect exactly two fetches if topic metadata is available but metadata response still returns
       // the same partition size (either because metadata are still stale at the broker too or because
       // there weren't any partitions added in the first place).
       PowerMock.reset(metadata);
       EasyMock.expect(metadata.fetch()).andReturn(initialCluster).once();
       EasyMock.expect(metadata.fetch()).andReturn(initialCluster).once();
       EasyMock.expect(metadata.fetch()).andThrow(new IllegalStateException("Unexpected call to metadata.fetch()")).anyTimes();
       PowerMock.replay(metadata);
       try {
           producer.send(extendedRecord, null);
           fail("Expected KafkaException to be raised");
       } catch (KafkaException e) {
           // expected
       }
       PowerMock.verify(metadata);

       // Expect exactly two fetches if topic metadata is available but outdated for the given record
       PowerMock.reset(metadata);
       EasyMock.expect(metadata.fetch()).andReturn(initialCluster).once();
       EasyMock.expect(metadata.fetch()).andReturn(extendedCluster).once();
       EasyMock.expect(metadata.fetch()).andThrow(new IllegalStateException("Unexpected call to metadata.fetch()")).anyTimes();
       PowerMock.replay(metadata);
       producer.send(extendedRecord, null);
       PowerMock.verify(metadata);
   }

   @Test
   public void testTopicRefreshInMetadata() throws Exception {
       Properties props = new Properties();
       props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
       props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "600000");
       KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
       long refreshBackoffMs = 500L;
       long metadataExpireMs = 60000L;
       final Metadata metadata = new Metadata(refreshBackoffMs, metadataExpireMs, true,
               true, new ClusterResourceListeners());
       final Time time = new MockTime();
       MemberModifier.field(KafkaProducer.class, "metadata").set(producer, metadata);
       MemberModifier.field(KafkaProducer.class, "time").set(producer, time);
       final String topic = "topic";

       Thread t = new Thread() {
           @Override
           public void run() {
               long startTimeMs = System.currentTimeMillis();
               for (int i = 0; i < 10; i++) {
                   while (!metadata.updateRequested() && System.currentTimeMillis() - startTimeMs < 1000)
                       yield();
                   metadata.update(Cluster.empty(), Collections.singleton(topic), time.milliseconds());
                   time.sleep(60 * 1000L);
               }
           }
       };
       t.start();
       try {
           producer.partitionsFor(topic);
           fail("Expect TimeoutException");
       } catch (TimeoutException e) {
           // skip
       }
       Assert.assertTrue("Topic should still exist in metadata", metadata.containsTopic(topic));
   }

   @PrepareOnlyThisForTest(Metadata.class)
   @Test
   public void testHeaders() throws Exception {
       Properties props = new Properties();
       props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
       ExtendedSerializer keySerializer = PowerMock.createNiceMock(ExtendedSerializer.class);
       ExtendedSerializer valueSerializer = PowerMock.createNiceMock(ExtendedSerializer.class);

       KafkaProducer<String, String> producer = new KafkaProducer<>(props, keySerializer, valueSerializer);
       Metadata metadata = PowerMock.createNiceMock(Metadata.class);
       MemberModifier.field(KafkaProducer.class, "metadata").set(producer, metadata);

       String topic = "topic";
       final Cluster cluster = new Cluster(
               "dummy",
               Collections.singletonList(new Node(0, "host1", 1000)),
               Arrays.asList(new PartitionInfo(topic, 0, null, null, null)),
               Collections.<String>emptySet(),
               Collections.<String>emptySet());


       EasyMock.expect(metadata.fetch()).andReturn(cluster).anyTimes();

       PowerMock.replay(metadata);

       String value = "value";

       ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
       EasyMock.expect(keySerializer.serialize(topic, record.headers(), null)).andReturn(null).once();
       EasyMock.expect(valueSerializer.serialize(topic, record.headers(), value)).andReturn(value.getBytes()).once();

       PowerMock.replay(keySerializer);
       PowerMock.replay(valueSerializer);


       //ensure headers can be mutated pre send.
       record.headers().add(new RecordHeader("test", "header2".getBytes()));

       producer.send(record, null);

       //ensure headers are closed and cannot be mutated post send
       try {
           record.headers().add(new RecordHeader("test", "test".getBytes()));
           fail("Expected IllegalStateException to be raised");
       } catch (IllegalStateException ise) {
           //expected
       }

       //ensure existing headers are not changed, and last header for key is still original value
       assertTrue(Arrays.equals(record.headers().lastHeader("test").value(), "header2".getBytes()));

       PowerMock.verify(valueSerializer);
       PowerMock.verify(keySerializer);
   }

   /**
    * 关闭资源应该遵从幂等性
    */
   @Test
   public void closeShouldBeIdempotent() {
       Properties producerProps = new Properties();
       producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
       Producer producer = new KafkaProducer<>(producerProps, new ByteArraySerializer(), new ByteArraySerializer());
       producer.close();
       producer.close();
   }
   
   
   @Test
   public void testMetricConfigRecordingLevel() {
       Properties props = new Properties();
       props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
       try (KafkaProducer producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
           assertEquals(Sensor.RecordingLevel.INFO, producer.metrics.config().recordLevel());
       }

       props.put(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
       try (KafkaProducer producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
           assertEquals(Sensor.RecordingLevel.DEBUG, producer.metrics.config().recordLevel());
       }
   }
   
   @PrepareOnlyThisForTest(Metadata.class)
   @Test
   public void testInterceptorPartitionSetOnTooLargeRecord() throws Exception {
       Properties props = new Properties();
       props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
       props.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1");
       String topic = "topic";
       ProducerRecord<String, String> record = new ProducerRecord<>(topic, "value");

       KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(),
               new StringSerializer());
       Metadata metadata = PowerMock.createNiceMock(Metadata.class);
       MemberModifier.field(KafkaProducer.class, "metadata").set(producer, metadata);
       final Cluster cluster = new Cluster(
           "dummy",
           Collections.singletonList(new Node(0, "host1", 1000)),
           Arrays.asList(new PartitionInfo(topic, 0, null, null, null)),
           Collections.<String>emptySet(),
           Collections.<String>emptySet());
       EasyMock.expect(metadata.fetch()).andReturn(cluster).once();

       // Mock interceptors field
       ProducerInterceptors interceptors = PowerMock.createMock(ProducerInterceptors.class);
       EasyMock.expect(interceptors.onSend(record)).andReturn(record);
       interceptors.onSendError(EasyMock.eq(record), EasyMock.<TopicPartition>notNull(), EasyMock.<Exception>notNull());
       EasyMock.expectLastCall();
       MemberModifier.field(KafkaProducer.class, "interceptors").set(producer, interceptors);

       PowerMock.replay(metadata);
       EasyMock.replay(interceptors);
       producer.send(record);

       EasyMock.verify(interceptors);
   }
}
