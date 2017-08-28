package com.tony.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.log4j.PropertyConfigurator;


public class ConsumerTest1 {

	public static void main(String args[]) {
		PropertyConfigurator.configure("/software/gitworkspace/self developed/kafka/config/log4j.properties");  
		test4();
	}
	
	/**
	 * 本例使用Kafka的自动commit机制，每隔一段时间（可通过auto.commit.interval.ms来设置）就会自动进行commit offset。
	 * 这里有几点需要注意：
	 * 在使用自动commit时，系统是保证at least once，因为offset是在这些messages被应用处理成功后才进行commit的； 
	 * subscribe方法需要传入所有topic的列表，一个group所消费的topic是不能动态增加的，但是可以在任何时间改变这个列表，它会把前面的设置覆盖掉； 
	 * poll中的参数就是设置一个时长，Consumer在进行拉取数据进行block的最大时间限制； Consumer Manual Offset Control 
	 * 
	 * group.id :必须设置 
	 * auto.offset.reset：如果想获得消费者启动前生产者生产的消息，则必须设置为earliest；如果只需要获得消费者启动后生产者生产的消息，则不需要设置该项 
	 * enable.auto.commit(默认值为true)：如果使用手动commit offset则需要设置为false，并再适当的地方调用consumer.commitSync()，否则每次启动消费折后都会
	 * 从头开始消费信息(在auto.offset.reset=earliest的情况下);
	 * @param args
	 */
	public static void test1() {
		System.out.println("【开始启动test1 消费者程序】");
		Properties props = new Properties();
		//brokerServer(kafka)ip地址,不需要把所有集群中的地址都写上，可是一个或一部分
		props.put("bootstrap.servers", "localhost:9092");
		//设置consumer group name,必须设置
		props.put("group.id", "group");
		//设置使用最开始的offset偏移量为该group.id的最早。如果不设置，则会是latest即该topic最新一个消息的offset
		//如果采用latest，消费者只能得道其启动后，生产者生产的消息
		props.put("auto.offset.reset", "earliest");
		//设置自动提交偏移量(offset),由auto.commit.interval.ms控制提交频率
		props.put("enable.auto.commit", "true"); // 自动commit
		//偏移量(offset)提交频率
		props.put("auto.commit.interval.ms", "10000"); // 自动commit的间隔
		//设置心跳时间
		props.put("session.timeout.ms", "30000");
		//设置key以及value的解析（反序列）类
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("tsuperviseprivate", "superviseEsBasicDataTopic1", "superviseEsStatisticsDataTopic1")); // 可消费多个topic,组成一个list
		while (true) {
			//每次取100条信息
			ConsumerRecords<String, String> records = consumer.poll(100);
			System.out.println("【records 大小为:"+ records.count() +"】");
			for (ConsumerRecord record : records) {
				System.out.printf("【offset = %d, key = %s, value = %s /n 】", record.offset(), record.key(), record.value());
				System.out.println();
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	/**
	 *  自己控制偏移量提交
	 * 手动控制commit
	 * 要进行手动commit，需要在配置文件中将enable.auto.commit设置为false，来禁止自动commit，本例以手动同步commit为例
	 * 在本例中，我们调用了commitSync方法，这是同步commit的方式，同时Kafka还提供了commitAsync方法，它们的区别是：使用同步提交时，consumer会进行block知道commit的结果返回，这样的话如果commit失败就可以今早地发现错误，而当使用异步commit时，commit的结果还未返回，Consumer就会开始拉取下一批的数据，但是使用异步commit可以系统的吞吐量，具体使用哪种方式需要开发者自己权衡； 
	 * 本例中的实现依然是保证at least once，但是如果每次拉取到数据之后，就进行commit，最后再处理数据，就可以保证at last once。
	 * Consumer Manual Partition Assign 
	 */
	public static void test2(){
		System.out.println("【开始启动test2 消费者程序】");
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "group");
		props.put("enable.auto.commit", "false"); // 关闭自动commit
		props.put("session.timeout.ms", "30000");

        //设置使用最开始的offset偏移量为该group.id的最早。如果不设置，则会是latest即该topic最新一个消息的offset
        //如果采用latest，消费者只能得道其启动后，生产者生产的消息
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("tsuperviseprivate", "superviseEsBasicDataTopic1", "superviseEsStatisticsDataTopic1")); // 可消费多个topic,组成一个list
		final int minBatchSize = 10; //批量提交数量
		List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			System.out.println("【records 大小为:"+ records.count() +"】");
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("【offset = %d, key = %s, value = %s /n 】", record.offset(), record.key(),
						record.value());
				 buffer.add(record);
			}
			if (buffer.size() >= minBatchSize) {
				System.out.println("【批量提交offset】");
				consumer.commitSync(); // 批量完成写入后，手工同步commit offset
				buffer.clear();
			}
		}
	}
	
	/**
	 * 消费者手动设置分区
	 * Kafka在进行消费数据时，可以指定消费某个topic的某个partition，这种使用情况比较特殊，并不需要coordinator进行rebalance，
	 * 也就意味着这种模式虽然需要设置group id，但是它跟前面的group的机制并不一样，它与旧的Consumer中的Simple Consumer相似，
	 * 这是Kafka在新的Consumer API中对这种情况的支持。
	 * 注意：
	 * 与前面的subscribe方法一样，在调用assign方法时，需要传入这个Consumer要消费的所有TopicPartition的列表； 
	 * 不管对于simple consumer还是consumer group，所有offset的commit都必须经过group coordinator； 
	 * 在进行commit时，必须设置一个合适的group.id，避免与其他的group产生冲突。如果一个simple consumer试图使用一个与一个
	 * active group相同的id进行commit offset，coordinator将会拒绝这个commit请求，会返回一个CommitFailedException异常，但是，
	 * 如果一个simple consumer与另一个simple consumer使用同一个id，系统就不会报任何错误。
	 */
	public static void test3(){
		System.out.println("【开始启动test3 消费者程序】");
		// TODO Auto-generated method stub
        Properties props = new Properties();
        //props.put("bootstrap.servers", bootstrapServers);//"172.16.49.173:9092;172.16.49.173:9093");
        //设置brokerServer(kafka)ip地址
        props.put("bootstrap.servers", "172.16.49.173:9092");
        //设置consumer group name
        props.put("group.id","manual_g2");

        props.put("enable.auto.commit", "false");

        //设置使用最开始的offset偏移量为该group.id的最早。如果不设置，则会是latest即该topic最新一个消息的offset
        //如果采用latest，消费者只能得道其启动后，生产者生产的消息
        props.put("auto.offset.reset", "earliest");
        //
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String ,String> consumer = new KafkaConsumer<String ,String>(props);
        consumer.subscribe(Arrays.asList("producer_test"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                for (ConsumerRecord<String, String> record : partitionRecords) {
                	System.out.printf("now consumer the message it's offset is :"+record.offset() + " and the value is :" + record.value());
                }
                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                System.out.printf("now commit the partition[ "+partition.partition()+"] offset");
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
            }
        }
	}
	
	public static void test4() {
		Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stockstat");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "1");
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
       /* props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());*/

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        StreamsConfig config = new StreamsConfig(props);
        
		Topology builder = new Topology();
		builder.addSource("superviseTopologySourceName", new StringDeserializer(), new BytesDeserializer(), "tsuperviseprivate")
			   .addProcessor("basicDataPro1", () -> new CommonStreamProcessor(), "superviseTopologySourceName")
				//.addStateStore(Stores.create("supervise-stream-store").withStringKeys().withValues(getSerde(getValueSerializer(), getValueDeserializer())).inMemory().maxEntries(100).build(), "PLATFORM_STREAMPROCESSOR")
			   .addSink("basicAnalysisSink1", "superviseEsStatisticsDataTopic1", new StringSerializer() , new BytesSerializer(), "basicDataPro1")
			   .addSink("basicAnalysisSink2", "superviseEsStatisticsDataTopic1", new StringSerializer() , new BytesSerializer(),
					"basicDataPro1");
		
		//addSink(s.getSinkName(), s.getSinkTopic(),  context.getKeySerializer() , context.getValueSerializer(), parentNames);
		KafkaStreams kafkaStreams = new KafkaStreams(builder,config);
		kafkaStreams.cleanUp();
		kafkaStreams.start();
		System.out.println("启动CommonStreamProccessorDriver成功");
	}
	
	public static void test5() {
		Properties props = new Properties();  
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");  
        props.put("group.id", "test");  
        props.put("enable.auto.commit", "false");  
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);  
        //consumer.subscribe(Arrays.asList("my-topic"));  
        final int minBatchSize = 1;  
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();  
        TopicPartition partition0 = new TopicPartition("ycx3", 0);  
        TopicPartition partition1 = new TopicPartition("ycx3", 1);  
        TopicPartition partition2 = new TopicPartition("ycx3", 2);  
        consumer.assign(Arrays.asList(partition0,partition1,partition2));  
        consumer.seek(partition0, 220);  
        consumer.seek(partition1,160);  
        consumer.seek(partition2,180);  
        //consumer.seekToEnd(Arrays.asList(partition1,partition2));  
        while (true) {  
            try{  
            	ConsumerRecords<String, String> records = consumer.poll(1000); 
            }
         } 
	}
}
