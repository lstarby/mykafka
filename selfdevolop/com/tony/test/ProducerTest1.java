package com.tony.test;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProducerTest1 {
	private static Logger LOG = LoggerFactory.getLogger(ProducerTest1.class);
    public static KafkaProducer<String, String> producer = null;

    public static void main(String[] args) throws Exception { 
    	test1();
    }
    
    
    public static void test1() throws Exception {
    	PropertyConfigurator.configure("/software/gitworkspace/self developed/kafka/config/log4j.properties");  
        System.out.println("Press CTRL-C to stop generating data");

        // add shutdown hook
       /* Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Shutting Down");
                if (producer != null)
                    producer.close();
            }
        });*/

     // TODO Auto-generated method stub
        Properties props = new Properties();
        //props.put("bootstrap.servers", "172.16.49.173:9092;172.16.49.173:9093");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("retries", 0);
        // props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        // props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
       //props.put("partitioner.class", "com.tony.demo.MyPartition");
        producer = new KafkaProducer<String, String>(props);
        
        for(int i = 0 ; i < 20; i++) {
        	System.out.println("【生产第"+ i +"条信息】");
        	// ProducerRecord<String, String> record = new ProducerRecord<String, String>("tsuperviseprivate", "22233333", "生产者发过来的消息：hello world" + i);
        	List<Header> headers = new ArrayList<>();
        	String key1 = "supervise_org_add";
        	String valueStr = "生产者发过来的消息：hello world" + i;
        	Header h1 = new RecordHeader(key1, valueStr.getBytes());
        	headers.add(h1);
        	int partitionsNumber = i%2;
        	System.out.println("【partitionsNumber："+ partitionsNumber +"】");
        	ProducerRecord<String, String> record = new ProducerRecord<String, String>("tsuperviseprivate", partitionsNumber, 38483843L,  key1, valueStr) ;
        	//ProducerRecord<String, String> record = new ProducerRecord<String, String>("tsuperviseprivate", 1, 38483843L,  key1, valueStr, headers) ;
             producer.send(record, new Callback() {
                 public void onCompletion(RecordMetadata metadata, Exception e) {
                     // TODO Auto-generated method stub
                     if (e != null)
                         LOG.error("the producer has a error:" + e.getMessage());
                     else {
                         LOG.info("【The offset of the record we just sent is: " + metadata.offset() + "】");
                         LOG.info("【The partition of the record we just sent is: " + metadata.partition() + "】");
                     }

                 }
             });
             try {
                 Thread.sleep(1000);
             } catch (InterruptedException e1) {
                 e1.printStackTrace();
             }
        }
       
    }
}
